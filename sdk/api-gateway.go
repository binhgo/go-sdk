package sdk

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

// GWRoute ...
type GWRoute struct {
	Path     string `json:"path" bson:"path"`
	Address  string `json:"address" bson:"address"`
	Protocol string `json:"protocol" bson:"protocol"`
}

type GWWhiteList struct {
	IP     string `json:"ip" bson:"ip"`
	Status bool   `json:"status" bson:"status"`
	Name   string `json:"name" bson:"name"`
}

// APIGateway ...
type APIGateway struct {
	server       APIServer
	conf         *mgo.Collection
	slow         *DBModel2
	routes       []GWRoute
	clients      map[string]APIClient
	preForward   Handler
	onBadGateway Handler
	debug        bool

	whiteListDB *mgo.Collection
	whiteList   map[string]bool
	onBlacklist Handler

	slowThreshold int
}

// NewAPIGateway Create new API Gateway
func NewAPIGateway(server APIServer) *APIGateway {
	var gw = &APIGateway{
		server:        server,
		clients:       make(map[string]APIClient),
		debug:         false,
		slowThreshold: 2000,
	}
	server.PreRequest(gw.route)
	return gw
}

// SetDebug  ...
func (gw *APIGateway) SetDebug(val bool) {
	gw.debug = val
}

// SetDebug  ...
func (gw *APIGateway) SetSlowMsThreshold(val int) {
	gw.slowThreshold = val
}

// GetGWRoutes get current routes
func (gw *APIGateway) GetGWRoutes() []GWRoute {
	return gw.routes
}

// LoadConfigFromDB ..
func (gw *APIGateway) LoadConfigFromDB(col *mgo.Collection) {
	gw.conf = col
	go gw.scanConfig()
}

// InitDB ..
func (gw *APIGateway) InitDB(s *DBSession, dbName string) {
	db := s.GetMGOSession().DB(dbName)
	gw.conf = db.C("gateway_config")
	gw.slow = &DBModel2{
		ColName:        "slow_request",
		DBName:         dbName,
		TemplateObject: &requestLog{},
	}
	gw.whiteListDB = db.C("white_list")

	gw.slow.Init(s)
	go gw.scanConfig()
	go gw.scanWhiteList()
}

// LoadConfigFromObject ..
func (gw *APIGateway) LoadConfigFromObject(routes []GWRoute) {
	gw.routes = routes
}

// SetPreForward set pre-handler for filter / authen / author
func (gw *APIGateway) SetPreForward(hdl Handler) {
	gw.preForward = hdl
}

// SetBadGateway set handler for bad gateway cases
func (gw *APIGateway) SetBadGateway(hdl Handler) {
	gw.onBadGateway = hdl
}

// SetBadGateway set handler for bad gateway cases
func (gw *APIGateway) SetBlackList(hdl Handler) {
	gw.onBlacklist = hdl
}

func (gw *APIGateway) scanConfig() {
	for true {
		var result []GWRoute
		gw.conf.Find(bson.M{}).All(&result)
		if result != nil {
			gw.routes = result
		}
		time.Sleep(10 * time.Second)
	}
}

func (gw *APIGateway) scanWhiteList() {
	for true {
		time.Sleep(7 * time.Second)
		var result []GWWhiteList
		gw.whiteListDB.Find(bson.M{}).All(&result)
		if result != nil {
			if gw.whiteList == nil {
				gw.whiteList = make(map[string]bool)
			}

			for _, v := range result {
				gw.whiteList[v.IP] = v.Status
			}
		}
	}
}

func (gw *APIGateway) getClient(routeInfo GWRoute) APIClient {
	if gw.clients[routeInfo.Path] != nil {
		return gw.clients[routeInfo.Path]
	}
	config := &APIClientConfiguration{
		Address:       routeInfo.Address,
		Protocol:      routeInfo.Protocol,
		Timeout:       20 * time.Second,
		MaxRetry:      0,
		WaitToRetry:   2 * time.Second,
		MaxConnection: 200,
	}

	client := NewAPIClient(config)
	client.SetDebug(gw.debug)

	gw.clients[routeInfo.Path] = client
	return client
}

func (gw *APIGateway) route(req APIRequest, res APIResponder) error {

	if !gw.isAllowIP(req.GetIP()) {
		if gw.onBlacklist != nil {
			return gw.onBlacklist(req, res)
		}
		return gw.onBadGateway(req, res)
	}

	start := time.Now()
	path := req.GetPath()
	method := req.GetMethod()

	if method.Value == APIMethod.OPTIONS.Value {
		return gw.onBadGateway(req, res)
	}

	if gw.debug {
		fmt.Println("Receive Method / Path = " + method.Value + " => " + path)
		bytes, _ := json.Marshal(gw.routes)
		fmt.Println("Current routes: " + string(bytes))
	}
	for i := 0; i < len(gw.routes); i++ {
		if strings.HasPrefix(path, gw.routes[i].Path) {

			if gw.debug {
				fmt.Println(" => Found route: " + gw.routes[i].Protocol + " / " + gw.routes[i].Address)
			}

			// filter, for authen / author ....
			if gw.preForward != nil {
				if gw.debug {
					fmt.Println(" + Gateway has pre-handler")
				}
				err := gw.preForward(req, res)
				if err != nil {
					if gw.debug {
						fmt.Println(" => Pre-handler return error: " + err.Error())
					}
					return err
				}
			}

			// forward the request
			client := gw.getClient(gw.routes[i])
			if gw.debug {
				fmt.Println(" + Get client successfully.")
			}

			// forward remote IP
			xForwarded := req.GetHeader("X-Forwarded-For")
			remoteIP := req.(*HTTPAPIRequest).GetIP()
			if xForwarded == "" {
				xForwarded = remoteIP
			} else {
				xForwarded += "," + remoteIP
			}

			// check added header
			headers := req.GetAttribute("AddedHeaders")
			curHeaders := req.GetHeaders()
			if headers != nil {
				headerMap := headers.(map[string]string)
				for key, value := range headerMap {
					curHeaders[key] = value
				}
			}
			curHeaders["X-Forwarded-For"] = xForwarded

			req = NewOutboundAPIRequest(
				req.GetMethod().Value,
				req.GetPath(),
				req.GetParams(),
				req.GetContentText(),
				curHeaders,
			)

			resp := client.MakeRequest(req)
			if gw.debug {
				fmt.Println(" => Call ended.")
				fmt.Println(" => Result: " + resp.Status + " / " + resp.Message)
			}
			if resp.Headers == nil {
				resp.Headers = make(map[string]string)
			}
			if resp.Headers["X-Execution-Time"] != "" {
				resp.Headers["X-Endpoint-Time"] = resp.Headers["X-Execution-Time"]
			}

			if resp.Headers["X-Hostname"] != "" {
				resp.Headers["X-Endpoint-Hostname"] = resp.Headers["X-Hostname"]
			}

			resp.Headers["Access-Control-Allow-Origin"] = "*"
			resp.Headers["Access-Control-Allow-Methods"] = "OPTIONS, GET, POST, PUT, DELETE"

			res.Respond(resp)

			if gw.slow != nil {
				var dif = float64(time.Since(start).Nanoseconds()) / 1000000
				if dif > float64(gw.slowThreshold) {
					go gw.writeSlowLog(req, resp, dif)
				}
			}

			return &Error{Type: "PROXY_FOUND", Message: "Proxy found."}
		}
	}

	// if not found on gateway config
	if gw.onBadGateway != nil {
		return gw.onBadGateway(req, res)
	}

	return nil
}

type requestLog struct {
	Req  APIRequest   `bson:"request"`
	Resp *APIResponse `bson:"response"`
	Time string       `bson:"elapsed_time"`
}

func (gw *APIGateway) writeSlowLog(req APIRequest, resp *APIResponse, timeDif float64) {
	gw.slow.Create(&requestLog{
		Req:  req,
		Resp: resp,
		Time: fmt.Sprintf("%.2f ms", timeDif),
	})
}

func (gw *APIGateway) isAllowIP(ip string) bool {

	// if no config -> always allow
	if gw.whiteList == nil || len(gw.whiteList) == 0 {
		return true
	}

	// ipv6, ipv4 localhost -> always allow
	if ip == "::1" || ip == "127.0.0.1" {
		return true
	}

	// if config = '*' -> allow
	status, ok := gw.whiteList["*"]
	if ok && status {
		return true
	}

	// search for exact IP
	status, ok = gw.whiteList[ip]
	if ok && status {
		return true
	}

	// split IP address into 4 strings
	// 192.168.100.123 -> [192, 168, 100, 123]
	arr := strings.Split(ip, ".")

	// 1* -> search for 192.168.100.*
	wcIP := fmt.Sprintf("%s.%s.%s.*", arr[0], arr[1], arr[2])
	status, ok = gw.whiteList[wcIP]
	if ok && status {
		return true
	}

	// 2* -> search for 192.168.*.*
	wcIP = fmt.Sprintf("%s.%s.*.*", arr[0], arr[1])
	status, ok = gw.whiteList[wcIP]
	if ok && status {
		return true
	}

	// 3* -> search for 192.*.*.*
	wcIP = fmt.Sprintf("%s.*.*.*", arr[0])
	status, ok = gw.whiteList[wcIP]
	if ok && status {
		return true
	}

	return false
}
