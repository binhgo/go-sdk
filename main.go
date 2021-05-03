package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"gitlab.ghn.vn/common-projects/go-sdk/sdk"
)

// APIInfo ...
type APIInfo struct {
	StartTime time.Time
	Env       string
	Version   string
}

var gateway *sdk.APIGateway
var env string

//var configMap map[string]string
var apiInfo APIInfo
var dbName string

// for authorization
var authClient sdk.APIClient
var appCode = "newops"

// onDBConnected function that handle on connected to DB event
func onDBConnected(s *sdk.DBSession) error {
	gateway.InitDB(s, dbName)
	return nil
}

func main2132() {

	fmt.Println("[Gateway] Starting ...")

	// get config & env
	env = os.Getenv("env")
	//configStr := os.Getenv("config")
	//decoded, err := base64.URLEncoding.DecodeString(configStr)
	//if err != nil {
	//	fmt.Println("[Gateway] Convert B64 config string error: " + err.Error())
	//	return
	//}
	//err = json.Unmarshal(decoded, &configMap)
	//if err != nil {
	//	fmt.Println("[Gateway] Parse JSON with config string error: " + err.Error())
	//	return
	//}
	//
	//fmt.Println("[Gateway] Get config completed.")

	apiInfo = APIInfo{
		Version:   os.Getenv("version"),
		Env:       env,
		StartTime: time.Now(),
	}

	// setup new app
	var app = sdk.NewApp("Newops API Gateway")

	// get config from env
	configMap, err := app.GetConfigFromEnv()
	if err != nil {
		panic(err)
	}

	// setup DB
	dbName = appCode + "_" + env + "_gateway"
	authDB := configMap["authDB"]
	if authDB == "" {
		authDB = dbName
	}
	var db = app.SetupDBClient(sdk.DBConfiguration{
		Address:  strings.Split(configMap["dbHost"], ","),
		Username: configMap["dbUser"],
		Password: configMap["dbPassword"],
	})
	db.OnConnected(onDBConnected)

	// setup API Server
	var server, _ = app.SetupAPIServer("HTTP")
	server.Expose(80)

	// setup gateway
	gateway = sdk.NewAPIGateway(server)
	gateway.SetPreForward(secure)
	gateway.SetBadGateway(badGateway)
	gateway.SetBlackList(blacklist)
	gateway.SetDebug(env != "prd")

	// launch app
	err = app.Launch()

	if err != nil {
		name, _ := os.Hostname()
		fmt.Println("Launch error " + name + " " + err.Error())
	}
}

// AuthorizationInput input data of the API
type AuthorizationInput struct {
	PartnerCode string `json:"partnerCode"`
	OrgCode     string `json:"orgCode"`
	Token       string `json:"token"`
	Method      string `json:"method"`
	Path        string `json:"path"`
	AppCode     string `json:"appCode"`
}

// secure Function that filter unauthorized request
func secure(req sdk.APIRequest, resp sdk.APIResponder) error {

	path := req.GetPath()
	addedHeaders := map[string]string{
		"X-AppCode": appCode,
	}

	// by pass
	if path == "/iam/v1/system-partner/access-token" {
		if req.GetHeader("Authorization") == "" {
			rawStr := req.GetParam("partner") + ":" + req.GetParam("public")
			b64 := base64.StdEncoding.EncodeToString([]byte(rawStr))
			addedHeaders["Authorization"] = "Basic " + b64
			req.SetAttribute("AddedHeaders", addedHeaders)
		}
		return nil
	}

	if authClient == nil {
		gwRoutes := gateway.GetGWRoutes()
		for _, route := range gwRoutes {
			if strings.HasPrefix(route.Path, "/iam") {
				authClient = sdk.NewAPIClient(&sdk.APIClientConfiguration{
					Protocol:      route.Protocol,
					Address:       route.Address,
					Timeout:       10 * time.Second,
					MaxConnection: 50,
					WaitToRetry:   2 * time.Second,
				})
				break
			}
		}
	}

	authStr := req.GetHeader("Authorization")
	if authStr == "" {
		resp.Respond(&sdk.APIResponse{
			Status:  sdk.APIStatus.Forbidden,
			Message: "Authorization info is required.",
		})

		return &sdk.Error{Type: "AUTHORIZATION_MISSING"}
	}

	// tok bearer
	arr := strings.Split(authStr, " ")
	if len(arr) != 2 {
		resp.Respond(&sdk.APIResponse{
			Status:  sdk.APIStatus.Forbidden,
			Message: "Authorization header is invalid.",
		})

		return &sdk.Error{Type: "AUTHORIZATION_INVALID"}
	}

	if authClient != nil && !strings.HasPrefix(path, "/iam/v1") {
		// setup auth input
		input := map[string]string{
			"method":   req.GetMethod().Value,
			"path":     req.GetPath(),
			"authType": arr[0],
			"authStr":  arr[1],
			"appCode":  appCode,
		}
		bytes, _ := json.Marshal(input)

		authResponse := authClient.MakeRequest(sdk.NewOutboundAPIRequest("GET",
			"/iam/v1/authorization",
			input,
			string(bytes),
			nil,
		))

		// debug
		if env != "prd" {
			fmt.Println("Auth info: " + string(bytes))
			if authResponse != nil {
				fmt.Println("Auth result: " + authResponse.Status + " " + authResponse.Message)
			}
		}

		if authResponse == nil {
			resp.Respond(&sdk.APIResponse{
				Status:  sdk.APIStatus.Error,
				Message: "System temporary can not verify authorization.",
			})
			return &sdk.Error{Type: "AUTHORIZATION_ERROR"}
		} else if authResponse.Status != sdk.APIStatus.Ok {
			resp.Respond(authResponse)
			return &sdk.Error{Type: "AUTHORIZATION_FAILED"}
		}

		// auth successful
		if authResponse.Data != nil {
			authData := authResponse.Data.([]interface{})[0]
			sourceBytes, err := json.Marshal(authData)
			if err == nil {
				addedHeaders["X-Source"] = string(sourceBytes)
			}
		}
	}

	req.SetAttribute("AddedHeaders", addedHeaders)
	return nil
}

// badGateway custom return when call wrong resource
func badGateway(req sdk.APIRequest, resp sdk.APIResponder) error {
	method := req.GetMethod()
	if method.Value == sdk.APIMethod.OPTIONS.Value {
		resp.Respond(&sdk.APIResponse{
			Status:  sdk.APIStatus.Ok,
			Message: "Newops System - API Gateway run normally.",
			Headers: map[string]string{
				"Access-Control-Allow-Origin":  "*",
				"Access-Control-Allow-Methods": "OPTIONS, GET, POST, PUT, DELETE",
				"Access-Control-Allow-Headers": "Authorization",
				"Access-Control-Max-Age":       "99999999",
			},
		})
		return &sdk.Error{Type: "OPTIONS"}
	} else {
		resp.Respond(&sdk.APIResponse{
			Status:  sdk.APIStatus.NotFound,
			Data:    []APIInfo{apiInfo},
			Message: "Newops System - API Gateway run normally.",
		})
		return &sdk.Error{Type: "BAD_GATEWAY"}
	}
}

//
func blacklist(req sdk.APIRequest, resp sdk.APIResponder) error {
	resp.Respond(&sdk.APIResponse{
		Status:  sdk.APIStatus.Forbidden,
		Data:    []APIInfo{apiInfo},
		Message: "Blacklist",
	})
	return &sdk.Error{Type: "BAD_GATEWAY"}
}
