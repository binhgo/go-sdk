package websocket

import (
	"net/http"
	"strconv"
	"sync"
	"time"

	"golang.org/x/net/websocket"
)

// WSServer represent web socket server
type WSServer struct {
	Timeout           int
	ConnectionTimeout int
	Port              int
	Name              string

	server    *http.Server
	mux       *http.ServeMux
	lock      *sync.Mutex
	idCounter int
	closedCon int
	routing   map[string]*wsRoute

	countConnection bool
	instanceId      string
}

// genConId generate new connection id
func (wss *WSServer) genConId() int {
	wss.lock.Lock()
	defer wss.lock.Unlock()

	wss.idCounter++
	return wss.idCounter
}

// closeACon close one ws connection
func (wss *WSServer) closeACon() {
	wss.lock.Lock()
	defer wss.lock.Unlock()

	wss.closedCon++
}

// NewWebSocketServer create new WS server
func NewWebSocketServer(name string) (wss *WSServer) {
	wss = &WSServer{
		Name:    name,
		lock:    &sync.Mutex{},
		routing: map[string]*wsRoute{},
		mux:     &http.ServeMux{},
	}

	return wss
}

// NewRoute create new coordinator, which can routing WS request by path
func (wss *WSServer) NewRoute(path string) *wsRoute {
	wsr := newWSRoute()
	if wss.countConnection {
		wsr.SetCountConnection(wss.countConnection, wss.instanceId)
	}
	wss.routing[path] = wsr

	// bind this coordinator to path
	wss.mux.Handle(path, websocket.Handler(func(conn *websocket.Conn) {
		con := newConnection(wss.genConId(), conn, wss.Timeout)
		if wss.ConnectionTimeout != 0 {
			con.timeout = wss.ConnectionTimeout
		}
		wsr.addCon(con)

		// setup handler
		if wsr.OnConnected != nil {
			wsr.OnConnected(con)
		}

		// chatting with client
		wsr.transferring(con)
	}))

	return wsr
}

// GetRoute get coordinator of given path
func (wss *WSServer) GetRoute(path string) *wsRoute {
	return wss.routing[path]
}

// Expose expose port
func (wss *WSServer) Expose(port int) {
	wss.Port = port
}

// GetActiveCon get active connection number in server
func (wss *WSServer) GetActiveCon() int {
	wss.lock.Lock()
	defer wss.lock.Unlock()

	return wss.idCounter - wss.closedCon
}

// Start Start API server
func (wss *WSServer) Start() {
	// prevent Start run twice
	if wss.server != nil {
		return
	}

	// setup http server
	wss.server = &http.Server{
		Addr:         ":" + strconv.Itoa(wss.Port),
		Handler:      wss.mux,
		ReadTimeout:  time.Duration(wss.Timeout*2+20) * time.Second,
		WriteTimeout: time.Duration(wss.Timeout*2+20+20) * time.Second,
	}

	// start to listen
	err := wss.server.ListenAndServe()
	if err != nil {
		panic(err)
	}
}

func (wss *WSServer) ActivateCountConnection(instanceId string) {
	wss.countConnection = true
	wss.instanceId = instanceId
}
