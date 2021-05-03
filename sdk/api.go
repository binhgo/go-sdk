package sdk

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
)

type APIServer interface {
	PreRequest(Handler) error
	SetHandler(*MethodValue, string, Handler) error
	Expose(int)
	Start(*sync.WaitGroup)
	GetHostname() string
}

// HTTPAPIServer ...
type HTTPAPIServer struct {
	T        string
	Echo     *echo.Echo
	Thrift   *ThriftServer
	Port     int
	ID       int
	RunSSL   bool
	SSLPort  int
	hostname string
}

func newHTTPAPIServer(id int, hostname string) APIServer {
	var server = HTTPAPIServer{
		T:        "HTTP",
		Echo:     echo.New(),
		ID:       id,
		hostname: hostname,
	}
	server.Echo.Use(middleware.Gzip())
	return &server
}

//SetHandle Add api handler
func (server *HTTPAPIServer) SetHandler(method *MethodValue, path string, fn Handler) error {
	var wrapper = &HandlerWrapper{
		handler: fn,
		server:  server,
	}

	switch method.Value {
	case APIMethod.GET.Value:
		server.Echo.GET(path, wrapper.processCore)
	case APIMethod.POST.Value:
		server.Echo.POST(path, wrapper.processCore)
	case APIMethod.PUT.Value:
		server.Echo.PUT(path, wrapper.processCore)
	case APIMethod.DELETE.Value:
		server.Echo.DELETE(path, wrapper.processCore)
	case APIMethod.OPTIONS.Value:
		server.Echo.OPTIONS(path, wrapper.processCore)
	}

	return nil
}

//PreRequest ...
func (server *HTTPAPIServer) PreRequest(fn PreHandler) error {

	var preWrapper = &PreHandlerWrapper{
		preHandler: fn,
		server:     server,
	}

	server.Echo.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		preWrapper.next = next
		return preWrapper.processCore
	})
	return nil
}

//Expose Add api handler
func (server *HTTPAPIServer) Expose(port int) {
	server.Port = port
}

//ExposeSSL Add api handler
func (server *HTTPAPIServer) ExposeSSL(port int) {
	server.RunSSL = true
	server.SSLPort = port
}

//Start Start API server
func (server *HTTPAPIServer) Start(wg *sync.WaitGroup) {
	var ps = strconv.Itoa(server.Port)
	fmt.Println("  [ API Server " + strconv.Itoa(server.ID) + " ] Try to listen at " + ps)
	server.Echo.HideBanner = true

	if server.RunSSL {
		go func() {
			err := server.Echo.StartTLS(":"+strconv.Itoa(server.SSLPort), "crt.pem", "key.pem")
			if err != nil {
				fmt.Println("[Start TLS error] " + err.Error())
			}
		}()
	}

	err := server.Echo.Start(":" + ps)
	if err != nil {
		fmt.Println("Fail to start " + err.Error())
	}
	wg.Done()
}

func (server *HTTPAPIServer) GetHostname() string {
	return server.hostname
}

// HandlerWrapper handler object
type HandlerWrapper struct {
	handler Handler
	server  *HTTPAPIServer
}

// Handler ...
type Handler = func(req APIRequest, res APIResponder) error

// processCore Process basic logic of Echo
func (hw *HandlerWrapper) processCore(c echo.Context) error {
	hw.handler(newHTTPAPIRequest(c), newHTTPAPIResponder(c, hw.server.GetHostname()))
	return nil
}

// PreHandlerWrapper
type PreHandlerWrapper struct {
	preHandler Handler
	next       echo.HandlerFunc
	server     *HTTPAPIServer
}

// PreHandler ...
type PreHandler = func(req APIRequest, res APIResponder) error

// processCore Process basic logic of Echo
func (hw *PreHandlerWrapper) processCore(c echo.Context) error {
	req := newHTTPAPIRequest(c)
	resp := newHTTPAPIResponder(c, hw.server.GetHostname())
	err := hw.preHandler(req, resp)
	if err == nil {
		hw.next(c)
	}
	return nil
}
