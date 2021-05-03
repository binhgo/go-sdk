package sdk

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/binhgo/go-sdk/sdk/thriftapi"

	"github.com/apache/thrift/lib/go/thrift"
)

// ThriftServer ...
type ThriftServer struct {
	rootServer    *thrift.TSimpleServer
	thriftHandler *ThriftHandler
	port          int
	ID            int
	hostname      string
}

// NewThriftServer ...
func newThriftServer(id int, hostname string) APIServer {
	return &ThriftServer{
		thriftHandler: &ThriftHandler{
			Handlers: make(map[string]Handler),
			hostname: hostname,
		},
		ID:       id,
		port:     9090,
		hostname: hostname,
	}
}

// SetHandle ...
func (server *ThriftServer) SetHandler(method *MethodValue, path string, fn Handler) error {
	fullPath := string(method.Value) + "://" + path
	server.thriftHandler.Handlers[fullPath] = fn
	return nil
}

// PreRequest ...
func (server *ThriftServer) PreRequest(fn PreHandler) error {
	server.thriftHandler.preHandler = fn
	return nil
}

// Expose Add api handler
func (server *ThriftServer) Expose(port int) {
	server.port = port
}

// Start Start API server
func (server *ThriftServer) Start(wg *sync.WaitGroup) {
	var ps = strconv.Itoa(server.port)
	fmt.Println("  [ Thrift API Server " + strconv.Itoa(server.ID) + " ] Try to listen at " + ps)

	var transport thrift.TServerTransport
	transport, _ = thrift.NewTServerSocket("0.0.0.0:" + ps)
	proc := thriftapi.NewAPIServiceProcessor(server.thriftHandler)
	server.rootServer = thrift.NewTSimpleServer4(proc, transport,
		thrift.NewTFramedTransportFactory(thrift.NewTBufferedTransportFactory(24*1024)),
		thrift.NewTBinaryProtocolFactoryDefault())
	server.rootServer.Serve()
	wg.Done()
}

func (server *ThriftServer) GetHostname() string {
	return server.hostname
}

// ThriftHandler ...
type ThriftHandler struct {
	Handlers   map[string]Handler
	preHandler PreHandler
	hostname   string
}

// Call Override abstract thrift interface
func (th *ThriftHandler) Call(ctx context.Context, request *thriftapi.APIRequest) (r *thriftapi.APIResponse, err error) {

	var req = newThriftAPIRequest(request)
	var responder = newThriftAPIResponder(th.hostname)
	var resp *thriftapi.APIResponse

	// process pre-request
	if th.preHandler != nil {
		err := th.preHandler(req, responder)
		resp = responder.GetThriftResponse()
		if err != nil || resp != nil {
			if resp == nil {
				resp = &thriftapi.APIResponse{
					Status:  thriftapi.Status_ERROR,
					Message: "PreRequest error: " + err.Error(),
				}
			}
			return resp, err
		}
	}

	// process routing
	fullPath := request.GetMethod() + "://" + request.GetPath()
	if th.Handlers[fullPath] != nil {
		th.Handlers[fullPath](req, responder)
		return responder.GetThriftResponse(), nil
	}
	return &thriftapi.APIResponse{
		Status:  thriftapi.Status_NOT_FOUND,
		Message: "API Method/Path isn't found",
	}, nil
}
