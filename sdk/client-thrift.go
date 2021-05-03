package sdk

import (
	"context"
	"encoding/json"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/binhgo/go-sdk/sdk/thriftapi"
)

// ThriftClient Client for call Thrift service
type ThriftClient struct {
	adr           string
	timeout       time.Duration
	maxConnection int
	maxRetry      int
	waitToRetry   time.Duration
	cons          map[string]*ThriftCon
	debug         bool
	lock          *sync.Mutex
}

// ThriftCon Single connection to APIServer
type ThriftCon struct {
	Client   *thriftapi.APIServiceClient
	socket   *thrift.TTransport
	inUsed   bool
	hasError bool
	lock     *sync.Mutex
	id       string
}

// NewThriftClient Constructor
func NewThriftClient(adr string, timeout time.Duration, maxCon int, maxRetry int, waitToRetry time.Duration) *ThriftClient {
	return &ThriftClient{
		adr:           adr,
		timeout:       timeout,
		maxConnection: maxCon,
		maxRetry:      maxRetry,
		waitToRetry:   waitToRetry,
		cons:          make(map[string]*ThriftCon),
		lock:          &sync.Mutex{},
	}
}

// SetDebug
func (client *ThriftClient) SetDebug(val bool) {
	client.debug = val
}

// newThriftCon ...
func (client *ThriftClient) newThriftCon() *ThriftCon {
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	addr, _ := net.ResolveTCPAddr("tcp", client.adr)
	var transport thrift.TTransport
	transport = thrift.NewTSocketFromAddrTimeout(addr, client.timeout)
	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTBufferedTransportFactory(8192))
	transport, _ = transportFactory.GetTransport(transport)

	iprot := protocolFactory.GetProtocol(transport)
	oprot := protocolFactory.GetProtocol(transport)
	transport.Open()

	return &ThriftCon{
		socket:   &transport,
		Client:   thriftapi.NewAPIServiceClient(thrift.NewTStandardClient(iprot, oprot)),
		inUsed:   false,
		lock:     &sync.Mutex{},
		hasError: false,
	}
}

// pickCon ...
func (client *ThriftClient) pickCon(useOld bool) *ThriftCon {
	if useOld {
		client.lock.Lock()
		for conID, con := range client.cons {
			// verify if connection is free
			con.lock.Lock()
			if !con.inUsed {
				con.inUsed = true
				socket := *con.socket
				if socket.IsOpen() {
					con.lock.Unlock()
					client.lock.Unlock()
					return con
				}
				socket.Close()
				delete(client.cons, conID)
			}
			con.lock.Unlock()
		}
		if len(client.cons) < client.maxConnection {
			useOld = false
		}

		client.lock.Unlock()
	}

	if !useOld {

		// if not find any available connection, create new
		con := client.newThriftCon()
		con.inUsed = true

		// append to connection pool if have space
		if len(client.cons) < client.maxConnection {
			id := rand.Intn(999999999) + 1000000000
			con.id = strconv.Itoa(id)
			client.lock.Lock()
			client.cons[con.id] = con
			client.lock.Unlock()
		}

		return con
	}

	return nil
}

// call private function to call & retry
func (client *ThriftClient) call(req APIRequest, useNewCon bool) (*thriftapi.APIResponse, error) {

	// map to thrift request
	var r = &thriftapi.APIRequest{
		Path:    req.GetPath(),
		Params:  req.GetParams(),
		Headers: req.GetHeaders(),
		Method:  req.GetMethod().Value,
	}

	if r.Method != "GET" && r.Method != "DELETE" {
		r.Content = req.GetContentText()
	}

	// pick available connection
	var con *ThriftCon
	con = client.pickCon(!useNewCon)
	var retryToGetCon = 0
	for retryToGetCon < 100 && con == nil {
		time.Sleep(10 * time.Millisecond)
		con = client.pickCon(!useNewCon)
		retryToGetCon++
	}

	if con == nil {
		return &thriftapi.APIResponse{
			Status:  500,
			Message: "Service is temporary overloaded!",
		}, &Error{Type: "OVERLOAD", Message: "Connection pool is overloaded!"}
	}
	result, err := con.Client.Call(context.Background(), r)

	// verify error
	if err == nil {
		con.lock.Lock()
		con.inUsed = false
		con.lock.Unlock()
	} else {
		con.hasError = true
		client.lock.Lock()
		(*con.socket).Close()
		delete(client.cons, con.id)
		client.lock.Unlock()
	}

	return result, err
}

// MakeRequest Call Thrift Service
func (client *ThriftClient) MakeRequest(req APIRequest) *APIResponse {
	now := time.Now()
	canRetry := client.maxRetry
	result, err := client.call(req, false)

	// free retry immediately if connection is not open or last connection was failed
	if err != nil {
		errMsg := strings.ToLower(err.Error())
		if strings.Contains(errMsg, "connection not open") || ((strings.Contains(errMsg, "eof") ||
			strings.Contains(errMsg, "connection timed out") || strings.Contains(errMsg, "i/o timeout")) &&
			time.Now().Before(now.Add(200*time.Millisecond))) {
			result, err = client.call(req, true)
		}
	}

	// retry if failed
	for err != nil && canRetry > 0 {
		time.Sleep(client.waitToRetry)
		canRetry--
		result, err = client.call(req, true)
	}

	if err != nil {
		return &APIResponse{
			Status:  APIStatus.Error,
			Message: "Endpoint error: " + err.Error(),
		}
	}

	// parse result
	resp := &APIResponse{
		Status:    result.GetStatus().String(),
		Message:   result.GetMessage(),
		Headers:   result.GetHeaders(),
		Total:     result.GetTotal(),
		ErrorCode: result.GetErrorCode(),
	}
	json.Unmarshal([]byte(result.GetContent()), &resp.Data)
	return resp
}
