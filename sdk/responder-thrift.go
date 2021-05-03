package sdk

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/binhgo/go-sdk/sdk/thriftapi"
)

// ThriftAPIResponder This is response object with JSON format
type ThriftAPIResponder struct {
	t        string
	resp     *thriftapi.APIResponse
	start    time.Time
	hostname string
}

func newThriftAPIResponder(hostname string) APIResponder {
	return &ThriftAPIResponder{
		t:        "THRIFT",
		start:    time.Now(),
		hostname: hostname,
	}
}

// GetThriftResponse ..
func (responder *ThriftAPIResponder) GetThriftResponse() *thriftapi.APIResponse {
	return responder.resp
}

// Respond ..
func (responder *ThriftAPIResponder) Respond(response *APIResponse) error {
	if response.Data != nil && reflect.TypeOf(response.Data).Kind() != reflect.Slice {
		return errors.New("data response must be a slice")
	}

	var dif = float64(time.Since(responder.start).Nanoseconds()) / 1000000

	responder.resp = &thriftapi.APIResponse{
		ErrorCode: response.ErrorCode,
		Message:   response.Message,
		Total:     response.Total,
		Headers:   make(map[string]string),
	}
	responder.resp.Status, _ = thriftapi.StatusFromString(response.Status)
	bytes, _ := json.Marshal(response.Data)
	responder.resp.Content = string(bytes)
	responder.resp.Headers["X-Execution-Time"] = fmt.Sprintf("%.4f ms", dif)
	responder.resp.Headers["X-Hostname"] = responder.hostname
	return nil
}
