package sdk

import "time"

// APIClient
type APIClient interface {
	MakeRequest(APIRequest) *APIResponse
	SetDebug(val bool)
}

// APIClientConfiguration
type APIClientConfiguration struct {
	Address       string
	Protocol      string
	Timeout       time.Duration
	MaxRetry      int
	WaitToRetry   time.Duration
	LoggingCol    string
	MaxConnection int
}

func NewAPIClient(config *APIClientConfiguration) APIClient {
	switch config.Protocol {
	case "THRIFT":
		return NewThriftClient(config.Address, config.Timeout, config.MaxConnection, config.MaxRetry, config.WaitToRetry)
	case "HTTP":
		return NewHTTPClient(config)
	}
	return nil
}
