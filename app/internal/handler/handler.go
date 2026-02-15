package handler

import (
	"github.com/codecrafters-io/kafka-starter-go/app/internal"
	"github.com/codecrafters-io/kafka-starter-go/app/internal/protocol"
)

// RequestAPIVersion builds an APIVersions response for the given request.
func RequestAPIVersion(msg *internal.KafkaRequest) internal.APIVersionsResponseV4 {
	resp := new(internal.APIVersionsResponseV4)
	if msg.Header.RequestAPIVersion >= 0 && msg.Header.RequestAPIVersion <= 4 {
		resp.ErrorCode = 0
	} else {
		resp.ErrorCode = 35
	}
	apiKey := internal.APIKeys{}
	apiKey.APIKey = 18
	apiKey.MinVersion = 0
	apiKey.MaxVersion = 4
	apiKey.TagBuffer = internal.TaggedFields{Data: []byte{0}}
	resp.APIKeys = append(resp.APIKeys, apiKey)
	apiKey.APIKey = 17
	apiKey.MinVersion = 1
	apiKey.MaxVersion = 3
	apiKey.TagBuffer = internal.TaggedFields{Data: []byte{0}}
	resp.APIKeys = append(resp.APIKeys, apiKey)

	resp.ThrottleTimeMS = 0
	return *resp
}

// HandleAPIKeys dispatches by request API key and returns the response (or nil).
func HandleAPIKeys(msg *internal.KafkaRequest) internal.Response {
	if msg == nil {
		return nil
	}
	switch msg.Header.RequestAPIKey {
	case 18:
		return protocol.APIVersionsResponse{V: RequestAPIVersion(msg)}
	default:
		return nil
	}
}
