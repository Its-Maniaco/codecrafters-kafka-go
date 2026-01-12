package internal

type KafkaRequest struct {
	MessageSize int32   `json:"message_size"`
	Header      Headers `json:"header"`
	Body        string  `json:"body"`
}

type Headers struct {
	RequestAPIKey     int16        `json:"request_api_key"`
	RequestAPIVersion int16        `json:"request_api_version"`
	CorrelationID     int32        `json:"correlation_id"`
	ClientID          *string      `json:"client_id,omitempty"` // nullable string; Go does not allow null for string.
	TagBuffer         TaggedFields `json:"TAG_BUFFER,omitempty"`
}

func NewKafkaRequest() KafkaRequest {
	return KafkaRequest{}
}

type APIVersionsResponseV4 struct {
	ErrorCode      int16        `json:"error_code"`
	APIKeys        []APIKeys    `json:"api_keys"`
	ThrottleTimeMS int32        `json:"throttle_time_ms"`
	TagBuffer      TaggedFields `json:"TAG_BUFFER,omitempty"`
}

type APIKeys struct {
	APIKey     int16        `json:"api_key"`
	MinVersion int16        `json:"min_version"`
	MaxVersion int16        `json:"max_version"`
	TagBuffer  TaggedFields `json:"TAG_BUFFER,omitempty"`
}

type TaggedFields struct {
	Data []byte
}
