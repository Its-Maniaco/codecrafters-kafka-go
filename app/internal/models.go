package internal

type KafkaMessage struct {
	MessageSize int32   `json:"message_size"`
	Header      Headers `json:"header"`
	Body        string  `json:"body"`
}

type Headers struct {
	RequestAPIKey     int16   `json:"request_api_key"`
	RequestAPIVersion int16   `json:"request_api_version"`
	CorrelationID     int32   `json:"correlation_id"`
	ClientID          *string `json:"client_id,omitempty"` // nullable string; Go does not allow null for string.
	TagBuffer         any     `json:"TAG_BUFFER,omitempty"`
}

func NewKafkaMessage() KafkaMessage {
	return KafkaMessage{
		MessageSize: 32,
		Header: Headers{
			RequestAPIKey:     2,
			RequestAPIVersion: 2,
			CorrelationID:     4,
		},
	}
}
