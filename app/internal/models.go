package internal

type KafkaMessage struct {
	MessageSize int32
	Header      Headers
}

type Headers struct {
	CorrelationID int32
}

func NewKafkaMessage() KafkaMessage {
	return KafkaMessage{
		MessageSize: 32,
		Header: Headers{
			CorrelationID: 7,
		},
	}
}
