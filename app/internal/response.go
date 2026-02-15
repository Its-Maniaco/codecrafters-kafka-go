package internal

// Response is a Kafka response that can be encoded to the wire format.
type Response interface {
	Encode(correlationID int32) ([]byte, error)
}
