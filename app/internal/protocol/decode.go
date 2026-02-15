package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/codecrafters-io/kafka-starter-go/app/internal"
)

// DecodeRequest decodes a Kafka request payload into a KafkaRequest.
func DecodeRequest(msgLen int32, req []byte) (internal.KafkaRequest, error) {
	fmt.Println("req: ", req)
	msg := internal.NewKafkaRequest()
	msg.MessageSize = msgLen

	reader := bytes.NewReader(req)

	err := binary.Read(reader, binary.BigEndian, &msg.Header.RequestAPIKey)
	if err != nil {
		return msg, err
	}
	err = binary.Read(reader, binary.BigEndian, &msg.Header.RequestAPIVersion)
	if err != nil {
		return msg, err
	}
	err = binary.Read(reader, binary.BigEndian, &msg.Header.CorrelationID)
	if err != nil {
		return msg, err
	}

	if reader.Len() == 0 {
		return msg, nil
	}

	var clientIDLength int16
	if err := binary.Read(reader, binary.BigEndian, &clientIDLength); err != nil {
		return msg, err
	}

	if clientIDLength == -1 {
		msg.Header.ClientID = nil
	} else if clientIDLength == 0 {
		s := ""
		msg.Header.ClientID = &s
	} else {
		clientIDBytes := make([]byte, clientIDLength)
		if _, err := io.ReadFull(reader, clientIDBytes); err != nil {
			return msg, err
		}
		clientIDString := string(clientIDBytes)
		msg.Header.ClientID = &clientIDString
	}

	if reader.Len() == 0 {
		return msg, nil
	}

	tagCount, err := binary.ReadUvarint(reader)
	if err != nil {
		return msg, fmt.Errorf("failed to read header tag count: %v", err)
	}

	if tagCount > 0 {
		tagDataLeft := reader.Len()
		if tagDataLeft > 0 {
			data := make([]byte, tagDataLeft)
			if _, err := reader.Read(data); err != nil {
				return msg, err
			}
			msg.Header.TagBuffer.Data = data
		}
	}
	return msg, nil
}
