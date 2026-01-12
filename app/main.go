package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/app/internal"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		handleClient(conn)
	}

}

func handleClient(conn net.Conn) {
	defer conn.Close()
	// read incoming header
	// we need to create buffer long enough for message size (1024 can be limiting)

	bufSize := make([]byte, 4)
	// read first 4 bytes to get message length
	_, err := io.ReadFull(conn, bufSize)
	if err != nil {
		return
	}
	msgLen := int32(binary.BigEndian.Uint32(bufSize))

	payload := make([]byte, msgLen)
	_, err = io.ReadFull(conn, payload)
	if err != nil {
		log.Println("Error reading payload:", err)
		return
	}
	// Parse request into Message
	msg, err := parseKafkaRequest(msgLen, payload)
	if err != nil {
		log.Println("error parsing kafka message: ", err)
		conn.Write([]byte(err.Error()))
		return
	}

	// fmt.Println("Kafka Req\t: ", msg)

	resp := bytes.Buffer{}

	apiResponse := HandleAPIKeys(&msg)

	switch v := apiResponse.(type) {
	case internal.APIVersionsResponseV4:
		// fmt.Println("v:::", v)
		binary.Write(&resp, binary.BigEndian, msg.Header.CorrelationID)
		binary.Write(&resp, binary.BigEndian, v.ErrorCode)
		lenApiKeys := uint64(len(v.APIKeys) + 1)
		// Write as unsigned varint for compact array
		var varintBuf [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(varintBuf[:], lenApiKeys)
		resp.Write(varintBuf[:n])

		for _, apiKey := range v.APIKeys {
			binary.Write(&resp, binary.BigEndian, apiKey.APIKey)
			binary.Write(&resp, binary.BigEndian, apiKey.MinVersion)
			binary.Write(&resp, binary.BigEndian, apiKey.MaxVersion)
			if apiKey.TagBuffer.Data == nil {
				binary.Write(&resp, binary.BigEndian, uint8(0)) // tag buffer expects one byte
			} else {
				//TODO: implement if tag buffer is not empty then write it currently we still write 0
				binary.Write(&resp, binary.BigEndian, uint8(0))
			}
		}
		binary.Write(&resp, binary.BigEndian, v.ThrottleTimeMS)
		binary.Write(&resp, binary.BigEndian, uint8(0)) // tag_buffer

		// now we know the size of message so we write that
		msgLen := int32(resp.Len())
		kafkaResp := bytes.Buffer{}
		binary.Write(&kafkaResp, binary.BigEndian, msgLen)
		kafkaResp.Write(resp.Bytes())
		conn.Write(kafkaResp.Bytes())
		return
	default:
		conn.Write([]byte("Unknown API response type"))
	}

	conn.Write(resp.Bytes())
}

func parseKafkaRequest(msgLen int32, req []byte) (internal.KafkaRequest, error) {
	fmt.Println("req: ", req)
	msg := internal.NewKafkaRequest()
	msg.MessageSize = msgLen

	reader := bytes.NewReader(req)

	// Read fixed sized fields (headers)
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

	/*
		Kafka protocol: strings are encoded as:
		Length prefix (int16 for nullable strings, where -1 means null)
		String bytes (if length > 0)
	*/

	// only read if there is more data to read else we will get EOF error
	if reader.Len() == 0 {
		return msg, nil
	}

	// Read nullable string (ClientID)
	/*
		From: https://kafka.apache.org/41/design/protocol/#protocol-primitive-types
		NULLABLE_STRING:
			For non-null strings, first the length N is given as an INT16.
			Then N bytes of the character sequence.
			A null value is encoded with length of -1 and no following bytes.
	*/
	var clientIDLength int16 // this will represent the length prefix which tells us if string is present or not
	if err := binary.Read(reader, binary.BigEndian, &clientIDLength); err != nil {
		return msg, err
	}

	if clientIDLength == -1 { // null string has been sent in message
		msg.Header.ClientID = nil
	} else if clientIDLength == 0 { // empty string has been sent in message
		s := ""
		msg.Header.ClientID = &s
	} else {
		clientIDBytes := make([]byte, clientIDLength)
		// read upto those bits
		if _, err := reader.Read(clientIDBytes); err != nil {
			return msg, err
		}
		clientIDString := string(clientIDBytes)
		msg.Header.ClientID = &clientIDString
	}

	if reader.Len() == 0 {
		return msg, nil
	}

	// V4 uses varint so we cannot use binary.Read
	// WRONG:
	// // read TagBuffer
	// var tagBufferLength byte
	// if err := binary.Read(reader, binary.BigEndian, &tagBufferLength); err != nil {
	// 	return msg, fmt.Errorf("reading tag buffer size: %w", err)
	// }

	// if tagBufferLength > 0 {
	// 	tagBytes := make([]byte, tagBufferLength)
	// 	if _, err := reader.Read(tagBytes); err != nil {
	// 		return msg, fmt.Errorf("reading tag buffer: %w", err)
	// 	}
	// 	taggedFields := internal.TaggedFields{
	// 		Data: tagBytes,
	// 	}
	// 	msg.Header.TagBuffer = taggedFields
	// }

	// to read Unsigned Varint for TaggedField (V3+ uses Varint)
	tagCount, err := binary.ReadUvarint(reader)
	if err != nil {
		return msg, fmt.Errorf("failed to read header tag count: %v", err)
	}

	if tagCount > 0 {
		// store remaining bytes as is for now
		tagDataLeft := reader.Len()
		if tagDataLeft > 0 {
			data := make([]byte, tagDataLeft)
			_, err := reader.Read(data)
			if err != nil {
				return msg, err
			}

			// Store the tagCount at the beginning or just the raw data
			// Usually, you want the raw data that follows the length
			msg.Header.TagBuffer.Data = data
		}
	}
	return msg, nil
}

// data, error
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

func HandleAPIKeys(msg *internal.KafkaRequest) any {
	if msg == nil {
		return nil
	}

	switch msg.Header.RequestAPIKey {
	case 18:
		return RequestAPIVersion(msg)
	default:
		return nil
	}
}


