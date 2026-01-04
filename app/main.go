package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
	buf := make([]byte, 1024)
	n, _ := conn.Read(buf) // to only read the read bytes and not pad end with 0

	// Parse request into Message
	msg, err := parseKafkaMessage(buf[:n])
	if err != nil {
		log.Println("error parsing kafka message: ", err)
		conn.Write([]byte(err.Error()))
		return
	}

	// Manual Encoding: since we have to handle strings and we dont know the final size required
	// buffer will grow and help us
	resp := bytes.Buffer{}
	// skip all fields as we only need correlation id to be sent back
	binary.Write(&resp, binary.BigEndian, msg.MessageSize)
	binary.Write(&resp, binary.BigEndian, msg.Header.CorrelationID)

	apiResponse := HandleAPIKeys(&msg)
	if apiResponse == nil {
		conn.Write([]byte("Unknown API key"))
		return
	}

	binary.Write(&resp, binary.BigEndian, apiResponse)

	conn.Write(resp.Bytes())
}

func parseKafkaMessage(req []byte) (internal.KafkaMessage, error) {
	msg := internal.NewKafkaMessage()

	reader := bytes.NewReader(req)

	// Read fixed sized fields
	err := binary.Read(reader, binary.BigEndian, &msg.MessageSize)
	if err != nil {
		return msg, err
	}
	err = binary.Read(reader, binary.BigEndian, &msg.Header.RequestAPIKey)
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

	/* Wrong as these fields may not be present/empty
	err = binary.Read(reader, binary.BigEndian, &msg.Header.ClientID)
	if err != nil {
		return msg, err
	}
	err = binary.Read(reader, binary.BigEndian, &msg.Header.TagBuffer)
	if err != nil {
		return msg, err
	}
	*/

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

	// read TagBuffer
	var tagBufferLength byte
	if err := binary.Read(reader, binary.BigEndian, &tagBufferLength); err != nil {
		return msg, fmt.Errorf("reading tag buffer size: %w", err)
	}

	if tagBufferLength > 0 {
		tagBytes := make([]byte, tagBufferLength)
		if _, err := reader.Read(tagBytes); err != nil {
			return msg, fmt.Errorf("reading tag buffer: %w", err)
		}
		msg.Header.TagBuffer = tagBytes
	}

	return msg, nil
}

// data, error
func RequestAPIVersion(msg *internal.KafkaMessage) internal.APIVersionsResponse {
	resp := new(internal.APIVersionsResponse)
	if msg.Header.RequestAPIVersion >= 0 && msg.Header.RequestAPIVersion <= 4 {
		resp.ErrorCode = 0

	} else {
		resp.ErrorCode = 35
	}
	return *resp
}

func HandleAPIKeys(msg *internal.KafkaMessage) any {
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
