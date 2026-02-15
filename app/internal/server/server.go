package server

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"net"

	"github.com/codecrafters-io/kafka-starter-go/app/internal/handler"
	"github.com/codecrafters-io/kafka-starter-go/app/internal/protocol"
)

// HandleClient runs the per-connection loop: read request, dispatch, write response.
func HandleClient(conn net.Conn) {
	log.Println("Handling client")
	defer conn.Close()
	for {

		bufSize := make([]byte, 4)
		_, err := io.ReadFull(conn, bufSize)
		if err != nil {
			if err == io.EOF {
				break
			}
			return
		}
		msgLen := int32(binary.BigEndian.Uint32(bufSize))

		payload := make([]byte, msgLen)
		_, err = io.ReadFull(conn, payload)
		if err != nil {
			log.Println("Error reading payload:", err)
			return
		}
		msg, err := protocol.DecodeRequest(msgLen, payload)
		if err != nil {
			log.Println("error parsing kafka message: ", err)
			conn.Write([]byte(err.Error()))
			return
		}

		resp := handler.HandleAPIKeys(&msg)
		if resp == nil {
			conn.Write([]byte("Unknown API response type"))
			return
		}
		body, err := resp.Encode(msg.Header.CorrelationID)
		if err != nil {
			log.Println("error encoding response:", err)
			return
		}
		frame := bytes.Buffer{}
		binary.Write(&frame, binary.BigEndian, int32(len(body)))
		frame.Write(body)
		conn.Write(frame.Bytes())
	}
}
