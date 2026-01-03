package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"

	internal "github.com/codecrafters-io/kafka-starter-go/app/internal"
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

	buf := make([]byte, 8)
	msg := internal.NewKafkaMessage()

	// struct cannot be written directly to sockets.
	// Kafka uses binary encoding (so we wont use json.Marshal)
	binary.Encode(buf, binary.BigEndian, msg)

	conn.Write(buf)
}
