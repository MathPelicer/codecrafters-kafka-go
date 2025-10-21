package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

type PropertyPositioning struct {
	Start int
	End   int
}

const API_KEY = "ApiKey"
const API_VERSION = "ApiVersion"
const CORRELATION_ID = "CorrelationId"
const MESSAGE_SIZE = "MessageSize"

var RequestProperties = map[string]PropertyPositioning{
	API_KEY:        {4, 6},
	API_VERSION:    {6, 8},
	CORRELATION_ID: {8, 12},
}

var ResponseProperties = map[string]PropertyPositioning{
	MESSAGE_SIZE:   {0, 4},
	CORRELATION_ID: {4, 8},
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	//Uncomment this block to pass the first stage

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	defer l.Close()

	for {
		conn, err := l.Accept()

		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	reqBuff := make([]byte, 1024)
	_, err := conn.Read(reqBuff)

	if err != nil {
		fmt.Println("Could not read the request", err)
		os.Exit(1)
	}

	var _ = parseRequestDataUint16(reqBuff, API_KEY)
	var _ = parseRequestDataUint16(reqBuff, API_VERSION)
	var correlationId = parseRequestDataUint32(reqBuff, CORRELATION_ID)

	var buff = make([]byte, 8)
	createResponseDataUint32(buff, MESSAGE_SIZE, 0)
	createResponseDataUint32(buff, CORRELATION_ID, correlationId)
	conn.Write(buff)

	defer conn.Close()
}

func parseRequestDataUint16(reqBuff []byte, property string) uint16 {
	return binary.BigEndian.Uint16(reqBuff[RequestProperties[property].Start:RequestProperties[property].End])
}

func parseRequestDataUint32(reqBuff []byte, property string) uint32 {
	return binary.BigEndian.Uint32(reqBuff[RequestProperties[property].Start:RequestProperties[property].End])
}

func createResponseDataUint32(buff []byte, property string, data uint32) {
	binary.BigEndian.PutUint32(buff[ResponseProperties[property].Start:ResponseProperties[property].End], data)
}
