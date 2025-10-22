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
const ERROR_CODE = "ErrorCode"
const LENGTH = "Length"
const MIN_VERSION = "MinVersion"
const MAX_VERSION = "MaxVersion"
const THROTTLE_TIME = "ThrottleTime"
const TAG_BUFFER = "TagBuffer"

const MAX_SUPPORTED_VERSION = 4

var RequestProperties = map[string]PropertyPositioning{
	MESSAGE_SIZE:   {0, 4},
	API_KEY:        {4, 6},
	API_VERSION:    {6, 8},
	CORRELATION_ID: {8, 12},
}

var ResponseProperties = map[string]PropertyPositioning{
	MESSAGE_SIZE:   {0, 4},
	CORRELATION_ID: {4, 8},
	ERROR_CODE:     {8, 10},
	LENGTH:         {10, 11},
	API_KEY:        {11, 13},
	MIN_VERSION:    {13, 15},
	MAX_VERSION:    {15, 17},
	THROTTLE_TIME:  {17, 21},
	TAG_BUFFER:     {21, 22},
}

var ErrorCodes = map[string]int{}

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

	var _ = parseRequestDataUint32(reqBuff, MESSAGE_SIZE)
	var apiKey = parseRequestDataUint16(reqBuff, API_KEY)
	var apiVersion = parseRequestDataUint16(reqBuff, API_VERSION)
	var correlationId = parseRequestDataUint32(reqBuff, CORRELATION_ID)
	//var _ = parseRequestDataUint16(reqBuff, LENGTH)
	var errorCode = 0

	if apiVersion > MAX_SUPPORTED_VERSION {
		errorCode = 35
	}

	var apiVersionsLenght = 1

	if apiKey != 0 {
		apiVersionsLenght += 1
	}

	var buff = make([]byte, 23)
	var bytesSize = 1
	createResponseDataUint32(buff, CORRELATION_ID, correlationId)
	bytesSize += 4
	createResponseDataUint16(buff, ERROR_CODE, uint16(errorCode))
	bytesSize += 2
	createResponseDataUint8(buff, LENGTH, uint8(apiVersionsLenght))
	bytesSize += 1
	createResponseDataUint16(buff, API_KEY, uint16(apiKey))
	bytesSize += 2
	createResponseDataUint16(buff, MIN_VERSION, uint16(0))
	bytesSize += 2
	createResponseDataUint16(buff, MAX_VERSION, uint16(MAX_SUPPORTED_VERSION))
	bytesSize += 2
	createResponseDataUint32(buff, THROTTLE_TIME, 0)
	bytesSize += 4
	createResponseDataUint8(buff, TAG_BUFFER, uint8(0))
	bytesSize += 1
	createResponseDataUint32(buff, MESSAGE_SIZE, uint32(bytesSize))
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

func createResponseDataUint16(buff []byte, property string, data uint16) {
	binary.BigEndian.PutUint16(buff[ResponseProperties[property].Start:ResponseProperties[property].End], data)
}

func createResponseDataUint8(buff []byte, property string, data uint8) {
	binary.PutUvarint(buff[ResponseProperties[property].Start:ResponseProperties[property].End], uint64(data))
}
