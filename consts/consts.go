package consts

const (
	MAX_DATA_PAYLOAD_SIZE = MAX_DATA_MESSAGE_SIZE - HEADERS_SIZE
	MAX_DATA_MESSAGE_SIZE = 2048
	HEADERS_SIZE          = 12 // operation, topic, client ID and name length; excludes the name bytes
	MAX_CLIENT_NAME_SIZE  = 255
	BUFFER_SIZE           = 50
	UDP_PORTS             = 3
	MAX_TOPICS            = 65536
)
