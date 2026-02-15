package protocol

import (
	"bytes"
	"encoding/binary"

	"github.com/codecrafters-io/kafka-starter-go/app/internal"
)

// EncodeAPIVersionsResponse encodes an APIVersionsResponseV4 as the response body (without the 4-byte length prefix).
func EncodeAPIVersionsResponse(correlationID int32, v internal.APIVersionsResponseV4) ([]byte, error) {
	var resp bytes.Buffer
	binary.Write(&resp, binary.BigEndian, correlationID)
	binary.Write(&resp, binary.BigEndian, v.ErrorCode)
	lenApiKeys := uint64(len(v.APIKeys) + 1)
	var varintBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(varintBuf[:], lenApiKeys)
	resp.Write(varintBuf[:n])

	for _, apiKey := range v.APIKeys {
		binary.Write(&resp, binary.BigEndian, apiKey.APIKey)
		binary.Write(&resp, binary.BigEndian, apiKey.MinVersion)
		binary.Write(&resp, binary.BigEndian, apiKey.MaxVersion)
		if apiKey.TagBuffer.Data == nil {
			binary.Write(&resp, binary.BigEndian, uint8(0))
		} else {
			binary.Write(&resp, binary.BigEndian, uint8(0))
		}
	}
	binary.Write(&resp, binary.BigEndian, v.ThrottleTimeMS)
	binary.Write(&resp, binary.BigEndian, uint8(0))

	return resp.Bytes(), nil
}

// APIVersionsResponse wraps internal.APIVersionsResponseV4 and implements internal.Response.
type APIVersionsResponse struct {
	V internal.APIVersionsResponseV4
}

// Encode encodes the response body (without the 4-byte length prefix).
func (r APIVersionsResponse) Encode(correlationID int32) ([]byte, error) {
	return EncodeAPIVersionsResponse(correlationID, r.V)
}
