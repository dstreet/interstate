package interstate

import (
	"encoding/binary"
	"fmt"
)

type MessageType int16

const (
	MessageTypeUnknown        MessageType = 0
	MessageTypePing           MessageType = 1
	MessageTypeUpdateRequest  MessageType = 2
	MessageTypeUpdateResponse MessageType = 3
	MessageTypeVersionUpdate  MessageType = 4
)

type ResponseError int16

const (
	ResponseErrorNone              ResponseError = 0
	ResponseErrorMismatchedVersion ResponseError = 1
	ResponseErrorWriteFailed       ResponseError = 2
)

func GetMessageType(data []byte) MessageType {
	return MessageType(binary.BigEndian.Uint16(data[:2]))
}

func GetMessageLength(data []byte) int64 {
	return int64(binary.BigEndian.Uint64(data[:8]))
}

func PrependRequestLength(data []byte) []byte {
	length := make([]byte, 8)
	binary.BigEndian.PutUint64(length, uint64(len(data)))
	req := append(length, data...)
	return req
}

type UpdateRequest struct {
	RequestID uint64
	Version   uint64
	Data      []byte
}

func (r *UpdateRequest) Encode() ([]byte, error) {
	var err error

	body := make([]byte, 0)

	body, err = binary.Append(body, binary.BigEndian, uint16(MessageTypeUpdateRequest))
	if err != nil {
		return nil, fmt.Errorf("failed to encode message type: %w", err)
	}

	body, err = binary.Append(body, binary.BigEndian, r.RequestID)
	if err != nil {
		return nil, fmt.Errorf("failed to encode request ID: %w", err)
	}

	body, err = binary.Append(body, binary.BigEndian, r.Version)
	if err != nil {
		return nil, fmt.Errorf("failed to encode version: %w", err)
	}

	body = append(body, r.Data...)

	return body, nil
}

func (r *UpdateRequest) Decode(data []byte) error {
	mt := GetMessageType(data)
	if mt != MessageTypeUpdateRequest {
		return fmt.Errorf("unexpected message type: %v", mt)
	}

	body := data[2:]

	r.RequestID = binary.BigEndian.Uint64(body[:8])
	r.Version = binary.BigEndian.Uint64(body[8:16])
	r.Data = body[16:]

	return nil
}

type UpdateResponse struct {
	Error     ResponseError
	RequestID uint64
	Version   uint64
	Data      []byte
}

func (r *UpdateResponse) Encode() ([]byte, error) {
	var err error

	body := make([]byte, 0)

	body, err = binary.Append(body, binary.BigEndian, uint16(MessageTypeUpdateResponse))
	if err != nil {
		return nil, fmt.Errorf("failed to encode message type: %w", err)
	}

	body, err = binary.Append(body, binary.BigEndian, r.RequestID)
	if err != nil {
		return nil, fmt.Errorf("failed to encode request ID: %w", err)
	}

	body, err = binary.Append(body, binary.BigEndian, r.Error)
	if err != nil {
		return nil, fmt.Errorf("failed to encode error: %w", err)
	}

	body, err = binary.Append(body, binary.BigEndian, r.Version)
	if err != nil {
		return nil, fmt.Errorf("failed to encode version: %w", err)
	}

	body = append(body, r.Data...)

	return body, nil
}

func (r *UpdateResponse) Decode(data []byte) error {
	mt := GetMessageType(data)
	if mt != MessageTypeUpdateResponse {
		return fmt.Errorf("unexpected message type: %v", mt)
	}

	body := data[2:]

	r.RequestID = binary.BigEndian.Uint64(body[:8])
	r.Error = ResponseError(binary.BigEndian.Uint16(body[8:10]))
	r.Version = binary.BigEndian.Uint64(body[10:18])
	r.Data = body[18:]

	return nil
}

type VersionUpdateMessage struct {
	Version uint64
	Data    []byte
}

func (m *VersionUpdateMessage) Encode() ([]byte, error) {
	var err error

	body := make([]byte, 0)

	body, err = binary.Append(body, binary.BigEndian, uint16(MessageTypeVersionUpdate))
	if err != nil {
		return nil, fmt.Errorf("failed to encode message type: %w", err)
	}

	body, err = binary.Append(body, binary.BigEndian, m.Version)
	if err != nil {
		return nil, fmt.Errorf("failed to encode version: %w", err)
	}

	body = append(body, m.Data...)

	return body, nil
}

func (m *VersionUpdateMessage) Decode(data []byte) error {
	mt := GetMessageType(data)
	if mt != MessageTypeVersionUpdate {
		return fmt.Errorf("unexpected message type: %v", mt)
	}

	body := data[2:]

	m.Version = binary.BigEndian.Uint64(body[:8])
	m.Data = body[8:]

	return nil
}
