package interstate

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"syscall"
)

type FollowerNode struct {
	ds            Datastore
	server        net.Conn
	requests      map[uint64]response
	nextRequestID uint64
	ready         bool
	readyChan     chan struct{}
	watchChannels []chan *Version
}

type response chan *UpdateResponse

var (
	ErrInvalidSocket       = errors.New("invalid socket")
	ErrLeaderFailedToWrite = errors.New("leader failed to write")
)

func NewFollowerNode(socket string, ds Datastore) (*FollowerNode, <-chan struct{}, error) {
	conn, err := net.Dial("unix", socket)
	if err != nil {
		if errors.Is(err, syscall.ENOTSOCK) {
			return nil, nil, ErrInvalidSocket
		}

		if errors.Is(err, syscall.ECONNREFUSED) {
			return nil, nil, ErrLeaderClosed
		}

		return nil, nil, fmt.Errorf("failed to dial: %w", err)
	}

	closeChan := make(chan struct{})
	n := &FollowerNode{
		ds:            ds,
		server:        conn,
		requests:      make(map[uint64]response, 0),
		nextRequestID: 1,
		readyChan:     make(chan struct{}),
	}

	go func() {
		reader := bufio.NewReader(conn)

		for {
			header := make([]byte, 8)
			_, err := reader.Read(header)
			if err != nil {
				break
			}

			length := GetMessageLength(header)
			body := make([]byte, length)
			_, err = reader.Read(body)
			if err != nil {
				fmt.Println("failed to read body:", err)
				continue
			}

			n.handleMessage(body)
		}

		close(closeChan)
	}()

	// Wait for initial data from leader
	<-n.readyChan

	return n, closeChan, nil
}

func (n *FollowerNode) handleMessage(body []byte) {
	mt := GetMessageType(body)

	switch mt {
	case MessageTypeUpdateResponse:
		res := &UpdateResponse{}
		if err := res.Decode(body); err != nil {
			fmt.Println("failed to decode update response:", err)
			return
		}

		if ch, ok := n.requests[res.RequestID]; ok {
			ch <- res
		}

	case MessageTypeVersionUpdate:
		msg := &VersionUpdateMessage{}
		if err := msg.Decode(body); err != nil {
			fmt.Println("failed to decode version update message:", err)
			return
		}

		if err := n.ds.Put(msg.Version, msg.Data); err != nil {
			fmt.Println("failed to put version and data:", err)
			return
		}

		if !n.ready {
			n.ready = true
			close(n.readyChan)
			return
		}

		v := NewVersion(msg.Version, msg.Data, n)

		for _, c := range n.watchChannels {
			go func() { c <- v }()
		}
	}
}

func (n *FollowerNode) Write(version uint64, data []byte) error {
	v, err := n.Version()
	if err != nil {
		return fmt.Errorf("failed to get version: %w", err)
	}

	if version != v {
		return ErrVersionMismatch
	}

	ur := &UpdateRequest{
		RequestID: n.nextRequestID,
		Version:   version,
		Data:      data,
	}

	body, err := ur.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode update request: %w", err)
	}

	req := PrependRequestLength(body)

	// Send update request to leader
	if _, err := n.server.Write(req); err != nil {
		return fmt.Errorf("failed to write to server: %w", err)
	}

	resChan := make(response)
	n.requests[n.nextRequestID] = resChan
	n.nextRequestID++

	res := <-resChan

	switch res.Error {
	case ResponseErrorNone:
		return n.ds.Put(res.Version, data)
	case ResponseErrorMismatchedVersion:
		return ErrVersionMismatch
	case ResponseErrorWriteFailed:
		return ErrLeaderFailedToWrite
	default:
		return fmt.Errorf("unknown error: %d", res.Error)
	}
}

func (n *FollowerNode) Version() (uint64, error) {
	v, _, err := n.ds.Get()
	if err != nil {
		return 0, fmt.Errorf("failed to get version: %w", err)
	}

	return v, nil
}

func (n *FollowerNode) Data() ([]byte, error) {
	_, d, err := n.ds.Get()
	if err != nil {
		return nil, fmt.Errorf("failed to get data: %w", err)
	}

	return d, nil
}

func (n *FollowerNode) Watch(ch chan *Version) {
	n.watchChannels = append(n.watchChannels, ch)
}

func (n *FollowerNode) Close() error {
	return n.server.Close()
}
