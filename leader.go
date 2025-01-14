package interstate

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"slices"

	"github.com/google/uuid"
)

type LeaderNode struct {
	clients       map[string]Client
	listener      net.Listener
	ds            Datastore
	requests      chan ClientRequest
	watchChannels []chan *Version
}

type Datastore interface {
	Open() error
	Close() error
	Get() (version uint64, data []byte, err error)
	Put(version uint64, data []byte) error
}

type Client net.Conn

type ClientRequest struct {
	ClientID string
	Body     []byte
}

func NewLeaderNode(socket string, ds Datastore) (*LeaderNode, error) {
	exists, err := fileExists(socket)
	if err != nil {
		return nil, fmt.Errorf("failed to check if socket exists: %w", err)
	}

	if exists {
		return nil, ErrLeaderAlreadyExists
	}

	n := &LeaderNode{
		clients:  make(map[string]Client, 0),
		requests: make(chan ClientRequest),
		ds:       ds,
	}

	if err := ds.Open(); err != nil {
		return nil, fmt.Errorf("failed to open datastore: %w", err)
	}

	listener, err := net.Listen("unix", socket)
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %w", err)
	}

	n.listener = listener

	go n.acceptClientConnections()
	go n.syncRequests()

	return n, nil
}

func (n *LeaderNode) Write(version uint64, data []byte) error {
	_, _, err := n.write(version, data, nil)
	return err
}

func (n *LeaderNode) Version() (uint64, error) {
	v, _, err := n.ds.Get()
	if err != nil {
		return 0, fmt.Errorf("failed to get version: %w", err)
	}

	return v, nil
}

func (n *LeaderNode) Data() ([]byte, error) {
	_, d, err := n.ds.Get()
	if err != nil {
		return nil, fmt.Errorf("failed to get data: %w", err)
	}

	return d, nil
}

func (n *LeaderNode) Watch(ch chan *Version) {
	n.watchChannels = append(n.watchChannels, ch)
}

func (n *LeaderNode) Close() error {
	if err := n.ds.Close(); err != nil {
		return fmt.Errorf("failed to close datastore: %w", err)
	}
	return n.listener.Close()
}

func (n *LeaderNode) acceptClientConnections() {
	for {
		conn, err := n.listener.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				fmt.Println(err)
			}
			return
		}

		n.handleConnection(conn)
	}
}

// request format:
// [ length (8 bytes) ][ request type (2 byte) ][ data ]
func (n *LeaderNode) handleConnection(conn net.Conn) {
	id := uuid.New().String()

	v, d, err := n.ds.Get()
	if err != nil {
		fmt.Println("failed to get version and data:", err)
	}

	update := &VersionUpdateMessage{
		Version: v,
		Data:    d,
	}

	body, err := update.Encode()
	if err != nil {
		fmt.Printf("failed to encode version update message: %v\n", err)
		return
	}

	msg := PrependRequestLength(body)

	if _, err := conn.Write(msg); err != nil {
		fmt.Println("failed to write version update message to client:", err)
		return
	}

	n.clients[id] = conn

	go func() {
		defer conn.Close()
		reader := bufio.NewReader(conn)

		for {
			header := make([]byte, 8)
			_, err := reader.Read(header)
			if err != nil {
				fmt.Println("failed to read header bytes:", err, "clients", len(n.clients))
				break
			}

			length := GetMessageLength(header)
			body := make([]byte, length)
			_, err = reader.Read(body)
			if err != nil {
				fmt.Println("failed to read body:", err)
				continue
			}

			n.requests <- ClientRequest{
				ClientID: id,
				Body:     body,
			}
		}

		delete(n.clients, id)
	}()
}

// syncRequests ensures that all client requests are handled in order
func (n *LeaderNode) syncRequests() {
	for req := range n.requests {
		n.handleClientRequest(req.ClientID, req.Body)
	}
}

func (n *LeaderNode) handleClientRequest(clientID string, reqBody []byte) {
	requestType := GetMessageType(reqBody)

	switch requestType {
	case MessageTypeUpdateRequest:
		updateReq := &UpdateRequest{}
		if err := updateReq.Decode(reqBody); err != nil {
			fmt.Println("failed to decode update request:", err)
			return
		}

		res := &UpdateResponse{
			RequestID: updateReq.RequestID,
			Error:     ResponseErrorNone,
		}

		newVersion, newData, err := n.write(updateReq.Version, updateReq.Data, []string{clientID})
		if err != nil {
			if err == ErrVersionMismatch {
				res.Error = ResponseErrorMismatchedVersion
			} else {
				res.Error = ResponseErrorWriteFailed
			}
		}

		res.Version = newVersion
		res.Data = newData

		data, err := res.Encode()
		if err != nil {
			fmt.Println("failed to encode update response:", err)
			return
		}

		msg := PrependRequestLength(data)

		if _, err := n.clients[clientID].Write(msg); err != nil {
			fmt.Println("failed to write response to client:", err)
		}

		v := NewVersion(newVersion, newData, n)

		for _, c := range n.watchChannels {
			go func() { c <- v }()
		}
	}
}

func (n *LeaderNode) write(version uint64, data []byte, excludeClients []string) (uint64, []byte, error) {
	v, err := n.Version()
	if err != nil {
		return 0, nil, fmt.Errorf("failed to get version: %w", err)
	}

	if version != v {
		return v, nil, ErrVersionMismatch
	}

	version++

	if err := n.ds.Put(version, data); err != nil {
		return v, nil, fmt.Errorf("failed to put data: %w", err)
	}

	if len(n.clients) > 0 {
		update := &VersionUpdateMessage{
			Version: version,
			Data:    data,
		}

		body, err := update.Encode()
		if err != nil {
			return 0, nil, fmt.Errorf("failed to encode version update message: %w", err)
		}

		msg := PrependRequestLength(body)

		for k, c := range n.clients {
			if slices.Contains(excludeClients, k) {
				continue
			}

			c.Write(msg)
		}
	}

	return version, data, nil
}
