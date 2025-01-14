package interstate

import (
	"errors"
	"fmt"
	"os"
	"path"
	"time"
)

type State struct {
	socket     string
	leaderDS   Datastore
	followerDS Datastore
	n          Node
	watchers   []chan *Version
	resetCh    chan struct{}
	errorCh    chan error
	closed     bool
}

type Node interface {
	Write(version uint64, data []byte) error
	Version() (uint64, error)
	Data() ([]byte, error)
	Watch(ch chan *Version)
	Close() error
}

func NewState(dir string, leaderDS, followerDS Datastore) *State {
	socket := path.Join(dir, "interstate.sock")

	return &State{
		socket:     socket,
		leaderDS:   leaderDS,
		followerDS: followerDS,
		resetCh:    make(chan struct{}),
		errorCh:    make(chan error),
	}
}

func (s *State) Open() error {
	if err := s.initNodeWithRetry(); err != nil {
		return fmt.Errorf("failed to open node: %w", err)
	}

	go func() {
		for range s.resetCh {
			if err := s.initNodeWithRetry(); err != nil {
				s.errorCh <- fmt.Errorf("failed to reset node: %w", err)
			}
		}
	}()

	return nil
}

func (s *State) Close() error {
	s.closed = true
	return s.n.Close()
}

func (s *State) Errors() <-chan error {
	return s.errorCh
}

func (s *State) Watch() <-chan *Version {
	watcher := make(chan *Version)
	s.watchers = append(s.watchers, watcher)

	if s.n != nil {
		s.n.Watch(watcher)
	}

	return watcher
}

func (s *State) Current() (*Version, error) {
	v, err := s.n.Version()
	if err != nil {
		return nil, err
	}

	d, err := s.n.Data()
	if err != nil {
		return nil, err
	}

	return NewVersion(v, d, s), nil
}

func (s *State) Write(version uint64, data []byte) error {
	return s.n.Write(version, data)
}

func (s *State) initNode() error {
	// First, try to obtain leader role
	leader, err := NewLeaderNode(s.socket, s.leaderDS)
	if err == nil {
		s.n = leader

		for _, ch := range s.watchers {
			s.n.Watch(ch)
		}

		return nil
	} else if !errors.Is(err, ErrLeaderAlreadyExists) {
		return fmt.Errorf("failed to create leader node: %w", err)
	}

	// If not leader, try to obtain follower role
	follower, closeCh, err := NewFollowerNode(s.socket, s.followerDS)
	if err != nil {
		return fmt.Errorf("failed to create follower node: %w", err)
	}

	go func() {
		<-closeCh
		if s.closed {
			return
		}
		s.resetCh <- struct{}{}
	}()

	s.n = follower

	for _, ch := range s.watchers {
		s.n.Watch(ch)
	}

	return nil
}

func (s *State) initNodeWithRetry() error {
	for attempts := 0; attempts < 10; attempts++ {
		if err := s.initNode(); err != nil {
			if errors.Is(err, ErrLeaderClosed) {
				fmt.Println("Leader is closed. Deleting socket and retrying...")

				if err := os.Remove(s.socket); err != nil {
					fmt.Println("failed to remove socket:", err)
				}
			}

			time.Sleep(200 * time.Millisecond)
			continue
		}

		return nil
	}

	return fmt.Errorf("maximum number of attempts reached")
}
