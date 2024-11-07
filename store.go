package interstate

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path"
	"time"
)

var (
	ErrKeyLocked   = errors.New("key is already locked")
	ErrLockTimeout = errors.New("timed out waiting for lock")
	ErrKeyNotFound = errors.New("key is not found")
	ErrNoLock      = errors.New("no lock has been aquired")
)

type UpdateOperation string

var (
	UpdateOperationPut    UpdateOperation = "PUT"
	UpdateOperationDelete UpdateOperation = "DELETE"
)

type SubscribeHandler func(op UpdateOperation, data []byte)
type UnsubscribeFn func()

type Store struct {
	dir      string
	notifier Notifier
}

type Notifier interface {
	Put(key string, data []byte)
	Delete(key string)
	Subscribe(key string, handler SubscribeHandler) UnsubscribeFn
}

type storeOptionsFn func(*Store)

func WithNotifier(n Notifier) storeOptionsFn {
	return func(s *Store) {
		s.notifier = n
	}
}

func NewStore(dir string, opts ...storeOptionsFn) *Store {
	store := &Store{
		dir: dir,
	}

	for _, o := range opts {
		o(store)
	}

	return store
}

// Open the store for reating and writing.
func (s *Store) Open() error {
	if err := os.MkdirAll(s.dir, 0755); err != nil {
		return fmt.Errorf("failed to create store directory: %w", err)
	}

	return nil
}

// Close removes the store directory and all the data within it.
// It is not necessary to call Close, and should only be called if you want to
// cleanup the data.
func (s *Store) Close() error {
	if err := os.RemoveAll(s.dir); err != nil {
		return fmt.Errorf("failed to remove the store directory: %w", err)
	}

	return nil
}

// Get the data for a key.
// If the key does not exist, an empty slice and ErrKeyNotFound will
// be returned.
func (s *Store) Get(key string) ([]byte, error) {
	hash := hashKey(key)
	path := path.Join(s.dir, hash)

	data, err := os.ReadFile(path)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		return nil, ErrKeyNotFound
	}

	if err != nil {
		return nil, fmt.Errorf("failed to read data for key %q: %w", key, err)
	}

	return data, nil
}

// Put writes data for the key.
// Will obtain a lock on the key so that no other process or goroutine can
// write to the key at the same time. The lock will be released as soon
// the operation has completed.
func (s *Store) Put(key string, data []byte, opts ...updaterOptionsFn) error {
	updater, err := s.Updater(key, opts...)
	if err != nil {
		return err
	}
	defer updater.Close()

	return updater.Put(data)
}

// Delete the key.
// Will obtain a lock on the key so that no other process or goroutine can
// write to the key at the same time. The lock will be released as soon
// the operation has completed.
func (s *Store) Delete(key string, opts ...updaterOptionsFn) error {
	updater, err := s.Updater(key, opts...)
	if err != nil {
		return err
	}
	defer updater.Close()

	return updater.Delete()
}

// Updater obtains a lock on the key so that Put and Delete operations can be
// made against the key without contention. To release the lock, the caller
// must call Close(). The lock placd on the key synchronizes updates (via Put
// and Delete), but Get operations are still able to be performed when the key
// is locked.
// By default, if a lock has already been obtained on the
// key, Updater will return ErrKeyLocked. Use the options params to override
// this behavior to wait for the lock to be available.
// When waiting for the lock, Updater will default to timeout after 10s and
// will poll the filesystem for the lock every 100ms.
func (s *Store) Updater(key string, opts ...updaterOptionsFn) (*Updater, error) {
	options := &updaterOptions{
		pollingInterval: 100 * time.Millisecond,
	}

	for _, o := range opts {
		o(options)
	}

	hash := hashKey(key)
	lock := path.Join(s.dir, fmt.Sprintf("%s.lock", hash))

	if options.waitForLock {
		if err := waitForLock(lock, options.pollingInterval, options.waitTimeout); err != nil {
			return nil, err
		}
	} else {
		if err := tryLock(lock); err != nil {
			return nil, err
		}
	}

	f, err := os.Create(lock)
	if err != nil {
		return nil, fmt.Errorf("failed to create lock file: %w", err)
	}
	defer f.Close()

	return &Updater{
		key:      key,
		keyPath:  path.Join(s.dir, hashKey(key)),
		lock:     lock,
		notifier: s.notifier,
	}, nil
}

func (s *Store) Subscribe(key string, handler func(UpdateOperation, []byte)) UnsubscribeFn {
	if s.notifier == nil {
		return func() {}
	}

	return s.notifier.Subscribe(key, handler)
}

type Updater struct {
	key      string
	keyPath  string
	lock     string
	unlocked bool
	notifier Notifier
}

// Put the data on the key.
func (u *Updater) Put(data []byte) error {
	if u.unlocked {
		return ErrNoLock
	}

	if err := os.WriteFile(u.keyPath, data, 0755); err != nil {
		return fmt.Errorf("failed to write data for key %q: %w", u.key, err)
	}

	if u.notifier != nil {
		u.notifier.Put(u.key, data)
	}

	return nil
}

// Delete the key.
func (u *Updater) Delete() error {
	if u.unlocked {
		return ErrNoLock
	}

	if err := os.Remove(u.keyPath); err != nil {
		return fmt.Errorf("failed to delete data for key %q: %w", u.key, err)
	}

	if u.notifier != nil {
		u.notifier.Delete(u.key)
	}

	return nil
}

// Close releases the lock.
// After calling Close, any calls to Put and Delete will fail with an ErrNoLock
// error.
func (u *Updater) Close() error {
	if err := os.Remove(u.lock); err != nil {
		return fmt.Errorf("failed to remove lock: %w", err)
	}

	u.unlocked = true
	return nil
}

func hashKey(key string) string {
	s := sha256.New()
	s.Write([]byte(key))
	hash := s.Sum(nil)
	return fmt.Sprintf("%x", hash)
}

func tryLock(lock string) error {
	delay := time.Duration(rand.IntN(500)) * time.Millisecond
	time.Sleep(delay)

	exists, err := fileExists(lock)
	if err != nil {
		return fmt.Errorf("failed to check lock: %w", err)
	}

	if exists {
		return ErrKeyLocked
	}

	return nil
}

func waitForLock(lock string, poll time.Duration, timeout *time.Duration) error {
	readyChan := make(chan struct{})
	errChan := make(chan error)

	timeoutChan := make(<-chan time.Time)
	if timeout != nil {
		timeoutChan = time.After(*timeout)
	}

	go func() {
		for {
			err := tryLock(lock)
			if err == nil {
				close(readyChan)
			}

			if !errors.Is(err, ErrKeyLocked) {
				errChan <- err
			}

			time.Sleep(poll)
		}
	}()

	for {
		select {
		case <-timeoutChan:
			return ErrLockTimeout
		case <-readyChan:
			return nil
		case err := <-errChan:
			return err
		}
	}
}

func fileExists(path string) (bool, error) {
	info, err := os.Stat(path)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	if info.IsDir() {
		return false, fmt.Errorf("path is a directory")
	}

	return true, nil
}
