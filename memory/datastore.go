package memory

import (
	"sync"
)

type Datastore struct {
	version uint64
	data    []byte
	mu      sync.Mutex
}

func NewDatastore() *Datastore {
	return &Datastore{}
}

func (d *Datastore) Open() error {
	return nil
}

func (d *Datastore) Close() error {
	return nil
}

func (d *Datastore) Get() (uint64, []byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.version, d.data, nil
}

func (d *Datastore) Put(version uint64, data []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.version = version
	d.data = data
	return nil
}
