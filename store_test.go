package interstate_test

import (
	"os"
	"path"
	"testing"

	"github.com/dstreet/interstate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreOpenAndClose(t *testing.T) {
	dir := path.Join(os.TempDir(), "interstate_test")
	store := interstate.NewStore(dir)

	err := store.Open()
	require.NoError(t, err)

	assert.DirExists(t, dir)

	err = store.Close()
	assert.NoError(t, err)

	assert.NoDirExists(t, dir)
}

func TestStoreGet(t *testing.T) {
	dir, err := os.MkdirTemp("", "interstate_*")
	require.NoError(t, err)

	store := interstate.NewStore(dir)
	defer store.Close()

	putData := []byte("testing")
	err = store.Put("test.data", putData)
	assert.NoError(t, err)

	getData, err := store.Get("test.data")
	assert.NoError(t, err)
	assert.Equal(t, putData, getData)
}

func TestStoreDelete(t *testing.T) {
	dir, err := os.MkdirTemp("", "interstate_*")
	require.NoError(t, err)

	store := interstate.NewStore(dir)
	defer store.Close()

	putData := []byte("testing")
	err = store.Put("test.data", putData)
	assert.NoError(t, err)

	err = store.Delete("test.data")
	assert.NoError(t, err)

	getData, err := store.Get("test.data")
	assert.Empty(t, getData)
	assert.ErrorIs(t, err, interstate.ErrKeyNotFound)
}

func TestStoreSubscribe(t *testing.T) {
	dir, err := os.MkdirTemp("", "interstate_*")
	require.NoError(t, err)

	store := interstate.NewStore(dir, interstate.WithNotifier(newMockNotifier()))
	defer store.Close()

	var receivedOp interstate.UpdateOperation
	var receivedData []byte
	unsubscribe := store.Subscribe("test.data", func(op interstate.UpdateOperation, data []byte) {
		receivedOp = op
		receivedData = data
	})
	defer unsubscribe()

	putData := []byte("new data")
	err = store.Put("test.data", putData)
	assert.NoError(t, err)

	assert.Equal(t, interstate.UpdateOperationPut, receivedOp)
	assert.Equal(t, putData, receivedData)
}

func TestUpdaterLock(t *testing.T) {
	dir, err := os.MkdirTemp("", "interstate_*")
	require.NoError(t, err)

	store := interstate.NewStore(dir)
	defer store.Close()

	first, err := store.Updater("test.data")
	assert.NoError(t, err)
	defer first.Close()

	second, err := store.Updater("test.data")
	assert.Nil(t, second)
	assert.ErrorIs(t, err, interstate.ErrKeyLocked)
}

func TestUpdaterClose(t *testing.T) {
	dir, err := os.MkdirTemp("", "interstate_*")
	require.NoError(t, err)

	store := interstate.NewStore(dir)
	defer store.Close()

	u, err := store.Updater("test.data")
	assert.NoError(t, err)
	u.Close()

	err = u.Put([]byte("testing"))
	assert.ErrorIs(t, err, interstate.ErrNoLock)

	err = u.Delete()
	assert.ErrorIs(t, err, interstate.ErrNoLock)
}

type mockNotifier struct {
	subscribers map[string]interstate.SubscribeHandler
}

func newMockNotifier() *mockNotifier {
	return &mockNotifier{
		subscribers: make(map[string]interstate.SubscribeHandler),
	}
}

func (n *mockNotifier) Put(key string, data []byte) {
	if handler, ok := n.subscribers[key]; ok {
		handler(interstate.UpdateOperationPut, data)
	}
}

func (n *mockNotifier) Delete(key string) {
	if handler, ok := n.subscribers[key]; ok {
		handler(interstate.UpdateOperationDelete, nil)
	}
}

func (n *mockNotifier) Subscribe(key string, handler interstate.SubscribeHandler) interstate.UnsubscribeFn {
	n.subscribers[key] = handler

	return func() {
		delete(n.subscribers, key)
	}
}
