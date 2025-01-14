package appendonly_test

import (
	"encoding/binary"
	"os"
	"path"
	"testing"

	"github.com/dstreet/interstate/appendonly"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppendOnlyDatafile_Open(t *testing.T) {
	t.Run("opens existing, empty file", func(t *testing.T) {
		f, err := os.CreateTemp(os.TempDir(), "test_*")
		require.NoError(t, err)
		defer os.Remove(f.Name())

		ds := appendonly.NewDatastore(f.Name())
		defer ds.Close()

		err = ds.Open()
		assert.NoError(t, err)
	})

	t.Run("creates new file", func(t *testing.T) {
		fp := path.Join(os.TempDir(), "test")
		defer os.Remove(fp)

		ds := appendonly.NewDatastore(fp)
		defer ds.Close()

		err := ds.Open()
		assert.NoError(t, err)

		assert.FileExists(t, fp)
	})

	t.Run("errors with unexpected file version", func(t *testing.T) {
		f, err := os.CreateTemp(os.TempDir(), "test_*")
		require.NoError(t, err)
		defer os.Remove(f.Name())

		err = binary.Write(f, binary.BigEndian, appendonly.FileVersionHeader(5))
		require.NoError(t, err)

		ds := appendonly.NewDatastore(f.Name())
		defer ds.Close()

		err = ds.Open()
		assert.ErrorIs(t, err, appendonly.ErrUnexpectedFileVersion)
	})

	t.Run("errors if offset is greater than next offset", func(t *testing.T) {
		f, err := os.CreateTemp(os.TempDir(), "test_*")
		require.NoError(t, err)
		defer os.Remove(f.Name())

		err = binary.Write(f, binary.BigEndian, appendonly.FileVersionHeader(1))
		require.NoError(t, err)

		err = binary.Write(f, binary.BigEndian, appendonly.OffsetHeader(2))
		require.NoError(t, err)

		err = binary.Write(f, binary.BigEndian, appendonly.NextOffsetHeader(1))
		require.NoError(t, err)

		ds := appendonly.NewDatastore(f.Name())
		defer ds.Close()

		err = ds.Open()
		assert.ErrorIs(t, err, appendonly.ErrNextOffsetOutOfRange)
	})

	t.Run("errors if offset is greater the size of the file", func(t *testing.T) {
		f, err := os.CreateTemp(os.TempDir(), "test_*")
		require.NoError(t, err)
		defer os.Remove(f.Name())

		err = binary.Write(f, binary.BigEndian, appendonly.FileVersionHeader(1))
		require.NoError(t, err)

		err = binary.Write(f, binary.BigEndian, appendonly.OffsetHeader(100))
		require.NoError(t, err)

		err = binary.Write(f, binary.BigEndian, appendonly.NextOffsetHeader(200))
		require.NoError(t, err)

		ds := appendonly.NewDatastore(f.Name())
		defer ds.Close()

		err = ds.Open()
		assert.ErrorIs(t, err, appendonly.ErrOffsetOutOfRange)
	})
}

func TestAppendOnlyDatafile_Put(t *testing.T) {
	t.Run("errors if file is not open", func(t *testing.T) {
		ds := appendonly.NewDatastore("test")
		defer ds.Close()

		err := ds.Put(1, []byte("test"))
		assert.ErrorIs(t, err, appendonly.ErrFileNotOpen)
	})
}

func TestAppendOnlyDatafile_Operations(t *testing.T) {
	t.Run("put and get new file", func(t *testing.T) {
		f, err := os.CreateTemp(os.TempDir(), "test_*")
		require.NoError(t, err)
		defer os.Remove(f.Name())

		ds := appendonly.NewDatastore(f.Name())
		defer ds.Close()

		err = ds.Open()
		require.NoError(t, err)

		version := uint64(1)
		data := []byte("testing put and get")
		err = ds.Put(version, data)
		assert.NoError(t, err)

		v, d, err := ds.Get()
		assert.NoError(t, err)
		assert.Equal(t, version, v, "version mismatch")
		assert.Equal(t, data, d, "data mismatch")
	})

	t.Run("update existing file", func(t *testing.T) {
		f, err := os.CreateTemp(os.TempDir(), "test_*")
		require.NoError(t, err)
		defer os.Remove(f.Name())

		ds := appendonly.NewDatastore(f.Name())
		defer ds.Close()

		err = ds.Open()
		require.NoError(t, err)

		version := uint64(1)
		data := []byte("intitial data")
		err = ds.Put(version, data)
		require.NoError(t, err)

		version = uint64(2)
		data = []byte("updated data")
		err = ds.Put(version, data)
		require.NoError(t, err)

		v, d, err := ds.Get()
		assert.NoError(t, err)
		assert.Equal(t, version, v, "version mismatch")
		assert.Equal(t, data, d, "data mismatch")
	})
}
