package interstate_test

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"path"
	"testing"

	"github.com/dstreet/interstate"
	"github.com/dstreet/interstate/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func socketPath(t *testing.T) string {
	bb := make([]byte, 16)
	_, err := rand.Read(bb)
	require.NoError(t, err)
	return path.Join(os.TempDir(), fmt.Sprintf("%s.sock", hex.EncodeToString(bb)))
}

func TestLeader_Constructor(t *testing.T) {
	t.Run("errors when socket already exists", func(t *testing.T) {
		// s, err := os.CreateTemp(os.TempDir(), "test.sock")
		s, err := os.Create(socketPath(t))
		require.NoError(t, err)
		defer os.Remove(s.Name())

		l, err := interstate.NewLeaderNode(s.Name(), nil)
		assert.Nil(t, l)
		assert.ErrorIs(t, err, interstate.ErrLeaderAlreadyExists)
	})

	t.Run("errors when datastore fails to open", func(t *testing.T) {
		s := socketPath(t)
		defer os.Remove(s)

		ds := mocks.NewDatastore(t)

		ds.EXPECT().Open().Return(errors.New("open failed"))

		l, err := interstate.NewLeaderNode(s, ds)
		assert.Nil(t, l)
		assert.Error(t, err)
	})

	t.Run("successfully creates a new leader node", func(t *testing.T) {
		s := socketPath(t)
		defer os.Remove(s)

		ds := mocks.NewDatastore(t)

		ds.EXPECT().Open().Return(nil)
		ds.EXPECT().Close().Return(nil)

		l, err := interstate.NewLeaderNode(s, ds)
		assert.NoError(t, err)
		assert.NotNil(t, l)

		assert.FileExists(t, s)
		l.Close()
		assert.NoFileExists(t, s)
	})
}

func TestLeader_Write(t *testing.T) {
	t.Run("errors when version does not match", func(t *testing.T) {
		s := socketPath(t)
		defer os.Remove(s)

		version := uint64(1)

		ds := mocks.NewDatastore(t)
		ds.EXPECT().Open().Return(nil)
		ds.EXPECT().Get().Return(version, nil, nil)
		ds.EXPECT().Close().Return(nil)

		l, err := interstate.NewLeaderNode(s, ds)
		require.NoError(t, err)
		defer l.Close()

		err = l.Write(uint64(2), []byte("data"))
		assert.ErrorIs(t, err, interstate.ErrVersionMismatch)
	})

	t.Run("does not update version or data when put fails", func(t *testing.T) {
		s := socketPath(t)
		defer os.Remove(s)

		version := uint64(1)
		newData := []byte("data")

		ds := mocks.NewDatastore(t)
		ds.EXPECT().Open().Return(nil)
		ds.EXPECT().Get().Return(version, nil, nil)
		ds.EXPECT().Close().Return(nil)
		ds.EXPECT().Put(version+1, newData).Return(errors.New("put failed"))

		l, err := interstate.NewLeaderNode(s, ds)
		require.NoError(t, err)
		defer l.Close()

		err = l.Write(version, newData)
		assert.Error(t, err)

		actualVersion, err := l.Version()
		assert.NoError(t, err)

		actualData, err := l.Data()
		assert.NoError(t, err)

		assert.Equal(t, version, actualVersion, "version should not be updated")
		assert.Nil(t, actualData, "data should not be updated")
	})

	t.Run("updates interstate version and data", func(t *testing.T) {
		s := socketPath(t)
		defer os.Remove(s)

		version := uint64(1)
		newData := []byte("data")

		ds := mocks.NewDatastore(t)
		ds.EXPECT().Open().Return(nil)
		ds.EXPECT().Get().Return(version, nil, nil).Once()
		ds.EXPECT().Close().Return(nil)
		ds.EXPECT().Put(version+1, newData).Return(nil)

		l, err := interstate.NewLeaderNode(s, ds)
		require.NoError(t, err)
		defer l.Close()

		err = l.Write(version, newData)
		assert.NoError(t, err)

		ds.EXPECT().Get().Return(version+1, newData, nil)

		actualVersion, err := l.Version()
		assert.NoError(t, err)

		actualData, err := l.Data()
		assert.NoError(t, err)

		assert.Equal(t, version+1, actualVersion, "version should be updated")
		assert.Equal(t, newData, actualData, "data should be updated")
	})

	t.Run("sends update message to all clients", func(t *testing.T) {
		s := socketPath(t)
		defer os.Remove(s)

		version := uint64(1)
		newData := []byte("data")

		ds := mocks.NewDatastore(t)
		ds.EXPECT().Open().Return(nil)
		ds.EXPECT().Close().Return(nil)
		ds.EXPECT().Put(version+1, newData).Return(nil)

		l, err := interstate.NewLeaderNode(s, ds)
		require.NoError(t, err)
		defer l.Close()

		ds.EXPECT().Get().Return(version, nil, nil).Once()

		client, err := net.Dial("unix", s)
		require.NoError(t, err)
		defer client.Close()

		// Wait to consume the initial version update message
		_, err = waitForMessage(client)
		require.NoError(t, err)

		ds.EXPECT().Get().Return(version, nil, nil).Once()
		err = l.Write(version, newData)
		assert.NoError(t, err)

		ds.EXPECT().Get().Return(version+1, newData, nil)

		actualVersion, err := l.Version()
		assert.NoError(t, err)

		actualData, err := l.Data()
		assert.NoError(t, err)

		assert.Equal(t, version+1, actualVersion, "version should be updated")
		assert.Equal(t, newData, actualData, "data should be updated")

		body, err := waitForMessage(client)
		require.NoError(t, err)
		assert.NotNil(t, body)

		mt := interstate.GetMessageType(body)
		assert.Equal(t, interstate.MessageTypeVersionUpdate, mt)

		msg := &interstate.VersionUpdateMessage{}
		err = msg.Decode(body)
		assert.NoError(t, err)
		assert.Equal(t, version+1, msg.Version, s)
		assert.Equal(t, newData, msg.Data)
	})
}

func TestLeader_HandleUpdateRequest(t *testing.T) {
	t.Run("returns error when version does not match", func(t *testing.T) {
		s := socketPath(t)
		defer os.Remove(s)

		version := uint64(1)
		newData := []byte("data")

		ds := mocks.NewDatastore(t)
		ds.EXPECT().Open().Return(nil)
		ds.EXPECT().Close().Return(nil)

		l, err := interstate.NewLeaderNode(s, ds)
		require.NoError(t, err)
		defer l.Close()

		ds.EXPECT().Get().Return(version, nil, nil).Once()

		client, err := net.Dial("unix", s)
		require.NoError(t, err)

		// Wait to consume the initial version update message
		_, err = waitForMessage(client)
		require.NoError(t, err)

		req := &interstate.UpdateRequest{
			RequestID: 1,
			Version:   version - 1,
			Data:      newData,
		}

		body, err := req.Encode()
		require.NoError(t, err)
		msg := interstate.PrependRequestLength(body)

		ds.EXPECT().Get().Return(version, nil, nil).Once()

		_, err = client.Write(msg)
		require.NoError(t, err)

		res, err := waitForMessage(client)
		require.NoError(t, err)

		mt := interstate.GetMessageType(res)
		assert.Equal(t, interstate.MessageTypeUpdateResponse, mt)

		updateResponse := &interstate.UpdateResponse{}
		err = updateResponse.Decode(res)
		require.NoError(t, err)
		assert.Equal(t, interstate.ResponseErrorMismatchedVersion, updateResponse.Error, "response error should be mismatched version")
	})

	t.Run("returns error when datastore put fails", func(t *testing.T) {
		s := socketPath(t)
		defer os.Remove(s)

		version := uint64(1)
		newData := []byte("data")

		ds := mocks.NewDatastore(t)
		ds.EXPECT().Open().Return(nil)
		ds.EXPECT().Get().Return(version, nil, nil)
		ds.EXPECT().Close().Return(nil)
		ds.EXPECT().Put(version+1, newData).Return(errors.New("put failed"))

		l, err := interstate.NewLeaderNode(s, ds)
		require.NoError(t, err)
		defer l.Close()

		client, err := net.Dial("unix", s)
		require.NoError(t, err)
		defer client.Close()

		// Wait to consume the initial version update message
		_, err = waitForMessage(client)
		require.NoError(t, err)

		req := &interstate.UpdateRequest{
			RequestID: 1,
			Version:   version,
			Data:      newData,
		}

		body, err := req.Encode()
		require.NoError(t, err)
		msg := interstate.PrependRequestLength(body)

		_, err = client.Write(msg)
		require.NoError(t, err)

		res, err := waitForMessage(client)
		require.NoError(t, err)

		mt := interstate.GetMessageType(res)
		assert.Equal(t, interstate.MessageTypeUpdateResponse, mt)

		updateResponse := &interstate.UpdateResponse{}
		err = updateResponse.Decode(res)
		require.NoError(t, err)
		assert.Equal(t, interstate.ResponseErrorWriteFailed, updateResponse.Error, "response error should be write failed")
	})

	t.Run("returns success", func(t *testing.T) {
		s := socketPath(t)
		defer os.Remove(s)

		version := uint64(1)
		newData := []byte("data")

		ds := mocks.NewDatastore(t)
		ds.EXPECT().Open().Return(nil)
		ds.EXPECT().Get().Return(version, nil, nil)
		ds.EXPECT().Close().Return(nil)
		ds.EXPECT().Put(version+1, newData).Return(nil)

		l, err := interstate.NewLeaderNode(s, ds)
		require.NoError(t, err)
		defer l.Close()

		client, err := net.Dial("unix", s)
		require.NoError(t, err)

		// Wait to consume the initial version update message
		_, err = waitForMessage(client)
		require.NoError(t, err)

		req := &interstate.UpdateRequest{
			RequestID: 1,
			Version:   version,
			Data:      newData,
		}

		body, err := req.Encode()
		require.NoError(t, err)
		msg := interstate.PrependRequestLength(body)

		_, err = client.Write(msg)
		require.NoError(t, err)

		res, err := waitForMessage(client)
		require.NoError(t, err)

		mt := interstate.GetMessageType(res)
		assert.Equal(t, interstate.MessageTypeUpdateResponse, mt)

		updateResponse := &interstate.UpdateResponse{}
		err = updateResponse.Decode(res)
		require.NoError(t, err)
		assert.Equal(t, interstate.ResponseErrorNone, updateResponse.Error, "response error should be none")
		assert.Equal(t, version+1, updateResponse.Version, "response version should be updated")
		assert.Equal(t, newData, updateResponse.Data, "response data should be updated")
	})
}

func waitForMessage(conn net.Conn) ([]byte, error) {
	header := make([]byte, 8)
	_, err := conn.Read(header)
	if err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint64(header)
	body := make([]byte, length)
	_, err = conn.Read(body)
	if err != nil {
		return nil, err
	}

	return body, nil
}
