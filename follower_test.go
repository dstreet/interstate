package interstate_test

import (
	"net"
	"os"
	"testing"
	"time"

	"github.com/dstreet/interstate"
	"github.com/dstreet/interstate/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestFollower_Constructor(t *testing.T) {
	t.Run("errors when socket does not exist", func(t *testing.T) {
		s := socketPath(t)
		ds := mocks.NewDatastore(t)
		f, closed, err := interstate.NewFollowerNode(s, ds)
		assert.Nil(t, f)
		assert.Nil(t, closed)
		assert.Error(t, err)
	})

	t.Run("errors when socket is invalid", func(t *testing.T) {
		s, err := os.Create(socketPath(t))
		require.NoError(t, err)
		defer os.Remove(s.Name())

		ds := mocks.NewDatastore(t)
		f, closed, err := interstate.NewFollowerNode(s.Name(), ds)
		assert.Nil(t, f)
		assert.Nil(t, closed)
		assert.ErrorIs(t, err, interstate.ErrInvalidSocket)
	})

	t.Run("waits for initial data from socket server", func(t *testing.T) {
		s := socketPath(t)
		defer os.Remove(s)

		listener, err := net.Listen("unix", s)
		require.NoError(t, err)
		defer listener.Close()

		go func() {
			conn, err := listener.Accept()
			require.NoError(t, err)

			versionUpdate := &interstate.VersionUpdateMessage{
				Version: 1,
				Data:    []byte("test"),
			}

			body, err := versionUpdate.Encode()
			require.NoError(t, err)

			msg := interstate.PrependRequestLength(body)
			conn.Write(msg)
		}()

		ds := mocks.NewDatastore(t)
		ds.EXPECT().Put(uint64(1), []byte("test")).Return(nil)

		f, closed, err := interstate.NewFollowerNode(s, ds)
		assert.NoError(t, err)
		defer f.Close()
		assert.NotNil(t, f)
		assert.NotNil(t, closed)

		ds.EXPECT().Get().Return(uint64(1), []byte("test"), nil)

		actualVersion, err := f.Version()
		assert.NoError(t, err)

		actualData, err := f.Data()
		assert.NoError(t, err)

		assert.Equal(t, uint64(1), actualVersion, "expected version to be 1")
		assert.Equal(t, []byte("test"), actualData, "expected data to be 'test'")
	})
}

func TestFollower_Write(t *testing.T) {
	t.Run("errors with local version mismatch", func(t *testing.T) {
		s := socketPath(t)
		defer os.Remove(s)

		listener, err := net.Listen("unix", s)
		require.NoError(t, err)
		defer listener.Close()

		version := uint64(1)
		data := []byte("test")

		go func() {
			conn, err := listener.Accept()
			require.NoError(t, err)

			versionUpdate := &interstate.VersionUpdateMessage{
				Version: version,
				Data:    data,
			}

			body, err := versionUpdate.Encode()
			require.NoError(t, err)

			msg := interstate.PrependRequestLength(body)
			conn.Write(msg)
			conn.Close()
		}()

		ds := mocks.NewDatastore(t)
		ds.EXPECT().Put(version, data).Return(nil)

		f, _, err := interstate.NewFollowerNode(s, ds)
		require.NoError(t, err)
		defer f.Close()

		ds.EXPECT().Get().Return(version, data, nil)
		err = f.Write(version-1, []byte("new"))
		assert.ErrorIs(t, err, interstate.ErrVersionMismatch)
	})

	t.Run("errors with server version mismatch", func(t *testing.T) {
		s := socketPath(t)
		defer os.Remove(s)

		listener, err := net.Listen("unix", s)
		require.NoError(t, err)
		defer listener.Close()

		version := uint64(1)
		data := []byte("test")

		go func() {
			conn, err := listener.Accept()
			require.NoError(t, err)

			sendVersionUpdate(t, conn, version, data)
			msg, err := waitForMessage(conn)
			require.NoError(t, err)

			req := parseAndValidateUpdateRequest(t, msg)
			assert.Equal(t, version, req.Version)
			assert.Equal(t, []byte("new"), req.Data)

			sendUpdateResponse(t, conn, interstate.UpdateResponse{
				RequestID: req.RequestID,
				Version:   version,
				Data:      data,
				Error:     interstate.ResponseErrorMismatchedVersion,
			})
		}()

		ds := mocks.NewDatastore(t)
		ds.EXPECT().Put(version, data).Return(nil)

		f, _, err := interstate.NewFollowerNode(s, ds)
		require.NoError(t, err)
		defer f.Close()

		ds.EXPECT().Get().Return(version, data, nil)
		err = f.Write(version, []byte("new"))
		assert.ErrorIs(t, err, interstate.ErrVersionMismatch)
	})

	t.Run("errors with leader failed to write", func(t *testing.T) {
		s := socketPath(t)
		defer os.Remove(s)

		listener, err := net.Listen("unix", s)
		require.NoError(t, err)
		defer listener.Close()

		version := uint64(1)
		data := []byte("test")

		go func() {
			conn, err := listener.Accept()
			require.NoError(t, err)

			sendVersionUpdate(t, conn, version, data)
			msg, err := waitForMessage(conn)
			require.NoError(t, err)

			req := parseAndValidateUpdateRequest(t, msg)
			assert.Equal(t, version, req.Version)
			assert.Equal(t, []byte("new"), req.Data)

			sendUpdateResponse(t, conn, interstate.UpdateResponse{
				RequestID: req.RequestID,
				Version:   version,
				Data:      data,
				Error:     interstate.ResponseErrorWriteFailed,
			})
		}()

		ds := mocks.NewDatastore(t)
		ds.EXPECT().Put(version, data).Return(nil)

		f, _, err := interstate.NewFollowerNode(s, ds)
		require.NoError(t, err)
		defer f.Close()

		ds.EXPECT().Get().Return(version, data, nil)
		err = f.Write(version, []byte("new"))
		assert.ErrorIs(t, err, interstate.ErrLeaderFailedToWrite)
	})

	t.Run("updates version and data", func(t *testing.T) {
		s := socketPath(t)
		defer os.Remove(s)

		listener, err := net.Listen("unix", s)
		require.NoError(t, err)
		defer listener.Close()

		version := uint64(1)
		data := []byte("test")

		go func() {
			conn, err := listener.Accept()
			require.NoError(t, err)

			sendVersionUpdate(t, conn, version, data)
			msg, err := waitForMessage(conn)
			require.NoError(t, err)

			req := parseAndValidateUpdateRequest(t, msg)
			assert.Equal(t, version, req.Version)
			assert.Equal(t, []byte("new"), req.Data)

			sendUpdateResponse(t, conn, interstate.UpdateResponse{
				RequestID: req.RequestID,
				Version:   version + 1,
				Data:      req.Data,
				Error:     interstate.ResponseErrorNone,
			})
		}()

		ds := mocks.NewDatastore(t)
		ds.EXPECT().Put(version, data).Return(nil)

		f, _, err := interstate.NewFollowerNode(s, ds)
		require.NoError(t, err)
		defer f.Close()

		ds.EXPECT().Get().Return(version, data, nil).Once()
		ds.EXPECT().Put(version+1, []byte("new")).Return(nil)
		err = f.Write(version, []byte("new"))
		assert.NoError(t, err)

		ds.EXPECT().Get().Return(version+1, []byte("new"), nil)

		actualVersion, err := f.Version()
		assert.NoError(t, err)

		actualData, err := f.Data()
		assert.NoError(t, err)

		assert.Equal(t, version+1, actualVersion)
		assert.Equal(t, []byte("new"), actualData)
	})
}

func TestFollower_HandleUpdates(t *testing.T) {
	s := socketPath(t)
	defer os.Remove(s)

	listener, err := net.Listen("unix", s)
	require.NoError(t, err)
	defer listener.Close()

	version := uint64(1)
	data := []byte("test")

	updates := []string{
		"one",
		"two",
		"three",
		"four",
	}
	serverDone := make(chan struct{})

	go func() {
		conn, err := listener.Accept()
		require.NoError(t, err)

		sendVersionUpdate(t, conn, version, data)

		for _, u := range updates {
			version++
			sendVersionUpdate(t, conn, version, []byte(u))
		}

		close(serverDone)
		conn.Close()
	}()

	ds := mocks.NewDatastore(t)
	ds.EXPECT().Put(version, data).Return(nil).Once()
	ds.EXPECT().Put(mock.Anything, mock.Anything).Return(nil).Times(len(updates))

	f, _, err := interstate.NewFollowerNode(s, ds)
	require.NoError(t, err)
	defer f.Close()

	<-serverDone
	time.Sleep(10 * time.Millisecond)

	ds.EXPECT().Get().Return(version, []byte(updates[len(updates)-1]), nil)

	actualVersion, err := f.Version()
	assert.NoError(t, err)

	actualData, err := f.Data()
	assert.NoError(t, err)

	assert.Equal(t, version, actualVersion, "expected version to be updated")
	assert.Equal(t, []byte(updates[len(updates)-1]), actualData, "expected data to be updated")
}

func sendVersionUpdate(t *testing.T, conn net.Conn, version uint64, data []byte) {
	update := &interstate.VersionUpdateMessage{
		Version: version,
		Data:    data,
	}

	body, err := update.Encode()
	require.NoError(t, err)

	msg := interstate.PrependRequestLength(body)
	_, err = conn.Write(msg)
	require.NoError(t, err)
}

func sendUpdateResponse(t *testing.T, conn net.Conn, res interstate.UpdateResponse) {
	body, err := res.Encode()
	require.NoError(t, err)

	msg := interstate.PrependRequestLength(body)
	_, err = conn.Write(msg)
	require.NoError(t, err)
}

func parseAndValidateUpdateRequest(t *testing.T, req []byte) interstate.UpdateRequest {
	mt := interstate.GetMessageType(req)
	assert.Equal(t, interstate.MessageTypeUpdateRequest, mt)

	update := interstate.UpdateRequest{}
	err := update.Decode(req)
	require.NoError(t, err)

	return update
}
