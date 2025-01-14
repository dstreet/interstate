package interstate

import (
	"fmt"
	"io"
)

type Version struct {
	version uint64
	data    []byte
	writer  VersionWriter
}

type WriterFactory func(version uint64) (io.WriteCloser, error)

// WriteCloser is an interface alias for io.WriterCloser.
// It is used solely for mocking purposes.
type WriteCloser io.WriteCloser

type VersionWriter interface {
	Write(version uint64, data []byte) error
}

func NewVersion(v uint64, data []byte, writer VersionWriter) *Version {
	return &Version{
		version: v,
		data:    data,
		writer:  writer,
	}
}

func (v *Version) Version() uint64 {
	return v.version
}

func (v *Version) Bytes() []byte {
	return v.data
}

func (v *Version) Update(data []byte) error {
	if err := v.writer.Write(v.version, data); err != nil {
		return fmt.Errorf("failed to write update: %w", err)
	}

	return nil
}
