package appendonly

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

type Datastore struct {
	fp          string
	file        *os.File
	fileVersion FileVersionHeader
	offset      OffsetHeader
	nextOffset  NextOffsetHeader
}

type (
	FileVersionHeader uint16
	OffsetHeader      uint64
	NextOffsetHeader  uint64
	VersionHeader     uint64
)

const (
	FileVersionHeaderFieldSize = 2
	OffsetHeaderFieldSize      = 8
	NextOffsetHeaderFieldSize  = 8
	VersionHeaderFieldSize     = 8
)

const (
	FileVersionHeaderPos = 0
	OffsetHeaderPos      = FileVersionHeaderPos + FileVersionHeaderFieldSize
	NextOffsetHeaderPos  = OffsetHeaderPos + OffsetHeaderFieldSize
	VersionHeaderPos     = NextOffsetHeaderPos + NextOffsetHeaderFieldSize
)

var (
	ErrFileNotOpen           = fmt.Errorf("file is not open")
	ErrUnexpectedFileVersion = fmt.Errorf("unexpected file version")
	ErrOffsetOutOfRange      = fmt.Errorf("offset is out of range")
	ErrNextOffsetOutOfRange  = fmt.Errorf("offset is greater than next offset")
)

func NewDatastore(path string) *Datastore {
	return &Datastore{fp: path, fileVersion: 1}
}

func (aof *Datastore) Open() error {
	if aof.file != nil {
		return nil
	}

	f, err := os.OpenFile(aof.fp, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}

	info, err := os.Stat(aof.fp)
	if err != nil {
		return fmt.Errorf("failed to get file stats: %w", err)
	}

	aof.file = f

	// If this is a newly created file or an empty file, skip reading the headers
	if info.Size() == 0 {
		return nil
	}

	fileVersion, err := aof.readFileVersionHeader()
	if err != nil {
		return err
	}

	if fileVersion != aof.fileVersion {
		return ErrUnexpectedFileVersion
	}

	aof.offset, err = aof.readOffsetHeader()
	if err != nil {
		return err
	}

	aof.nextOffset, err = aof.readNextOffsetHeader()
	if err != nil {
		return err
	}

	// Offset cannot be greater than next offset
	if uint64(aof.offset) > uint64(aof.nextOffset) {
		return ErrNextOffsetOutOfRange
	}

	// Offset cannot exceed the file size
	if int64(aof.offset) > info.Size() {
		return ErrOffsetOutOfRange
	}

	return nil
}

func (aof *Datastore) Close() error {
	if aof.file == nil {
		return nil
	}

	if err := aof.file.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	aof.file = nil
	return nil
}

func (aof *Datastore) Get() (uint64, []byte, error) {
	if aof.file == nil {
		return 0, nil, ErrFileNotOpen
	}

	length := int(aof.nextOffset) - int(aof.offset)
	data := make([]byte, length)

	version, err := aof.readVersionHeader()
	if err != nil {
		return 0, nil, err
	}

	aof.file.Seek(aof.headerOffset()+int64(aof.offset), 0)

	n, err := aof.file.Read(data)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read data: %w", err)
	}

	if n != length {
		return 0, nil, fmt.Errorf("failed to read data: short read")
	}

	return uint64(version), data, nil
}

func (aof *Datastore) Put(version uint64, data []byte) error {
	if aof.file == nil {
		return ErrFileNotOpen
	}

	aof.file.Seek(aof.headerOffset()+int64(aof.nextOffset), 0)
	n, err := aof.file.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	if n != len(data) {
		return fmt.Errorf("failed to write data: short write")
	}

	aof.offset = OffsetHeader(aof.nextOffset)
	aof.nextOffset = NextOffsetHeader(uint64(aof.nextOffset) + uint64(len(data)))

	if err := aof.writeHeaders(version); err != nil {
		return err
	}

	return nil
}

func (aof *Datastore) headerOffset() int64 {
	return FileVersionHeaderFieldSize +
		OffsetHeaderFieldSize +
		NextOffsetHeaderFieldSize +
		VersionHeaderFieldSize
}

func (aof *Datastore) writeHeaders(version uint64) error {
	if aof.file == nil {
		return ErrFileNotOpen
	}

	headerBytes := make([]byte, aof.headerOffset())
	writePos := 0

	binary.BigEndian.PutUint16(headerBytes[writePos:writePos+FileVersionHeaderFieldSize], uint16(aof.fileVersion))
	writePos += FileVersionHeaderFieldSize

	binary.BigEndian.PutUint64(headerBytes[writePos:writePos+OffsetHeaderFieldSize], uint64(aof.offset))
	writePos += OffsetHeaderFieldSize

	binary.BigEndian.PutUint64(headerBytes[writePos:writePos+NextOffsetHeaderFieldSize], uint64(aof.nextOffset))
	writePos += NextOffsetHeaderFieldSize

	binary.BigEndian.PutUint64(headerBytes[writePos:writePos+VersionHeaderFieldSize], uint64(version))
	writePos += VersionHeaderFieldSize

	aof.file.Seek(0, 0)
	if _, err := aof.file.Write(headerBytes); err != nil {
		return fmt.Errorf("failed to write headers: %w", err)
	}

	return nil
}

func (aof *Datastore) readFileVersionHeader() (FileVersionHeader, error) {
	if aof.file == nil {
		return 0, ErrFileNotOpen
	}

	aof.file.Seek(FileVersionHeaderPos, 0)
	bb := make([]byte, FileVersionHeaderFieldSize)
	if _, err := aof.file.Read(bb); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, nil
		}

		return 0, fmt.Errorf("failed to read file version: %w", err)
	}

	fv := binary.BigEndian.Uint16(bb)
	return FileVersionHeader(fv), nil
}

func (aof *Datastore) readOffsetHeader() (OffsetHeader, error) {
	if aof.file == nil {
		return 0, ErrFileNotOpen
	}

	aof.file.Seek(OffsetHeaderPos, 0)
	bb := make([]byte, OffsetHeaderFieldSize)
	if _, err := aof.file.Read(bb); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, nil
		}

		return 0, fmt.Errorf("failed to read offset: %w", err)
	}

	fv := binary.BigEndian.Uint64(bb)
	return OffsetHeader(fv), nil
}

func (aof *Datastore) readNextOffsetHeader() (NextOffsetHeader, error) {
	if aof.file == nil {
		return 0, ErrFileNotOpen
	}

	aof.file.Seek(NextOffsetHeaderPos, 0)
	bb := make([]byte, NextOffsetHeaderFieldSize)
	if _, err := aof.file.Read(bb); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, nil
		}

		return 0, fmt.Errorf("failed to read next offset: %w", err)
	}

	fv := binary.BigEndian.Uint64(bb)
	return NextOffsetHeader(fv), nil
}

func (aof *Datastore) readVersionHeader() (VersionHeader, error) {
	if aof.file == nil {
		return 0, ErrFileNotOpen
	}

	aof.file.Seek(VersionHeaderPos, 0)
	bb := make([]byte, VersionHeaderFieldSize)
	if _, err := aof.file.Read(bb); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, nil
		}

		return 0, fmt.Errorf("failed to read version: %w", err)
	}

	fv := binary.BigEndian.Uint64(bb)
	return VersionHeader(fv), nil
}
