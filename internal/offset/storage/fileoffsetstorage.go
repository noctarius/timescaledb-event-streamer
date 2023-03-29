package storage

import (
	"bytes"
	"encoding/binary"
	"github.com/docker/docker/pkg/ioutils"
	"github.com/go-errors/errors"
	"github.com/noctarius/event-stream-prototype/internal/offset"
	"os"
	"path/filepath"
)

type fileOffsetStorage struct {
	path    string
	offsets map[string]offset.Offset
}

func NewFileOffsetStorage(path string) (OffsetStorage, error) {
	directory := filepath.Dir(path)
	fi, err := os.Stat(directory)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(directory, 0777); err != nil {
				return nil, errors.Wrap(err, 0)
			}
		} else {
			return nil, errors.Wrap(err, 0)
		}
	}

	if !fi.IsDir() {
		return nil, errors.Errorf(
			"path '%s' cannot be created since the parent-path '%s' is no directory", path, directory,
		)
	}

	fi, err = os.Stat(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, errors.Wrap(err, 0)
		}
	}

	if fi.IsDir() {
		return nil, errors.Errorf("path '%s' exists already but is not a file", path)
	}

	return &fileOffsetStorage{
		path:    path,
		offsets: make(map[string]offset.Offset, 0),
	}, nil
}

func (f *fileOffsetStorage) Start() error {
	return f.Load()
}

func (f *fileOffsetStorage) Stop() error {
	return nil
}

func (f *fileOffsetStorage) Save() error {
	writer, err := ioutils.NewAtomicFileWriter(f.path, 0777)
	if err != nil {
		return errors.Wrap(err, 0)
	}
	defer writer.Close()

	buffer := make([]byte, 4)
	writeUint32 := func(val uint32) (int, error) {
		binary.BigEndian.PutUint32(buffer[0:4], val)
		return writer.Write(buffer[0:4])
	}

	offsetBuffer := make([]byte, 65535)
	writeOffsetWithLength := func(val offset.Offset) (int, error) {
		wb := bytes.NewBuffer(offsetBuffer)
		if err := binary.Write(wb, binary.BigEndian, val); err != nil {
			return 0, errors.Wrap(err, 0)
		}

		length := uint32(wb.Len())
		if _, err := writeUint32(length); err != nil {
			return 0, errors.Wrap(err, 0)
		}

		if _, err := writer.Write(offsetBuffer[0:length]); err != nil {
			return 0, errors.Wrap(err, 0)
		}
		return 4 + int(length), nil
	}

	writeStringWithLength := func(val string) (int, error) {
		byteString := []byte(val)
		if _, err := writeUint32(uint32(len(byteString))); err != nil {
			return 0, errors.Wrap(err, 0)
		}

		if _, err := writer.Write(byteString); err != nil {
			return 0, errors.Wrap(err, 0)
		}
		return 4 + len(byteString), nil
	}

	numOfOffsets := uint32(len(f.offsets))
	if _, err := writeUint32(numOfOffsets); err != nil {
		return errors.Wrap(err, 0)
	}

	for key, value := range f.offsets {
		if _, err := writeStringWithLength(key); err != nil {
			return errors.Wrap(err, 0)
		}
		if _, err := writeOffsetWithLength(value); err != nil {
			return errors.Wrap(err, 0)
		}
	}

	return nil
}

func (f *fileOffsetStorage) Load() error {
	fi, err := os.Stat(f.path)
	if err != nil {
		if !os.IsNotExist(err) {
			return errors.Wrap(err, 0)
		} else {
			// Reset internal map
			f.offsets = make(map[string]offset.Offset, 0)
			return nil
		}
	}

	if fi.IsDir() {
		return errors.Errorf("path '%s' exists already but is not a file", f.path)
	}

	file, err := os.Open(f.path)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	buffer := make([]byte, 65535)

	readOffset := int64(0)
	readUint32 := func() (uint32, error) {
		n, err := file.ReadAt(buffer[0:4], readOffset)
		if err != nil {
			return 0, errors.Wrap(err, 0)
		}
		readOffset += int64(n)
		return binary.BigEndian.Uint32(buffer[0:4]), nil
	}

	readDataWithLength := func() (uint32, error) {
		length, err := readUint32()
		if err != nil {
			return 0, errors.Wrap(err, 0)
		}

		n, err := file.ReadAt(buffer[0:length], readOffset)
		if err != nil {
			return 0, errors.Wrap(err, 0)
		}
		readOffset += int64(n)
		return length, nil
	}

	numOfOffsets, err := readUint32()
	if err != nil {
		return errors.Wrap(err, 0)
	}

	for i := uint32(0); i < numOfOffsets; i++ {
		length, err := readDataWithLength()
		if err != nil {
			return errors.Wrap(err, 0)
		}
		key := string(buffer[0:length])

		length, err = readDataWithLength()
		if err != nil {
			return errors.Wrap(err, 0)
		}

		value := offset.Offset{}
		if err := binary.Read(bytes.NewReader(buffer[0:length]), binary.BigEndian, &value); err != nil {
			return errors.Wrap(err, 0)
		}

		f.offsets[key] = value
	}
	return nil
}

func (f *fileOffsetStorage) Get() (map[string]offset.Offset, error) {
	return f.offsets, nil
}

func (f *fileOffsetStorage) Set(key string, value offset.Offset) error {
	f.offsets[key] = value
	return nil
}
