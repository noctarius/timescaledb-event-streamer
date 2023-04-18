package file

import (
	"encoding/binary"
	"github.com/docker/docker/pkg/ioutils"
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting/logging"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/statestorage"
	"os"
	"path/filepath"
	"sync"
	"time"
)

func init() {
	statestorage.RegisterStateStorage(spiconfig.FileStorage, newFileStateStorage)
}

type fileStateStorage struct {
	path    string
	mutex   sync.Mutex
	logger  *logging.Logger
	offsets map[string]*statestorage.Offset

	stateEncoders map[string]statestorage.StateEncoder
	encodedStates map[string][]byte

	changeCounter  uint64
	ticker         *time.Ticker
	shutdownWaiter *supporting.ShutdownAwaiter
}

func newFileStateStorage(config *spiconfig.Config) (statestorage.Storage, error) {
	path := spiconfig.GetOrDefault(config, spiconfig.PropertyFileStateStoragePath, "")
	if path == "" {
		return nil, errors.Errorf("FileStateStorage needs a path to be configured")
	}
	return NewFileStateStorage(path)
}

func NewFileStateStorage(path string) (statestorage.Storage, error) {
	logger, err := logging.NewLogger("FileStateStorage")
	if err != nil {
		return nil, err
	}

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

	if fi != nil && fi.IsDir() {
		return nil, errors.Errorf("path '%s' exists already but is not a file", path)
	}

	return &fileStateStorage{
		path:           path,
		logger:         logger,
		shutdownWaiter: supporting.NewShutdownAwaiter(),
		offsets:        make(map[string]*statestorage.Offset, 0),
	}, nil
}

func (f *fileStateStorage) Start() error {
	f.logger.Infof("Starting FileStateStorage at %s", f.path)
	if err := f.Load(); err != nil {
		return err
	}

	if f.ticker == nil {
		f.ticker = time.NewTicker(time.Second * 20)
		go f.autoStoreHandler()
	}
	return nil
}

func (f *fileStateStorage) Stop() error {
	f.logger.Infof("Stopping FileStateStorage at %s", f.path)
	f.shutdownWaiter.SignalShutdown()
	if err := f.shutdownWaiter.AwaitDone(); err != nil {
		f.logger.Warnln("Failed to shutdown auto storage in time")
	}
	return f.Save()
}

func (f *fileStateStorage) Save() error {
	f.logger.Infof("Storing FileStateStorage at %s", f.path)



	f.mutex.Lock()
	defer f.mutex.Unlock()

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

	writeOffsetWithLength := func(val *statestorage.Offset) (int, error) {
		data, err := val.MarshalBinary()
		if err != nil {
			return 0, errors.Wrap(err, 0)
		}

		if _, err := writeUint32(uint32(len(data))); err != nil {
			return 0, errors.Wrap(err, 0)
		}

		if _, err := writer.Write(data); err != nil {
			return 0, errors.Wrap(err, 0)
		}
		return 4 + len(data), nil
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

func (f *fileStateStorage) Load() error {
	f.logger.Infof("Loading FileStateStorage at %s", f.path)

	f.mutex.Lock()
	defer f.mutex.Unlock()

	fi, err := os.Stat(f.path)
	if err != nil {
		if !os.IsNotExist(err) {
			return errors.Wrap(err, 0)
		} else {
			// Reset internal map
			f.offsets = make(map[string]*statestorage.Offset, 0)
			return nil
		}
	}

	if fi.IsDir() {
		return errors.Errorf("path '%s' exists already but is not a file", f.path)
	}

	if fi.Size() == 0 {
		// Reset internal map
		f.offsets = make(map[string]*statestorage.Offset, 0)
		return nil
	}

	file, err := os.Open(f.path)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	buffer := make([]byte, fi.Size())
	if _, err := file.Read(buffer); err != nil {
		return errors.Wrap(err, 0)
	}

	readerOffset := int64(0)
	readUint32 := func() uint32 {
		val := binary.BigEndian.Uint32(buffer[readerOffset : readerOffset+4])
		readerOffset += 4
		return val
	}

	readString := func() string {
		length := readUint32()
		val := string(buffer[readerOffset : readerOffset+int64(length)])
		readerOffset += int64(length)
		return val
	}

	readOffset := func() (*statestorage.Offset, error) {
		length := readUint32()
		o := &statestorage.Offset{}
		if err := o.UnmarshalBinary(buffer[readerOffset : readerOffset+int64(length)]); err != nil {
			return nil, err
		}
		readerOffset += int64(length)
		return o, nil
	}

	numOfOffsets := readUint32()
	for i := uint32(0); i < numOfOffsets; i++ {
		key := readString()
		value, err := readOffset()
		if err != nil {
			return errors.Wrap(err, 0)
		}
		f.offsets[key] = value
	}
	return nil
}

func (f *fileStateStorage) Get() (map[string]*statestorage.Offset, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return f.offsets, nil
}

func (f *fileStateStorage) Set(key string, value *statestorage.Offset) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.offsets[key] = value
	f.changeCounter++
	return nil
}

func (f *fileStateStorage) RegisterStateEncoder(name string, encoder statestorage.StateEncoder) {
	f.stateEncoders[name] = encoder
}

func (f *fileStateStorage) EncodedState(key string) (encodedState []byte, present bool) {
	encodedState, present = f.encodedStates[key]
	return
}

func (f *fileStateStorage) SetEncodedState(key string, encodedState []byte) {
	f.encodedStates[key] = encodedState
}

func (f *fileStateStorage) autoStoreHandler() {
	for {
		select {
		case <-f.shutdownWaiter.AwaitShutdownChan():
			f.ticker.Stop()
			f.shutdownWaiter.SignalDone()
			return

		case <-f.ticker.C:
			if f.changeCounter != 0 {
				f.logger.Infof("Auto storing FileStateStorage at %s", f.path)
				if err := f.Save(); err != nil {
					f.logger.Warnf("failed to auto storage offsets: %s", err.Error())
				}
				f.changeCounter = 0
			}
		}
	}
}
