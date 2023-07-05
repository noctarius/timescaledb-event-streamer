package logging

import (
	"fmt"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func Test_New_File_Handler(t *testing.T) {
	filename := supporting.RandomTextString(10)
	path := fmt.Sprintf("/tmp/%s", filename)
	defer os.Remove(path)

	config := spiconfig.LoggerFileConfig{
		Enabled:  supporting.AddrOf(true),
		Path:     path,
		Rotate:   supporting.AddrOf(true),
		MaxSize:  supporting.AddrOf("5MB"),
		Compress: false,
	}

	_, _, err := newFileHandler(config)
	assert.Nil(t, err)
}

func Test_New_File_Handler_Max_Duration(t *testing.T) {
	filename := supporting.RandomTextString(10)
	path := fmt.Sprintf("/tmp/%s", filename)
	defer os.Remove(path)

	config := spiconfig.LoggerFileConfig{
		Enabled:     supporting.AddrOf(true),
		Path:        path,
		Rotate:      supporting.AddrOf(true),
		MaxDuration: supporting.AddrOf(600),
		Compress:    false,
	}

	_, _, err := newFileHandler(config)
	assert.Nil(t, err)
}

func Test_New_File_Handler_Cache(t *testing.T) {
	filename := supporting.RandomTextString(10)
	path := fmt.Sprintf("/tmp/%s", filename)
	defer os.Remove(path)

	config := spiconfig.LoggerFileConfig{
		Enabled:  supporting.AddrOf(true),
		Path:     path,
		Rotate:   supporting.AddrOf(true),
		MaxSize:  supporting.AddrOf("5MB"),
		Compress: false,
	}

	cached, _, err := newFileHandler(config)
	assert.Nil(t, err)
	assert.False(t, cached)

	cached, _, err = newFileHandler(config)
	assert.Nil(t, err)
	assert.True(t, cached)
}
