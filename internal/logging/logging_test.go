package logging

import (
	"fmt"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func Test_New_File_Handler(
	t *testing.T,
) {

	filename := lo.RandomString(10, lo.LowerCaseLettersCharset)
	path := fmt.Sprintf("/tmp/%s", filename)
	defer os.Remove(path)

	config := spiconfig.LoggerFileConfig{
		Enabled:  lo.ToPtr(true),
		Path:     path,
		Rotate:   lo.ToPtr(true),
		MaxSize:  lo.ToPtr("5MB"),
		Compress: false,
	}

	_, _, err := newFileHandler(config)
	assert.Nil(t, err)
}

func Test_New_File_Handler_Max_Duration(
	t *testing.T,
) {

	filename := lo.RandomString(10, lo.LowerCaseLettersCharset)
	path := fmt.Sprintf("/tmp/%s", filename)
	defer os.Remove(path)

	config := spiconfig.LoggerFileConfig{
		Enabled:     lo.ToPtr(true),
		Path:        path,
		Rotate:      lo.ToPtr(true),
		MaxDuration: lo.ToPtr(600),
		Compress:    false,
	}

	_, _, err := newFileHandler(config)
	assert.Nil(t, err)
}

func Test_New_File_Handler_Cache(
	t *testing.T,
) {

	filename := lo.RandomString(10, lo.LowerCaseLettersCharset)
	path := fmt.Sprintf("/tmp/%s", filename)
	defer os.Remove(path)

	config := spiconfig.LoggerFileConfig{
		Enabled:  lo.ToPtr(true),
		Path:     path,
		Rotate:   lo.ToPtr(true),
		MaxSize:  lo.ToPtr("5MB"),
		Compress: false,
	}

	cached, _, err := newFileHandler(config)
	assert.Nil(t, err)
	assert.False(t, cached)

	cached, _, err = newFileHandler(config)
	assert.Nil(t, err)
	assert.True(t, cached)
}
