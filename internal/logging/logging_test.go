/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
