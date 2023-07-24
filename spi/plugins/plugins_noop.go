//go:build !linux && !freebsd && !darwin

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

package plugins

import (
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"runtime"
)

func LoadPlugins(
	config *config.Config,
) error {

	if len(config.Plugins) > 0 {
		return errors.Errorf("Plugins aren't supported on %s, but plugins are defined in config", runtime.GOOS)
	}
	return nil
}
