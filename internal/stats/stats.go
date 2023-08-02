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

package stats

import (
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/version"
	"github.com/segmentio/stats"
	"github.com/segmentio/stats/procstats"
	"github.com/segmentio/stats/prometheus"
	"golang.org/x/net/context"
	"net/http"
)

type Service struct {
	statsEnabled bool
	handler      *prometheus.Handler
	engine       *stats.Engine
	server       *http.Server
}

func NewStatsService(
	c *config.Config,
) *Service {

	statsHandler := &prometheus.Handler{
		TrimPrefix: version.BinName,
	}

	statsEnabled := config.GetOrDefault(c, config.PropertyStatsEnabled, true)
	runtimeStatsEnabled := config.GetOrDefault(c, config.PropertyRuntimeStatsEnabled, true)

	engine := stats.NewEngine(version.BinName, statsHandler)
	if runtimeStatsEnabled {
		runtimeMetrics := procstats.NewGoMetricsWith(engine)
		procstats.StartCollector(runtimeMetrics)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", statsHandler.ServeHTTP)

	return &Service{
		statsEnabled: statsEnabled,
		handler:      statsHandler,
		engine:       engine,
		server: &http.Server{
			Addr:    ":8081",
			Handler: mux,
		},
	}
}

func (s *Service) Start() error {
	if s.statsEnabled {
		go func() {
			err := s.server.ListenAndServe()
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				panic(err)
			}
		}()
	}
	return nil
}

func (s *Service) Stop() error {
	if !s.statsEnabled {
		return nil
	}
	return s.server.Shutdown(context.Background())
}

func (s *Service) NewReporter(
	prefix string,
) *Reporter {

	engine := s.engine.WithPrefix(prefix)
	return &Reporter{
		statsEnabled: s.statsEnabled,
		engine:       engine,
	}
}
