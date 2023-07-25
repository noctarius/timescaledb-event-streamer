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

package replication

import (
	"fmt"
	"github.com/noctarius/timescaledb-event-streamer/internal/logging"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/testsupport"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_Replicator_Select_Missing_Tables(
	t *testing.T,
) {

	knownTables := make([]systemcatalog.SystemEntity, 0, 1000)
	for i := 0; i < 1000; i++ {
		knownTables = append(
			knownTables, systemcatalog.NewSystemEntity(
				"_timescaledb_internal", fmt.Sprintf("_hyper_1_%d", i+1),
			),
		)
	}

	publishedChunkTables := make([]systemcatalog.SystemEntity, 0, 200)
	for i := 0; i < 1000; i = i + 5 {
		publishedChunkTables = append(publishedChunkTables, knownTables[i])
	}

	logger, err := logging.NewLogger("TestLogger")
	if err != nil {
		t.Error(err)
	}

	replicator := &Replicator{
		logger: logger,
	}

	encodedState := func(name string) ([]byte, bool) {
		return nil, false
	}

	getAllChunks := func() []systemcatalog.SystemEntity {
		return knownTables
	}

	readPublishedTables := func() ([]systemcatalog.SystemEntity, error) {
		return publishedChunkTables, nil
	}

	neededChunkTables, err := replicator.collectChunksForPublication(
		encodedState, getAllChunks, readPublishedTables,
	)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, 800, len(neededChunkTables))

	o := 0
	for i := 0; i < 1000; i++ {
		if i%5 == 0 {
			continue
		}
		assert.Equal(t, knownTables[i].CanonicalName(), neededChunkTables[o].CanonicalName())
		o++
	}
}

func Test_Replicator_Select_Missing_Tables_Random_Selection(
	t *testing.T,
) {

	knownTables := make([]systemcatalog.SystemEntity, 0, 1000)
	for i := 0; i < 1000; i++ {
		knownTables = append(
			knownTables, systemcatalog.NewSystemEntity(
				"_timescaledb_internal", fmt.Sprintf("_hyper_1_%d", i+1),
			),
		)
	}

	publishedChunkTables := make([]systemcatalog.SystemEntity, 0, 200)
	for i := 0; i < 200; i++ {
	retry:
		index := testsupport.RandomNumber(0, 1000)
		chunk := knownTables[index]

		if lo.ContainsBy(publishedChunkTables, func(other systemcatalog.SystemEntity) bool {
			return other.CanonicalName() == chunk.CanonicalName()
		}) {
			goto retry
		}
		publishedChunkTables = append(publishedChunkTables, chunk)
	}

	logger, err := logging.NewLogger("TestLogger")
	if err != nil {
		t.Error(err)
	}

	replicator := &Replicator{
		logger: logger,
	}

	encodedState := func(name string) ([]byte, bool) {
		return nil, false
	}

	getAllChunks := func() []systemcatalog.SystemEntity {
		return knownTables
	}

	readPublishedTables := func() ([]systemcatalog.SystemEntity, error) {
		return publishedChunkTables, nil
	}

	neededChunkTables, err := replicator.collectChunksForPublication(
		encodedState, getAllChunks, readPublishedTables,
	)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, 800, len(neededChunkTables))

	mergeChunkTables := make([]systemcatalog.SystemEntity, 0, 1000)
	for i := 0; i < 1000; i++ {
		chunk := knownTables[i]

		if lo.ContainsBy(publishedChunkTables, func(other systemcatalog.SystemEntity) bool {
			return other.CanonicalName() == chunk.CanonicalName()
		}) {
			mergeChunkTables = append(mergeChunkTables, chunk)
		}

		if lo.ContainsBy(neededChunkTables, func(other systemcatalog.SystemEntity) bool {
			return other.CanonicalName() == chunk.CanonicalName()
		}) {
			mergeChunkTables = append(mergeChunkTables, chunk)
		}
	}

	assert.Equal(t, 1000, len(mergeChunkTables))

	for i := 0; i < 1000; i++ {
		this := mergeChunkTables[i]
		for o := 0; o < 1000; o++ {
			if i == o {
				continue
			}

			other := mergeChunkTables[o]
			if this.CanonicalName() == other.CanonicalName() {
				t.Errorf("Duplicate found in merged chunk tables")
			}
		}
	}
}

func Test_Replicator_Filter_Old_Tables_In_State(
	t *testing.T,
) {

	knownTables := make([]systemcatalog.SystemEntity, 0, 11)
	for i := 0; i < 10; i++ {
		knownTables = append(
			knownTables, systemcatalog.NewSystemEntity(
				"_timescaledb_internal", fmt.Sprintf("_hyper_1_%d", i+1),
			),
		)
	}
	knownTables = append(
		knownTables, systemcatalog.NewSystemEntity(
			"_timescaledb_internal", "_hyper_2_1",
		),
	)

	allTables := make([]systemcatalog.SystemEntity, 0, 10)
	for i := 0; i < 10; i++ {
		allTables = append(allTables, knownTables[i])
	}

	logger, err := logging.NewLogger("TestLogger")
	if err != nil {
		t.Error(err)
	}

	replicator := &Replicator{
		logger: logger,
	}

	state, err := encodeKnownChunks(knownTables)
	if err != nil {
		t.Error(err)
	}

	encodedState := func(name string) ([]byte, bool) {
		return state, true
	}

	getAllChunks := func() []systemcatalog.SystemEntity {
		return allTables
	}

	readPublishedTables := func() ([]systemcatalog.SystemEntity, error) {
		return nil, nil
	}

	neededChunkTables, err := replicator.collectChunksForPublication(
		encodedState, getAllChunks, readPublishedTables,
	)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, 10, len(neededChunkTables))
	for i := 0; i < 10; i++ {
		if neededChunkTables[i].CanonicalName() != allTables[i].CanonicalName() {
			t.Errorf(
				"Chunk %s doesn't match %s at index %d",
				neededChunkTables[i].CanonicalName(), allTables[i].CanonicalName(), i,
			)
		}
	}
}
