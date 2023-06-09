package replication

import (
	"fmt"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting/logging"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_Replicator_Select_Missing_Tables(t *testing.T) {
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

func Test_Replicator_Select_Missing_Tables_Random_Selection(t *testing.T) {
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
		index := supporting.RandomNumber(0, 1000)
		chunk := knownTables[index]

		if supporting.IndexOfWithMatcher(publishedChunkTables, func(other systemcatalog.SystemEntity) bool {
			return other.CanonicalName() == chunk.CanonicalName()
		}) != -1 {
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

		if supporting.IndexOfWithMatcher(publishedChunkTables, func(other systemcatalog.SystemEntity) bool {
			return other.CanonicalName() == chunk.CanonicalName()
		}) >= 0 {
			mergeChunkTables = append(mergeChunkTables, chunk)
		}

		if supporting.IndexOfWithMatcher(neededChunkTables, func(other systemcatalog.SystemEntity) bool {
			return other.CanonicalName() == chunk.CanonicalName()
		}) >= 0 {
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
