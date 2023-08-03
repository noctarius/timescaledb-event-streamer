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

package containers

import (
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_RelationCache_Set_LowerBound_Update(
	t *testing.T,
) {

	msg1 := &pgtypes.RelationMessage{
		RelationID: 10256,
	}
	msg2 := &pgtypes.RelationMessage{
		RelationID: 12000,
	}
	msg3 := &pgtypes.RelationMessage{
		RelationID: 9999,
	}

	cache := NewRelationCache[*pgtypes.RelationMessage]()

	cache.Set(msg1.RelationID, msg1)
	msg1Back, present := cache.Get(msg1.RelationID)
	assert.True(t, present)
	assert.Equal(t, msg1, msg1Back)

	cache.Set(msg2.RelationID, msg2)
	msg1Back, present = cache.Get(msg1.RelationID)
	assert.True(t, present)
	assert.Equal(t, msg1, msg1Back)
	msg2Back, present := cache.Get(msg2.RelationID)
	assert.True(t, present)
	assert.Equal(t, msg2, msg2Back)

	cache.Set(msg3.RelationID, msg3)
	msg1Back, present = cache.Get(msg1.RelationID)
	assert.True(t, present)
	assert.Equal(t, msg1, msg1Back)
	msg2Back, present = cache.Get(msg2.RelationID)
	assert.True(t, present)
	assert.Equal(t, msg2, msg2Back)
	msg3Back, present := cache.Get(msg3.RelationID)
	assert.True(t, present)
	assert.Equal(t, msg3, msg3Back)
}
