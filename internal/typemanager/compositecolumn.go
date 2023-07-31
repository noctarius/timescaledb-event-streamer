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

package typemanager

import (
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
)

type compositeColumn struct {
	name      string
	typ       pgtypes.PgType
	modifiers int
	nullable  bool
}

func newCompositeColumn(
	name string, typ pgtypes.PgType, modifiers int, nullable bool,
) *compositeColumn {

	return &compositeColumn{
		name:      name,
		typ:       typ,
		modifiers: modifiers,
		nullable:  nullable,
	}
}

func (cc *compositeColumn) Name() string {
	return cc.name
}

func (cc *compositeColumn) Type() pgtypes.PgType {
	return cc.typ
}

func (cc *compositeColumn) Modifiers() int {
	return cc.modifiers
}

func (cc *compositeColumn) Nullable() bool {
	return cc.nullable
}

func (cc *compositeColumn) SchemaBuilder() schema.Builder {
	return cc.typ.SchemaBuilder().FieldName(cc.name).Clone()
}
