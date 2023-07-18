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

package systemcatalog

// SystemEntity represents an entity defined by
// its canonical elements (schema and table names)
type SystemEntity interface {
	// SchemaName returns the schema name of the entity
	SchemaName() string
	// TableName returns the table name of the entity
	TableName() string
	// CanonicalName returns the canonical name of the entity >>schema.table<<
	CanonicalName() string
}

type baseSystemEntity struct {
	schemaName            string
	tableName             string
	resolvedCanonicalName *string
}

// NewSystemEntity instantiates a new basic SystemEntity
func NewSystemEntity(schemaName, tableName string) SystemEntity {
	return &baseSystemEntity{
		schemaName: schemaName,
		tableName:  tableName,
	}
}

func (bse *baseSystemEntity) SchemaName() string {
	return bse.schemaName
}

func (bse *baseSystemEntity) TableName() string {
	return bse.tableName
}

func (bse *baseSystemEntity) CanonicalName() string {
	if bse.resolvedCanonicalName == nil {
		relationKey := MakeRelationKey(bse.schemaName, bse.tableName)
		bse.resolvedCanonicalName = &relationKey
	}
	return *bse.resolvedCanonicalName
}
