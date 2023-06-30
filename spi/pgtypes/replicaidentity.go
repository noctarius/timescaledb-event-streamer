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

package pgtypes

import "fmt"

type ReplicaIdentity string

func (ri *ReplicaIdentity) Scan(src interface{}) error {
	if c, ok := src.(string); ok {
		*ri = ReplicaIdentity(c)
		return nil
	}
	return fmt.Errorf("can not scan %T to ReplicaIdentity", src)
}

const (
	NOTHING ReplicaIdentity = "n"
	FULL    ReplicaIdentity = "f"
	DEFAULT ReplicaIdentity = "d"
	INDEX   ReplicaIdentity = "i"
	UNKNOWN ReplicaIdentity = ""
)

// Description returns a description of this REPLICA IDENTITY
// Values are in sync with debezium project:
// https://github.com/debezium/debezium/blob/main/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/connection/ServerInfo.java
func (ri ReplicaIdentity) Description() string {
	switch ri {
	case NOTHING:
		return "UPDATE and DELETE events will not contain any old values"
	case FULL:
		return "UPDATE AND DELETE events will contain the previous values of all the columns"
	case DEFAULT:
		return "UPDATE and DELETE events will contain previous values only for PK columns"
	case INDEX:
		return "UPDATE and DELETE events will contain previous values only for columns present in the REPLICA IDENTITY index"
	}
	return "Unknown REPLICA IDENTITY"
}

func AsReplicaIdentity(val string) ReplicaIdentity {
	switch val {
	case string(NOTHING):
		return NOTHING
	case string(FULL):
		return FULL
	case string(DEFAULT):
		return DEFAULT
	case string(INDEX):
		return INDEX
	}
	return UNKNOWN
}
