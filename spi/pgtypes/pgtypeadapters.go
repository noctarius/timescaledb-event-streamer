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

import "github.com/jackc/pglogrepl"

type LSN pglogrepl.LSN

func (lsn LSN) String() string {
	return pglogrepl.LSN(lsn).String()
}

type XLogData struct {
	pglogrepl.XLogData

	LastBegin  LSN
	LastCommit LSN
	Xid        uint32
}

type BeginMessage pglogrepl.BeginMessage

type CommitMessage pglogrepl.CommitMessage

type OriginMessage pglogrepl.OriginMessage

type RelationMessage pglogrepl.RelationMessage

type TypeMessage pglogrepl.TypeMessage

type TruncateMessage pglogrepl.TruncateMessage

type InsertMessage struct {
	*pglogrepl.InsertMessage
	NewValues map[string]any
}

type UpdateMessage struct {
	*pglogrepl.UpdateMessage
	OldValues map[string]any
	NewValues map[string]any
}

type DeleteMessage struct {
	*pglogrepl.DeleteMessage
	OldValues map[string]any
}
