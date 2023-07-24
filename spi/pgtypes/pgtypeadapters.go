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

import (
	"fmt"
	"github.com/jackc/pglogrepl"
	"github.com/samber/lo"
	"strconv"
	"strings"
)

type LSN pglogrepl.LSN

func (lsn LSN) String() string {
	return pglogrepl.LSN(lsn).String()
}

type XLogData struct {
	pglogrepl.XLogData

	DatabaseName string
	LastBegin    LSN
	LastCommit   LSN
	Xid          uint32
}

type BeginMessage pglogrepl.BeginMessage

func (m BeginMessage) String() string {
	builder := strings.Builder{}
	builder.WriteString("{")
	builder.WriteString(fmt.Sprintf("messageType:%s ", m.Type().String()))
	builder.WriteString(fmt.Sprintf("finalLSN:%s ", m.FinalLSN))
	builder.WriteString(fmt.Sprintf("commitTime:%s ", m.CommitTime.String()))
	builder.WriteString(fmt.Sprintf("xid:%d", m.Xid))
	builder.WriteString("}")
	return builder.String()
}

type CommitMessage pglogrepl.CommitMessage

func (m CommitMessage) String() string {
	builder := strings.Builder{}
	builder.WriteString("{")
	builder.WriteString(fmt.Sprintf("messageType:%s ", m.Type().String()))
	builder.WriteString(fmt.Sprintf("flags:%d ", m.Flags))
	builder.WriteString(fmt.Sprintf("commitLSN:%s ", m.CommitLSN))
	builder.WriteString(fmt.Sprintf("transactionEndLSN:%s ", m.TransactionEndLSN))
	builder.WriteString(fmt.Sprintf("commitTime:%s", m.CommitTime.String()))
	builder.WriteString("}")
	return builder.String()
}

type OriginMessage pglogrepl.OriginMessage

func (m OriginMessage) String() string {
	builder := strings.Builder{}
	builder.WriteString("{")
	builder.WriteString(fmt.Sprintf("messageType:%s ", m.Type().String()))
	builder.WriteString(fmt.Sprintf("name:%s ", m.Name))
	builder.WriteString(fmt.Sprintf("commitLSN:%s", m.CommitLSN))
	builder.WriteString("}")
	return builder.String()
}

type RelationMessage pglogrepl.RelationMessage

func (m RelationMessage) String() string {
	builder := strings.Builder{}
	builder.WriteString("{")
	builder.WriteString(fmt.Sprintf("messageType:%s ", m.Type().String()))
	builder.WriteString(fmt.Sprintf("relationId:%d ", m.RelationID))
	builder.WriteString(fmt.Sprintf("namespace:%s ", m.Namespace))
	builder.WriteString(fmt.Sprintf("relationName:%s ", m.RelationName))
	builder.WriteString(fmt.Sprintf("replicaIdentity:%d ", m.ReplicaIdentity))
	builder.WriteString(fmt.Sprintf("columnNum:%d ", m.ColumnNum))
	builder.WriteString("columns:[")
	for i, column := range m.Columns {
		builder.WriteString("{")
		builder.WriteString(fmt.Sprintf("name:%s ", column.Name))
		builder.WriteString(fmt.Sprintf("flags:%d ", column.Flags))
		builder.WriteString(fmt.Sprintf("dataType:%d ", column.DataType))
		builder.WriteString(fmt.Sprintf("typeModifier:%d", column.TypeModifier))
		builder.WriteString("}")
		if i < len(m.Columns)-1 {
			builder.WriteString(" ")
		}
	}
	builder.WriteString("]}")
	return builder.String()
}

type TypeMessage pglogrepl.TypeMessage

func (m TypeMessage) String() string {
	builder := strings.Builder{}
	builder.WriteString("{")
	builder.WriteString(fmt.Sprintf("messageType:%s ", m.Type().String()))
	builder.WriteString(fmt.Sprintf("namespace:%s ", m.Namespace))
	builder.WriteString(fmt.Sprintf("name:%s ", m.Name))
	builder.WriteString(fmt.Sprintf("dataType:%d", m.DataType))
	builder.WriteString("}")
	return builder.String()
}

type TruncateMessage pglogrepl.TruncateMessage

func (m TruncateMessage) String() string {
	builder := strings.Builder{}
	builder.WriteString("{")
	builder.WriteString(fmt.Sprintf("messageType:%s ", m.Type().String()))
	builder.WriteString(fmt.Sprintf("option:%d ", m.Option))
	builder.WriteString(fmt.Sprintf("relationNum:%d ", m.RelationNum))
	builder.WriteString(fmt.Sprintf("relationIds:[%s]", strings.Join(
		lo.Map(m.RelationIDs, func(element uint32, _ int) string {
			return strconv.FormatUint(uint64(element), 10)
		}), ", ",
	)))
	builder.WriteString("}")
	return builder.String()
}

type InsertMessage struct {
	*pglogrepl.InsertMessage
	NewValues map[string]any
}

func (m InsertMessage) String() string {
	builder := strings.Builder{}
	builder.WriteString("{")
	builder.WriteString(fmt.Sprintf("messageType:%s ", m.Type().String()))
	builder.WriteString(fmt.Sprintf("newValues:%+v", m.NewValues))
	builder.WriteString("}")
	return builder.String()
}

type UpdateMessage struct {
	*pglogrepl.UpdateMessage
	OldValues map[string]any
	NewValues map[string]any
}

func (m UpdateMessage) String() string {
	builder := strings.Builder{}
	builder.WriteString("{")
	builder.WriteString(fmt.Sprintf("messageType:%s ", m.Type().String()))
	builder.WriteString(fmt.Sprintf("oldValues:%+v ", m.OldValues))
	builder.WriteString(fmt.Sprintf("newValues:%+v", m.NewValues))
	builder.WriteString("}")
	return builder.String()
}

type DeleteMessage struct {
	*pglogrepl.DeleteMessage
	OldValues map[string]any
}

func (m DeleteMessage) String() string {
	builder := strings.Builder{}
	builder.WriteString("{")
	builder.WriteString(fmt.Sprintf("messageType:%s ", m.Type().String()))
	builder.WriteString(fmt.Sprintf("oldValues:%+v", m.OldValues))
	builder.WriteString("}")
	return builder.String()
}
