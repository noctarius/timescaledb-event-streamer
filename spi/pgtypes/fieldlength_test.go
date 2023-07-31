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
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"testing"
)

var fieldLengthTestCases = []fieldLengthTestCase{
	{
		name:                "bits with length",
		oid:                 pgtype.BitOID,
		modifiers:           3,
		expectedFieldLength: 3,
		expectedApplicable:  true,
	},
	{
		name:                "bits without length",
		oid:                 pgtype.BitOID,
		modifiers:           1,
		expectedFieldLength: -1,
		expectedApplicable:  false,
	},
	{
		name:                "varbits with length",
		oid:                 pgtype.VarbitOID,
		modifiers:           3,
		expectedFieldLength: 3,
		expectedApplicable:  true,
	},
	{
		name:                "varbits without length",
		oid:                 pgtype.VarbitOID,
		modifiers:           -1,
		expectedFieldLength: -1,
		expectedApplicable:  false,
	},
	{
		name:                "bpchars with length",
		oid:                 pgtype.BPCharOID,
		modifiers:           8,
		expectedFieldLength: 4,
		expectedApplicable:  true,
	},
	{
		name:                "bpchars without length",
		oid:                 pgtype.BPCharOID,
		modifiers:           5,
		expectedFieldLength: -1,
		expectedApplicable:  false,
	},
	{
		name:                "varchars with length",
		oid:                 pgtype.VarcharOID,
		modifiers:           8,
		expectedFieldLength: 4,
		expectedApplicable:  true,
	},
	{
		name:                "varchars without length",
		oid:                 pgtype.VarcharOID,
		modifiers:           5,
		expectedFieldLength: -1,
		expectedApplicable:  false,
	},
	{
		name:                "bits array with length",
		oid:                 pgtype.BitArrayOID,
		modifiers:           3,
		expectedFieldLength: 3,
		expectedApplicable:  true,
	},
	{
		name:                "bits array without length",
		oid:                 pgtype.BitArrayOID,
		modifiers:           1,
		expectedFieldLength: -1,
		expectedApplicable:  false,
	},
	{
		name:                "varbits array with length",
		oid:                 pgtype.VarbitArrayOID,
		modifiers:           3,
		expectedFieldLength: 3,
		expectedApplicable:  true,
	},
	{
		name:                "varbits array without length",
		oid:                 pgtype.VarbitArrayOID,
		modifiers:           -1,
		expectedFieldLength: -1,
		expectedApplicable:  false,
	},
	{
		name:                "bpchars array with length",
		oid:                 pgtype.BPCharArrayOID,
		modifiers:           8,
		expectedFieldLength: 4,
		expectedApplicable:  true,
	},
	{
		name:                "bpchars array without length",
		oid:                 pgtype.BPCharArrayOID,
		modifiers:           5,
		expectedFieldLength: -1,
		expectedApplicable:  false,
	},
	{
		name:                "varchars array with length",
		oid:                 pgtype.VarcharArrayOID,
		modifiers:           8,
		expectedFieldLength: 4,
		expectedApplicable:  true,
	},
	{
		name:                "varchars array without length",
		oid:                 pgtype.VarcharArrayOID,
		modifiers:           5,
		expectedFieldLength: -1,
		expectedApplicable:  false,
	},
}

func Test_Field_Length(
	t *testing.T,
) {

	for _, testCase := range fieldLengthTestCases {
		t.Run(testCase.name, func(t *testing.T) {
			typ := &mockPgType{}
			typ.On("Oid").Return(testCase.oid)
			fieldLength, applicable := AsFieldLength(typ, testCase.modifiers)
			assert.Equal(t, testCase.expectedApplicable, applicable)
			assert.Equal(t, testCase.expectedFieldLength, fieldLength)
		})
	}
}

type fieldLengthTestCase struct {
	name                string
	oid                 uint32
	modifiers           int
	expectedFieldLength int
	expectedApplicable  bool
}
