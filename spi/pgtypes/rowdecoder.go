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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type RowDecoderFactory = func(fields []pgconn.FieldDescription) (RowDecoder, error)

type RowDecoder interface {
	DecodeRowsMapAndSink(
		rows pgx.Rows, sink func(values map[string]any) error,
	) error
	DecodeRowsAndSink(
		rows pgx.Rows, sink func(values []any) error,
	) error
	Decode(
		rawRow [][]byte,
	) ([]any, error)
	DecodeAndSink(
		rawRow [][]byte, sink func(values []any) error,
	) error
	DecodeMapAndSink(
		rawRow [][]byte, sink func(values map[string]any) error,
	) error
}
