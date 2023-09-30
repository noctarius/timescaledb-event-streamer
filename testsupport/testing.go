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

package testsupport

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/samber/lo"
	"math/rand"
	"os"
	"strings"
	"time"
)

const (
	DatabaseSchema = "tsdb"
)

func CreateTempFile(
	pattern string,
) (string, error) {

	f, err := os.CreateTemp("", pattern)
	if err != nil {
		return "", err
	}
	defer f.Close()
	return f.Name(), nil
}

func CreateHypertable(
	pool *pgxpool.Pool, timeDimension string, chunkSize time.Duration, columns ...Column,
) (string, string, error) {

	tableName := randomTableName()
	tx, err := pool.Begin(context.Background())
	if err != nil {
		tx.Rollback(context.Background())
		return "", "", err
	}

	columnDefinitions := make([]string, len(columns))
	for i, column := range columns {
		columnDefinitions[i] = toDefinition(column)
	}

	query := fmt.Sprintf("CREATE TABLE \"%s\".\"%s\" (%s)", DatabaseSchema,
		tableName, strings.Join(columnDefinitions, ", "))

	if _, err := tx.Exec(context.Background(), query); err != nil {
		tx.Rollback(context.Background())
		return "", "", err
	}

	query = fmt.Sprintf(
		"SELECT create_hypertable('%s.%s', '%s', chunk_time_interval := interval '%d seconds')",
		DatabaseSchema, tableName, timeDimension, int64(chunkSize.Seconds()),
	)
	if _, err := tx.Exec(context.Background(), query); err != nil {
		tx.Rollback(context.Background())
		return "", "", err
	}

	tx.Commit(context.Background())
	return DatabaseSchema, tableName, nil
}

func CreateVanillaTable(
	pool *pgxpool.Pool, columns ...Column,
) (string, string, error) {

	tableName := randomTableName()
	tx, err := pool.Begin(context.Background())
	if err != nil {
		tx.Rollback(context.Background())
		return "", "", err
	}

	columnDefinitions := make([]string, len(columns))
	for i, column := range columns {
		columnDefinitions[i] = toDefinition(column)
	}

	query := fmt.Sprintf("CREATE TABLE \"%s\".\"%s\" (%s)", DatabaseSchema,
		tableName, strings.Join(columnDefinitions, ", "))

	if _, err := tx.Exec(context.Background(), query); err != nil {
		tx.Rollback(context.Background())
		return "", "", err
	}

	tx.Commit(context.Background())
	return DatabaseSchema, tableName, nil
}

func RandomNumber(
	min, max int,
) int {

	return min + rand.Intn(max-min)
}

func randomTableName() string {
	return lo.RandomString(20, lo.LowerCaseLettersCharset)
}

func toDefinition(
	column Column,
) string {

	builder := strings.Builder{}
	builder.WriteString(column.Name())
	builder.WriteString(" ")
	builder.WriteString(column.PgType())
	if !column.IsNullable() {
		builder.WriteString(" NOT NULL")
	}
	if column.DefaultValue() != nil {
		builder.WriteString(fmt.Sprintf(" DEFAULT '%s'", *column.DefaultValue()))
	}
	if column.IsPrimaryKey() {
		builder.WriteString(" PRIMARY KEY")
	}
	return builder.String()
}
