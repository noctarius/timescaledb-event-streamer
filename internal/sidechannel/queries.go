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

package sidechannel

// region System Information Queries
const queryReadSystemInformation = `
SELECT current_database(), pcs.system_identifier, pcc.timeline_id 
FROM pg_control_system() pcs, pg_control_checkpoint() pcc`

const queryTimescaleDbVersion = `
SELECT extversion
FROM pg_catalog.pg_extension
WHERE extname = 'timescaledb'`

const queryPostgreSqlVersion = `SHOW SERVER_VERSION`

const queryConfiguredWalLevel = `SHOW WAL_LEVEL`

const queryReplicationMarkersEnabled = `SHOW timescaledb.enable_decompression_logrep_markers`

const queryTemplateReadPostgreSqlTypes = `
SELECT DISTINCT ON (t.typname) sp.nspname, t.typname, t.typinput='array_in'::REGPROC,
                               t.typinput='record_in'::REGPROC,
                               t.typtype, t.oid, t.typarray, t.typelem,
                               t.typcategory, t.typbasetype, t.typtypmod,
                               e.enum_values, t.typdelim
FROM pg_catalog.pg_type t
LEFT JOIN (
    SELECT ns.oid AS nspoid, ns.nspname, r.rank
    FROM pg_catalog.pg_namespace ns
    JOIN (
        SELECT row_number() OVER () AS rank, s.s AS nspname
        FROM (
            SELECT r.r AS s FROM unnest(current_schemas(TRUE)) r
            UNION ALL
            SELECT '_timescaledb_internal' AS s
        ) s
    ) r ON ns.nspname = r.nspname
) sp ON sp.nspoid = t.typnamespace
LEFT JOIN (
    SELECT e.enumtypid AS id, array_agg(e.enumlabel) AS enum_values
    FROM pg_catalog.pg_enum e
    GROUP BY 1 
) e ON e.id = t.oid 
WHERE sp.rank IS NOT NULL
  AND t.typtype != 'p'
%s
ORDER BY t.typname DESC, sp.rank, t.oid;`

// endregion

// region Publication Related Queries
const queryTemplateAddTableToPublication = "ALTER PUBLICATION %s ADD TABLE %s"

const queryTemplateDropTableFromPublication = "ALTER PUBLICATION %s DROP TABLE %s"

const queryCreatePublication = "SELECT create_timescaledb_catalog_publication($1, $2)"

const queryTemplateDropPublication = "DROP PUBLICATION IF EXISTS %s"

const queryCheckPublicationExists = "SELECT TRUE FROM pg_publication WHERE pubname = $1"

const queryReadExistingAlreadyPublishedTables = `
SELECT pt.schemaname, pt.tablename
FROM pg_catalog.pg_publication_tables pt
WHERE pt.pubname = $1`

const queryCheckTableExistsInPublication = `
SELECT TRUE
FROM pg_catalog.pg_publication_tables pt
WHERE pt.pubname = $1
  AND pt.schemaname = $2
  AND pt.tablename = $3`

//endregion

// region Replication Slot Related Queries
const queryReadReplicationSlot = `
SELECT plugin, slot_type, restart_lsn, confirmed_flush_lsn
FROM pg_catalog.pg_replication_slots prs
WHERE slot_name = $1`

const queryCheckReplicationSlotExists = `
SELECT TRUE
FROM pg_catalog.pg_replication_slots prs
WHERE slot_name = $1`

// endregion

// region Hypertable / Chunk Related Queries
const queryReadHypertables = `
SELECT h1.id, h1.schema_name, h1.table_name, h1.associated_schema_name, h1.associated_table_prefix,
	 h1.compression_state, h1.compressed_hypertable_id, ca.user_view_schema, ca.user_view_name, c.relreplident
FROM _timescaledb_catalog.hypertable h1
LEFT JOIN timescaledb_information.hypertables h2
	 ON h2.hypertable_schema = h1.schema_name
	AND h2.hypertable_name = h1.table_name
LEFT JOIN _timescaledb_catalog.continuous_agg ca
    ON h1.id = ca.mat_hypertable_id
LEFT JOIN pg_catalog.pg_namespace n
    ON n.nspname = h1.schema_name
LEFT JOIN pg_catalog.pg_class c
    ON c.relname = h1.table_name
   AND c.relnamespace = n.oid;
`

const queryReadChunks = `
SELECT c1.id, c1.hypertable_id, c1.schema_name, c1.table_name, c1.compressed_chunk_id, c1.dropped, c1.status
FROM _timescaledb_catalog.chunk c1
LEFT JOIN timescaledb_information.chunks c2
       ON c2.chunk_schema = c1.schema_name
      AND c2.chunk_name = c1.table_name
ORDER BY c1.hypertable_id, c1.compressed_chunk_id NULLS FIRST, c2.range_start`

const queryReadHypertableSchema = `
SELECT
   c.column_name,
   t.oid::int,
   a.atttypmod,
   CASE WHEN c.is_nullable = 'YES' THEN TRUE ELSE FALSE END AS nullable,
   COALESCE(p.indisprimary, FALSE) AS is_primary_key,
   p.key_seq,
   c.column_default,
   COALESCE(p.indisreplident, FALSE) AS is_replica_ident,
   p.index_name,
   CASE o.option & 1 WHEN 1 THEN 'DESC' ELSE 'ASC' END AS index_column_order,
   CASE o.option & 2 WHEN 2 THEN 'NULLS FIRST' ELSE 'NULLS LAST' END AS index_nulls_order,
   d.column_name IS NOT NULL AS is_dimension,
   COALESCE(d.aligned, FALSE) AS dim_aligned,
   CASE WHEN d.interval_length IS NULL THEN 'space' ELSE 'time' END AS dim_type,
   CASE WHEN d.column_name IS NOT NULL THEN rank() over (ORDER BY d.id) END,
   C.character_maximum_length
FROM information_schema.columns C
LEFT JOIN (
    SELECT
        cl.relname,
        n.nspname,
        a.attname,
        (information_schema._pg_expandarray(i.indkey)).n AS key_seq,
        (information_schema._pg_expandarray(i.indkey)) AS keys,
        a.attnum,
        i.indisreplident,
        i.indisprimary,
        cl2.relname AS index_name,
        i.indoption
    FROM pg_index i, pg_attribute a, pg_class cl, pg_namespace n, pg_class cl2
    WHERE cl.relnamespace = n.oid
      AND a.attrelid = cl.oid
      AND i.indrelid = cl.oid
      AND i.indexrelid = cl2.oid
      AND i.indisprimary
) p ON p.attname = C.column_name AND p.nspname = C.table_schema AND p.relname = C.table_name AND p.attnum = (p.keys).x
LEFT JOIN pg_catalog.pg_namespace nt ON nt.nspname = C.udt_schema
LEFT JOIN pg_catalog.pg_type t ON t.typnamespace = nt.oid AND t.typname = C.udt_name
LEFT JOIN pg_catalog.pg_namespace nc ON nc.nspname = C.table_schema
LEFT JOIN pg_catalog.pg_class cl ON cl.relname = C.table_name AND cl.relnamespace = nc.oid
LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = cl.oid AND a.attnum = C.ordinal_position
LEFT JOIN unnest (p.indoption) WITH ORDINALITY o (OPTION, ORDINALITY) ON p.attnum = o.ordinality
LEFT JOIN _timescaledb_catalog.hypertable h ON h.schema_name = C.table_schema AND h.table_name = C.table_name
LEFT JOIN _timescaledb_catalog.dimension d ON d.hypertable_id = h.id AND d.column_name = C.column_name
WHERE C.table_schema = $1
  AND C.table_name = $2
ORDER BY C.ordinal_position`

const queryReadContinuousAggregateInformation = `
SELECT ca.user_view_schema, ca.user_view_name
FROM _timescaledb_catalog.continuous_agg ca 
WHERE ca.mat_hypertable_id = $1`

// endregion

// region PostgreSQL Catalog Queries
const queryReadReplicaIdentity = `
SELECT c.relreplident::text
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
WHERE n.nspname=$1 AND c.relname=$2`

const queryTemplateSnapshotHighWatermark = `
SELECT %s
FROM %s
ORDER BY %s
LIMIT 1`

const queryCheckUserTablePrivilege = `SELECT HAS_TABLE_PRIVILEGE($1, $2, $3)`

const queryReadCompositeTypeSchema = `
SELECT a.attname,
       a.atttypid,
       a.atttypmod,
       a.attnotnull
FROM pg_catalog.pg_attribute a
RIGHT JOIN pg_catalog.pg_class pc ON pc.reltype = $1
WHERE a.attrelid = pc.oid AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum`

const queryReadVanillaTables = `
SELECT c.oid, t.schemaname, t.tablename, c.relreplident
FROM pg_catalog.pg_tables t
LEFT JOIN pg_catalog.pg_namespace n
    ON n.nspname = t.schemaname
LEFT JOIN pg_catalog.pg_class c
    ON c.relname = t.tablename
   AND c.relnamespace = n.oid
LEFT JOIN _timescaledb_catalog.hypertable h ON h.schema_name = t.schemaname AND h.table_name = t.tablename
WHERE t.schemaname NOT LIKE '_timescaledb_%'
  AND h.id IS NULL`

const queryReadVanillaTableSchema = `
SELECT
   c.column_name,
   t.oid::int,
   a.atttypmod,
   CASE WHEN c.is_nullable = 'YES' THEN TRUE ELSE FALSE END AS nullable,
   COALESCE(p.indisprimary, FALSE) AS is_primary_key,
   p.key_seq,
   c.column_default,
   COALESCE(p.indisreplident, FALSE) AS is_replica_ident,
   p.index_name,
   CASE o.option & 1 WHEN 1 THEN 'DESC' ELSE 'ASC' END AS index_column_order,
   CASE o.option & 2 WHEN 2 THEN 'NULLS FIRST' ELSE 'NULLS LAST' END AS index_nulls_order,
   C.character_maximum_length
FROM information_schema.columns C
LEFT JOIN (
    SELECT
        cl.relname,
        n.nspname,
        a.attname,
        (information_schema._pg_expandarray(i.indkey)).n AS key_seq,
        (information_schema._pg_expandarray(i.indkey)) AS keys,
        a.attnum,
        i.indisreplident,
        i.indisprimary,
        cl2.relname AS index_name,
        i.indoption
    FROM pg_index i, pg_attribute a, pg_class cl, pg_namespace n, pg_class cl2
    WHERE cl.relnamespace = n.oid
      AND a.attrelid = cl.oid
      AND i.indrelid = cl.oid
      AND i.indexrelid = cl2.oid
      AND i.indisprimary
) p ON p.attname = C.column_name AND p.nspname = C.table_schema AND p.relname = C.table_name AND p.attnum = (p.keys).x
LEFT JOIN pg_catalog.pg_namespace nt ON nt.nspname = C.udt_schema
LEFT JOIN pg_catalog.pg_type t ON t.typnamespace = nt.oid AND t.typname = C.udt_name
LEFT JOIN pg_catalog.pg_namespace nc ON nc.nspname = C.table_schema
LEFT JOIN pg_catalog.pg_class cl ON cl.relname = C.table_name AND cl.relnamespace = nc.oid
LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = cl.oid AND a.attnum = C.ordinal_position
LEFT JOIN unnest (p.indoption) WITH ORDINALITY o (OPTION, ORDINALITY) ON p.attnum = o.ordinality
WHERE C.table_schema = $1
  AND C.table_name = $2
ORDER BY C.ordinal_position`

// endregion
