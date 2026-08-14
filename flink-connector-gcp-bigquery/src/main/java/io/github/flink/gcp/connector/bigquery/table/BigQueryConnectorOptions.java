/*
 * Copyright 2026 laughingman7743
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.flink.gcp.connector.bigquery.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

import io.github.flink.gcp.connector.bigquery.sink.CdcTableReconciliationPolicy;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.WriteDisposition;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.ParquetCompression;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.StagingFormat;

import java.time.Duration;
import java.util.List;

/**
 * The {@code WITH} options of the {@code bigquery} table connector.
 *
 * <p>Four rules this class follows, and the reasons they are rules rather than habits:
 *
 * <ol>
 *   <li><b>The DataStream APIs are the source of truth.</b> Runtime options map onto those
 *       builders; destination parts are assembled together, and {@code source.parent-project}
 *       overrides the {@code project} fallback passed to {@code
 *       BigQuerySourceBuilder.parentProject(...)}.
 *   <li><b>Every option is declared without a Flink default.</b> Direction-specific requirements
 *       and the parent-project fallback remain explicit in the factory rather than being hidden in
 *       the configuration object.
 *   <li><b>Byte-valued options are {@code MemorySize}</b>, converted to a {@code long} in the
 *       mapper that applies them, so the type never reaches the connector's public API.
 *   <li><b>There is no {@code format} option.</b> A Pub/Sub message has an opaque payload, so a
 *       format decides its bytes; a BigQuery row is structured and the DDL schema <em>is</em> the
 *       schema, so the connector supplies its own {@code RowData} converter and serializer.
 * </ol>
 *
 * <p>The enum-valued options accept the spellings their enums' {@code toString()} return, which are
 * hyphenated lower case: Flink matches an enum option case-insensitively on {@code toString()} and
 * normalizes nothing else.
 */
@PublicEvolving
public final class BigQueryConnectorOptions {

    // ------------------------------------------------------------------------
    //  Destination
    // ------------------------------------------------------------------------

    public static final ConfigOption<String> PROJECT =
            ConfigOptions.key("project")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Google Cloud project owning the destination table. Given as a"
                                    + " bare project id, not a resource path. For a source query,"
                                    + " this is the project that runs and pays for the query job"
                                    + " unless source.parent-project is set instead.");

    public static final ConfigOption<String> DATASET =
            ConfigOptions.key("dataset")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The BigQuery dataset holding the direct source or destination table,"
                                    + " as a bare dataset id. Not required for a source query.");

    public static final ConfigOption<String> TABLE =
            ConfigOptions.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The direct source or destination table, as a bare table id. Not"
                                    + " required for a source query. One destination per SQL"
                                    + " table: per-record routing has no SQL surface and stays on"
                                    + " the DataStream API.");

    public static final ConfigOption<String> EMULATOR_ENDPOINT =
            ConfigOptions.key("emulator-endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A BigQuery emulator's gRPC endpoint as 'host:port', for Storage Read"
                                    + " or Write API traffic. Connects over plaintext without"
                                    + " credentials, so it is for testing only.");

    public static final ConfigOption<String> EMULATOR_REST_ENDPOINT =
            ConfigOptions.key("emulator-rest-endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A BigQuery emulator's REST endpoint as 'host:port', for source query"
                                    + " or view materialization and sink table metadata. Separate"
                                    + " from 'emulator-endpoint' because BigQuery serves the two"
                                    + " transports on different ports.");

    public static final ConfigOption<String> SERVICE_ACCOUNT_KEY_FILE =
            ConfigOptions.key("service-account-key-file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A path at which the same service-account JSON key is available to"
                                    + " every Job Manager and Task Manager that opens a BigQuery"
                                    + " or Cloud Storage client. When absent, clients use"
                                    + " application-default credentials.");

    // ------------------------------------------------------------------------
    //  Source
    // ------------------------------------------------------------------------

    public static final ConfigOption<String> SOURCE_PARENT_PROJECT =
            ConfigOptions.key("source.parent-project")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The project that owns and is billed for the Storage Read session."
                                    + " Defaults to project; set it independently when reading a"
                                    + " table owned by another project.");

    public static final ConfigOption<String> SOURCE_QUERY =
            ConfigOptions.key("source.query")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A GoogleSQL query whose result is read instead of the configured"
                                    + " table. Dataset and table are not required; project is the"
                                    + " billing project unless source.parent-project overrides"
                                    + " it.");

    public static final ConfigOption<Boolean> SOURCE_MATERIALIZE_VIEWS =
            ConfigOptions.key("source.materialize-views")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Whether a configured table that is a logical or materialized view is"
                                    + " materialized through a query before it is read.");

    public static final ConfigOption<String> SOURCE_QUERY_LOCATION =
            ConfigOptions.key("source.query-location")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The location in which a source query job runs.");

    public static final ConfigOption<String> SOURCE_QUERY_RESULT_DATASET =
            ConfigOptions.key("source.query-result-dataset")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The dataset receiving a source query's temporary result table."
                                    + " Absent uses BigQuery's anonymous dataset.");

    public static final ConfigOption<Duration> SOURCE_REUSE_QUERY_RESULT_WITHIN =
            ConfigOptions.key("source.reuse-query-result-within")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "How long a re-planned source may reattach to the same query job."
                                    + " Requires source.query-location.");

    public static final ConfigOption<String> SOURCE_ROW_RESTRICTION =
            ConfigOptions.key("source.row-restriction")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A BigQuery Storage Read row restriction, written as a WHERE clause"
                                    + " without the WHERE keyword.");

    public static final ConfigOption<String> SOURCE_SNAPSHOT_TIME =
            ConfigOptions.key("source.snapshot-time")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "An ISO-8601 instant at which the configured table is read through"
                                    + " BigQuery time travel.");

    public static final ConfigOption<Integer> SOURCE_MAX_STREAM_COUNT =
            ConfigOptions.key("source.max-stream-count")
                    .intType()
                    .noDefaultValue()
                    .withDescription("An upper bound on Storage Read streams.");

    public static final ConfigOption<Integer> SOURCE_PREFERRED_MIN_STREAM_COUNT =
            ConfigOptions.key("source.preferred-min-stream-count")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The preferred minimum number of Storage Read streams.");

    public static final ConfigOption<Integer> SOURCE_MAX_RECORDS_PER_FETCH =
            ConfigOptions.key("source.max-records-per-fetch")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The most decoded rows one source fetch hands to the task thread.");

    public static final ConfigOption<Integer> SOURCE_RETRY_MAX_ATTEMPTS =
            ConfigOptions.key("source.retry-max-attempts")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The maximum consecutive Storage Read attempts without progress.");

    // ------------------------------------------------------------------------
    //  Sink — shared
    // ------------------------------------------------------------------------

    public static final ConfigOption<WriteMethod> SINK_WRITE_METHOD =
            ConfigOptions.key("sink.write-method")
                    .enumType(WriteMethod.class)
                    .noDefaultValue()
                    .withDescription(
                            "Which write path the sink uses. Each carries its own tuning family —"
                                    + " 'sink.default-stream.*', 'sink.buffered-stream.*' and"
                                    + " 'sink.file-loads.*' — and a key of a family this option"
                                    + " does not select is rejected rather than ignored.");

    public static final ConfigOption<CreateDisposition> SINK_CREATE_DISPOSITION =
            ConfigOptions.key("sink.create-disposition")
                    .enumType(CreateDisposition.class)
                    .noDefaultValue()
                    .withDescription(
                            "Whether a missing destination table is created from the DDL schema or"
                                    + " fails the job.");

    public static final ConfigOption<String> SINK_LOCATION =
            ConfigOptions.key("sink.location")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The BigQuery location of the destination table, for example 'US' or"
                                    + " 'asia-northeast1'. Setting it avoids a metadata lookup"
                                    + " when a write connection is opened and locates CDC"
                                    + " maximum-staleness jobs; under FILE_LOADS it is the"
                                    + " location load jobs run in, derived from the"
                                    + " destination dataset when unset.");

    public static final ConfigOption<Boolean> SINK_CDC_ENABLED =
            ConfigOptions.key("sink.cdc.enabled")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Whether the default-stream sink writes the table changelog as"
                                    + " BigQuery CDC UPSERT and DELETE mutations. Requires a"
                                    + " declared primary key and storage-api-at-least-once. With"
                                    + " create-if-needed, the sink creates and verifies the CDC"
                                    + " table before writing.");

    public static final ConfigOption<Duration> SINK_CDC_MAX_STALENESS =
            ConfigOptions.key("sink.cdc.max-staleness")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The maximum staleness the sink manages for a CDC table through"
                                    + " verified DDL. Absent, the property is unmanaged.");

    public static final ConfigOption<Boolean> SINK_CDC_CLEAR_MAX_STALENESS =
            ConfigOptions.key("sink.cdc.clear-max-staleness")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Whether the desired CDC table state has maximum staleness disabled."
                                    + " Mutually exclusive with 'sink.cdc.max-staleness'.");

    public static final ConfigOption<CdcTableReconciliationPolicy> SINK_CDC_TABLE_RECONCILIATION =
            ConfigOptions.key("sink.cdc.table-reconciliation")
                    .enumType(CdcTableReconciliationPolicy.class)
                    .noDefaultValue()
                    .withDescription(
                            "Whether an existing CDC table is only verified or has mutable"
                                    + " CDC properties reconciled. Defaults to"
                                    + " verify-only.");

    public static final ConfigOption<Boolean> SINK_SCHEMA_UPDATE_ALLOW_NEW_FIELDS =
            ConfigOptions.key("sink.schema-update.allow-new-fields")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Whether the sink may add columns to the destination table when the"
                                    + " DDL schema carries a column the table does not.");

    public static final ConfigOption<Boolean> SINK_SCHEMA_UPDATE_ALLOW_FIELD_RELAXATION =
            ConfigOptions.key("sink.schema-update.allow-field-relaxation")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Whether the sink may relax a REQUIRED column of the destination table"
                                    + " to NULLABLE. BigQuery cannot walk the reverse change back.");

    // ------------------------------------------------------------------------
    //  Sink — table creation
    // ------------------------------------------------------------------------

    public static final ConfigOption<TableCreateOptions.TimePartitioningType>
            SINK_TABLE_CREATE_TIME_PARTITIONING_TYPE =
                    ConfigOptions.key("sink.table-create.time-partitioning.type")
                            .enumType(TableCreateOptions.TimePartitioningType.class)
                            .noDefaultValue()
                            .withDescription(
                                    "The granularity a created table is time-partitioned at. Absent,"
                                            + " the table is not partitioned.");

    public static final ConfigOption<String> SINK_TABLE_CREATE_TIME_PARTITIONING_FIELD =
            ConfigOptions.key("sink.table-create.time-partitioning.field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The TIMESTAMP, DATE or DATETIME column a created table is partitioned"
                                    + " on. Absent, the table is partitioned on ingestion time,"
                                    + " which no column can name. Requires"
                                    + " 'sink.table-create.time-partitioning.type'.");

    public static final ConfigOption<Duration> SINK_TABLE_CREATE_TIME_PARTITIONING_EXPIRATION =
            ConfigOptions.key("sink.table-create.time-partitioning.expiration")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "How long BigQuery keeps a partition of a created table. Absent,"
                                    + " partitions never expire. Requires"
                                    + " 'sink.table-create.time-partitioning.type'.");

    public static final ConfigOption<List<String>> SINK_TABLE_CREATE_CLUSTERED_FIELDS =
            ConfigOptions.key("sink.table-create.clustered-fields")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "The columns a created table is clustered on, in precedence order."
                                    + " BigQuery takes at most four, and they must be top-level"
                                    + " columns of the table.");

    // ------------------------------------------------------------------------
    //  Sink — schema derivation from the DDL row type
    // ------------------------------------------------------------------------

    public static final ConfigOption<Boolean> SINK_DERIVE_REQUIRED_COLUMNS =
            ConfigOptions.key("sink.derive-required-columns")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Whether a column declared NOT NULL derives a REQUIRED BigQuery"
                                    + " column. Off, every derived column is NULLABLE — REQUIRED is"
                                    + " the mode BigQuery cannot walk back, so it is opted into"
                                    + " rather than inferred.");

    public static final ConfigOption<List<String>> SINK_JSON_FIELD_PATHS =
            ConfigOptions.key("sink.json-field-paths")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "Dotted paths of STRING or ROW columns to derive as BigQuery JSON"
                                    + " columns, for example 'payload' or 'event.body'. A map"
                                    + " value's path segment is 'value'.");

    public static final ConfigOption<List<String>> SINK_GEOGRAPHY_FIELD_PATHS =
            ConfigOptions.key("sink.geography-field-paths")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "Dotted paths of STRING columns to derive as BigQuery GEOGRAPHY"
                                    + " columns. The values are passed through unvalidated, as on"
                                    + " every other write path.");

    // ------------------------------------------------------------------------
    //  Sink — STORAGE_API_AT_LEAST_ONCE tuning
    // ------------------------------------------------------------------------

    public static final ConfigOption<MemorySize> SINK_DEFAULT_STREAM_MAX_APPEND_REQUEST_BYTES =
            ConfigOptions.key("sink.default-stream.max-append-request-bytes")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "The size at which the sink splits a batch into several append"
                                    + " requests, below the Storage Write API's own request limit.");

    public static final ConfigOption<Duration> SINK_DEFAULT_STREAM_RECOVERY_INITIAL_BACKOFF =
            ConfigOptions.key("sink.default-stream.recovery.initial-backoff")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The first backoff of the connector's own repair loop, which reopens a"
                                    + " write connection after a transient failure.");

    public static final ConfigOption<Duration> SINK_DEFAULT_STREAM_RECOVERY_MAX_BACKOFF =
            ConfigOptions.key("sink.default-stream.recovery.max-backoff")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The ceiling that repair loop's backoff grows to.");

    public static final ConfigOption<Integer> SINK_DEFAULT_STREAM_RECOVERY_MAX_ATTEMPTS =
            ConfigOptions.key("sink.default-stream.recovery.max-attempts")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "How many times that repair loop reopens a connection before the job"
                                    + " fails.");

    public static final ConfigOption<Duration> SINK_DEFAULT_STREAM_RETRY_INITIAL_DELAY =
            ConfigOptions.key("sink.default-stream.retry.initial-delay")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The first delay of the client library's own in-stream append retries,"
                                    + " a layer below the repair loop above.");

    public static final ConfigOption<Double> SINK_DEFAULT_STREAM_RETRY_DELAY_MULTIPLIER =
            ConfigOptions.key("sink.default-stream.retry.delay-multiplier")
                    .doubleType()
                    .noDefaultValue()
                    .withDescription("The factor those in-stream retry delays grow by.");

    public static final ConfigOption<Duration> SINK_DEFAULT_STREAM_RETRY_MAX_DELAY =
            ConfigOptions.key("sink.default-stream.retry.max-delay")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The ceiling those in-stream retry delays grow to.");

    public static final ConfigOption<Integer> SINK_DEFAULT_STREAM_RETRY_MAX_ATTEMPTS =
            ConfigOptions.key("sink.default-stream.retry.max-attempts")
                    .intType()
                    .noDefaultValue()
                    .withDescription("How many times the client library retries one append.");

    public static final ConfigOption<Duration> SINK_DEFAULT_STREAM_RETRY_MAX_DURATION =
            ConfigOptions.key("sink.default-stream.retry.max-duration")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The wall-clock budget the client library spends retrying one append,"
                                    + " whichever attempt it is on.");

    public static final ConfigOption<Integer> SINK_DEFAULT_STREAM_MAX_INFLIGHT_REQUESTS =
            ConfigOptions.key("sink.default-stream.max-inflight-requests")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "How many appends may be in flight on one connection before the client"
                                    + " library blocks. JVM-global in effect: the first writer's"
                                    + " value wins for the whole connection pool.");

    public static final ConfigOption<MemorySize> SINK_DEFAULT_STREAM_MAX_INFLIGHT_BYTES =
            ConfigOptions.key("sink.default-stream.max-inflight-bytes")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "The same bound measured in bytes, and JVM-global in the same way.");

    public static final ConfigOption<Integer> SINK_DEFAULT_STREAM_MIN_CONNECTIONS_PER_REGION =
            ConfigOptions.key("sink.default-stream.min-connections-per-region")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The connection pool's floor per region. Applied once per JVM, by"
                                    + " whichever sink opens a connection first.");

    public static final ConfigOption<Integer> SINK_DEFAULT_STREAM_MAX_CONNECTIONS_PER_REGION =
            ConfigOptions.key("sink.default-stream.max-connections-per-region")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The connection pool's ceiling per region.");

    public static final ConfigOption<Duration> SINK_DEFAULT_STREAM_DESTINATION_IDLE_TIMEOUT =
            ConfigOptions.key("sink.default-stream.destination-idle-timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "How long a destination may go unwritten before its write connection"
                                    + " is closed. Set a large duration to disable the sweep.");

    public static final ConfigOption<Duration> SINK_DEFAULT_STREAM_FLUSH_INTERVAL =
            ConfigOptions.key("sink.default-stream.flush-interval")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "A periodic flush, for jobs whose checkpoint interval is long or"
                                    + " absent. A mitigation only: the delivery guarantee still"
                                    + " rests on checkpointing.");

    public static final ConfigOption<Boolean> SINK_DEFAULT_STREAM_PER_DESTINATION_METRICS =
            ConfigOptions.key("sink.default-stream.per-destination-metrics")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Whether the sink reports its write metrics per destination table as"
                                    + " well as in total. One SQL table writes to one destination,"
                                    + " so this mainly adds the table name to the metric group.");

    // ------------------------------------------------------------------------
    //  Sink — STORAGE_API_EXACTLY_ONCE tuning
    // ------------------------------------------------------------------------

    public static final ConfigOption<MemorySize> SINK_BUFFERED_STREAM_MAX_APPEND_REQUEST_BYTES =
            ConfigOptions.key("sink.buffered-stream.max-append-request-bytes")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "The size at which the sink splits a batch into several append"
                                    + " requests, below the Storage Write API's own request limit.");

    public static final ConfigOption<Duration> SINK_BUFFERED_STREAM_DESTINATION_IDLE_TIMEOUT =
            ConfigOptions.key("sink.buffered-stream.destination-idle-timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "How long a checkpoint-clean destination may go unwritten before its"
                                    + " local appender and writer state are dropped. A later row"
                                    + " creates a new buffered stream.");

    public static final ConfigOption<Duration> SINK_BUFFERED_STREAM_RECOVERY_INITIAL_BACKOFF =
            ConfigOptions.key("sink.buffered-stream.recovery.initial-backoff")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The first backoff of the connector's own recovery schedule, which"
                                    + " creates the stream after a table auto-creation, reopens it"
                                    + " after a transient failure and drives the restore probe.");

    public static final ConfigOption<Duration> SINK_BUFFERED_STREAM_RECOVERY_MAX_BACKOFF =
            ConfigOptions.key("sink.buffered-stream.recovery.max-backoff")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The ceiling that recovery schedule's backoff grows to.");

    public static final ConfigOption<Integer> SINK_BUFFERED_STREAM_RECOVERY_MAX_ATTEMPTS =
            ConfigOptions.key("sink.buffered-stream.recovery.max-attempts")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "How many attempts that recovery schedule makes before the job fails.");

    public static final ConfigOption<Duration> SINK_BUFFERED_STREAM_RETRY_INITIAL_DELAY =
            ConfigOptions.key("sink.buffered-stream.retry.initial-delay")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The first delay of the client library's own in-stream append retries,"
                                    + " a layer below the recovery schedule above.");

    public static final ConfigOption<Double> SINK_BUFFERED_STREAM_RETRY_DELAY_MULTIPLIER =
            ConfigOptions.key("sink.buffered-stream.retry.delay-multiplier")
                    .doubleType()
                    .noDefaultValue()
                    .withDescription("The factor those in-stream retry delays grow by.");

    public static final ConfigOption<Duration> SINK_BUFFERED_STREAM_RETRY_MAX_DELAY =
            ConfigOptions.key("sink.buffered-stream.retry.max-delay")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The ceiling those in-stream retry delays grow to.");

    public static final ConfigOption<Integer> SINK_BUFFERED_STREAM_RETRY_MAX_ATTEMPTS =
            ConfigOptions.key("sink.buffered-stream.retry.max-attempts")
                    .intType()
                    .noDefaultValue()
                    .withDescription("How many times the client library retries one append.");

    public static final ConfigOption<Duration> SINK_BUFFERED_STREAM_RETRY_MAX_DURATION =
            ConfigOptions.key("sink.buffered-stream.retry.max-duration")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The wall-clock budget the client library spends retrying one append,"
                                    + " whichever attempt it is on.");

    // ------------------------------------------------------------------------
    //  Sink — FILE_LOADS tuning
    // ------------------------------------------------------------------------

    public static final ConfigOption<String> SINK_FILE_LOADS_STAGING_PATH =
            ConfigOptions.key("sink.file-loads.staging-path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Cloud Storage path staged files are written under, of the form"
                                    + " gs://bucket or gs://bucket/prefix. Required under the"
                                    + " 'file-loads' write method and rejected under the others.");

    public static final ConfigOption<String> SINK_FILE_LOADS_TEMP_DATASET =
            ConfigOptions.key("sink.file-loads.temp-dataset")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The dataset holding temporary tables when a load is too large for one"
                                    + " job or replacement rows span staging formats. Absent, each"
                                    + " destination table's own dataset is used; a dedicated"
                                    + " dataset with a default table expiration collects the ones"
                                    + " a hard failure orphans.");

    public static final ConfigOption<WriteDisposition> SINK_FILE_LOADS_WRITE_DISPOSITION =
            ConfigOptions.key("sink.file-loads.write-disposition")
                    .enumType(WriteDisposition.class)
                    .noDefaultValue()
                    .withDescription(
                            "How loaded rows land in a table that already holds data. Streaming"
                                    + " execution accepts 'write-append' only, since every"
                                    + " checkpoint commits its own staged files. In batch,"
                                    + " 'write-truncate-data' replaces rows while preserving the"
                                    + " destination schema and constraints.");

    public static final ConfigOption<Duration> SINK_FILE_LOADS_MIN_CHECKPOINT_INTERVAL =
            ConfigOptions.key("sink.file-loads.min-checkpoint-interval")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The smallest checkpoint interval streaming execution accepts, checked"
                                    + " when the job graph is built. BigQuery allows 1,500"
                                    + " modifications per standard destination table per day, and"
                                    + " each checkpoint issues a direct load or an overflow copy,"
                                    + " so lowering this is an explicit opt-in for a job whose daily"
                                    + " count stays safe.");

    public static final ConfigOption<MemorySize> SINK_FILE_LOADS_MAX_STAGING_FILE_BYTES =
            ConfigOptions.key("sink.file-loads.max-staging-file-bytes")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "The size at which an open staging file is finished and the next one"
                                    + " opened. Raise it for a very large volume to one destination,"
                                    + " which the 10,000-URI per-load-job cap would otherwise push"
                                    + " onto the temporary-table plus copy path; lowering it buys"
                                    + " little, because measured load time climbs steeply below"
                                    + " about 8 MiB per file.");

    public static final ConfigOption<StagingFormat> SINK_FILE_LOADS_STAGING_FORMAT =
            ConfigOptions.key("sink.file-loads.staging-format")
                    .enumType(StagingFormat.class)
                    .noDefaultValue()
                    .withDescription(
                            "The file format rows are staged in before loading. Avro is the default"
                                    + " and the recommended value; Parquet is opt-in, needs"
                                    + " dependencies this connector does not ship, cannot carry a"
                                    + " JSON column (such a destination falls back to Avro), and"
                                    + " loads several times more slowly below 256 MiB of input per"
                                    + " load job.");

    public static final ConfigOption<ParquetCompression> SINK_FILE_LOADS_PARQUET_COMPRESSION =
            ConfigOptions.key("sink.file-loads.parquet-compression")
                    .enumType(ParquetCompression.class)
                    .noDefaultValue()
                    .withDescription(
                            "How Parquet staging files are compressed. Rejected when the staging"
                                    + " format is Avro, whose codec is not configurable. 'none' is"
                                    + " the only value that needs no Hadoop runtime, and it stages"
                                    + " substantially more bytes than Avro does.");

    public static final ConfigOption<Duration> SINK_FILE_LOADS_LOAD_JOB_POLL_INITIAL_BACKOFF =
            ConfigOptions.key("sink.file-loads.load-job-poll.initial-backoff")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The first backoff between polls of a submitted load, copy, or terminal"
                                    + " query job."
                                    + " Lowering it notices a finished load sooner, at the cost of"
                                    + " more jobs.get calls.");

    public static final ConfigOption<Duration> SINK_FILE_LOADS_LOAD_JOB_POLL_MAX_BACKOFF =
            ConfigOptions.key("sink.file-loads.load-job-poll.max-backoff")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The ceiling those poll backoffs grow to.");

    public static final ConfigOption<Duration> SINK_FILE_LOADS_SCHEMA_RECONCILE_INITIAL_BACKOFF =
            ConfigOptions.key("sink.file-loads.schema-reconcile.initial-backoff")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The first backoff after losing an etag race while reconciling a"
                                    + " destination table's schema.");

    public static final ConfigOption<Duration> SINK_FILE_LOADS_SCHEMA_RECONCILE_MAX_BACKOFF =
            ConfigOptions.key("sink.file-loads.schema-reconcile.max-backoff")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The ceiling that reconcile backoff grows to.");

    public static final ConfigOption<Integer> SINK_FILE_LOADS_SCHEMA_RECONCILE_MAX_ATTEMPTS =
            ConfigOptions.key("sink.file-loads.schema-reconcile.max-attempts")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "How many attempts a schema reconciliation makes. Only a lost race"
                                    + " consumes one, so raise it when something outside this job"
                                    + " updates the same table concurrently.");

    public static final ConfigOption<Boolean> SINK_FILE_LOADS_PER_DESTINATION_METRICS =
            ConfigOptions.key("sink.file-loads.per-destination-metrics")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Whether the sink reports its write metrics per destination table as"
                                    + " well as in total. One SQL table writes to one destination,"
                                    + " so this mainly adds the table name to the metric group.");

    private BigQueryConnectorOptions() {}
}
