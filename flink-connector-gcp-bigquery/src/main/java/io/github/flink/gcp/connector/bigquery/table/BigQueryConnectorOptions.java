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

import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.WriteDisposition;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;

import java.time.Duration;
import java.util.List;

/**
 * The {@code WITH} options of the {@code bigquery} table connector.
 *
 * <p>Four rules this class follows, and the reasons they are rules rather than habits:
 *
 * <ol>
 *   <li><b>One option per builder setter.</b> The DataStream API is the source of truth; this layer
 *       only maps onto it, and a reflective test asserts the two sets match. Nothing is configured
 *       here that {@code BigQuerySink.builder()} cannot configure.
 *   <li><b>Every option is declared without a default</b>, and the factory applies it with {@code
 *       getOptional(...).ifPresent(...)}. "Absent from the DDL" and "left at the connector's
 *       default" are then the same state, with no third one to invent — and no default value is
 *       restated here or in a description, where nothing would keep the copy in step.
 *   <li><b>Byte-valued options are {@code MemorySize}</b>, converted to a {@code long} in the
 *       mapper that applies them, so the type never reaches the connector's public API.
 *   <li><b>There is no {@code format} option.</b> A Pub/Sub message has an opaque payload, so a
 *       format decides its bytes; a BigQuery row is structured and the DDL schema <em>is</em> the
 *       schema, so the connector supplies its own {@code RowData} serializer.
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
                                    + " bare project id, not a resource path.");

    public static final ConfigOption<String> DATASET =
            ConfigOptions.key("dataset")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The BigQuery dataset holding the destination table, as a bare dataset"
                                    + " id.");

    public static final ConfigOption<String> TABLE =
            ConfigOptions.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The destination table, as a bare table id. One table per SQL table:"
                                    + " per-record routing has no SQL surface and stays on the"
                                    + " DataStream API.");

    public static final ConfigOption<String> EMULATOR_ENDPOINT =
            ConfigOptions.key("emulator-endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A BigQuery emulator's gRPC endpoint as 'host:port', for the Storage"
                                    + " Write API traffic. Connects over plaintext without"
                                    + " credentials, so it is for testing only.");

    public static final ConfigOption<String> EMULATOR_REST_ENDPOINT =
            ConfigOptions.key("emulator-rest-endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A BigQuery emulator's REST endpoint as 'host:port', for table"
                                    + " creation and schema updates. Separate from"
                                    + " 'emulator-endpoint' because BigQuery serves the two"
                                    + " transports on different ports.");

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
                                    + " when a write connection is opened.");

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
                            "The dataset holding the temporary tables a load too large for one job"
                                    + " goes through. Absent, each destination table's own dataset"
                                    + " is used; a dedicated dataset with a default table"
                                    + " expiration collects the ones a hard failure orphans.");

    public static final ConfigOption<WriteDisposition> SINK_FILE_LOADS_WRITE_DISPOSITION =
            ConfigOptions.key("sink.file-loads.write-disposition")
                    .enumType(WriteDisposition.class)
                    .noDefaultValue()
                    .withDescription(
                            "How loaded rows land in a table that already holds data. Streaming"
                                    + " execution accepts 'write-append' only, since every"
                                    + " checkpoint issues its own load job.");

    public static final ConfigOption<Duration> SINK_FILE_LOADS_MIN_CHECKPOINT_INTERVAL =
            ConfigOptions.key("sink.file-loads.min-checkpoint-interval")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The smallest checkpoint interval streaming execution accepts, checked"
                                    + " when the job graph is built. BigQuery allows 1,500 load"
                                    + " jobs per table per day and each checkpoint issues at least"
                                    + " one, so lowering this is an explicit opt-in for a job whose"
                                    + " daily count stays safe.");

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

    public static final ConfigOption<Duration> SINK_FILE_LOADS_LOAD_JOB_POLL_INITIAL_BACKOFF =
            ConfigOptions.key("sink.file-loads.load-job-poll.initial-backoff")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The first backoff between polls of a submitted load or copy job."
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
