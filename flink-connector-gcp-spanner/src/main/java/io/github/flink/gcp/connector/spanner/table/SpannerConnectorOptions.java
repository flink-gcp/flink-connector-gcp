/*
 * Copyright 2026 The flink-gcp authors
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

package io.github.flink.gcp.connector.spanner.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

import com.google.cloud.spanner.Dialect;
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;
import io.github.flink.gcp.connector.spanner.source.SpannerChangeStreamSourceBuilder;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * The {@code WITH} options of the {@code spanner} table connector.
 *
 * <p>A mapped option is declared without a default — its default lives on the connector's own
 * builder and is applied by not calling the setter. A test records the exceptions: table-owned
 * selectors the factory reads with {@code get()}, and three change-stream knobs whose {@code
 * defaultValue()} references the builder's own constant. No description restates a default — a
 * builder's, an option's own {@code defaultValue()}, or the value absence selects: the reference
 * and table docs pages carry a default with its derivation, and a test rejects the restatement
 * phrases. A failure absence selects ("unset fails the source") is a contract, not a default, and
 * stays.
 */
@PublicEvolving
public final class SpannerConnectorOptions {

    /** The Google Cloud project containing the Spanner instance. */
    public static final ConfigOption<String> PROJECT =
            ConfigOptions.key("project")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Google Cloud project containing the Spanner instance.");

    /** The Spanner instance containing the database. */
    public static final ConfigOption<String> INSTANCE =
            ConfigOptions.key("instance")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Spanner instance containing the database.");

    /** The Spanner database containing the table. */
    public static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Spanner database containing the table.");

    /** The Spanner table receiving or supplying rows. */
    public static final ConfigOption<String> TABLE =
            ConfigOptions.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Spanner table receiving or supplying rows.");

    /** The named schema containing the table. */
    public static final ConfigOption<String> SCHEMA =
            ConfigOptions.key("schema")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The named schema containing the table.");

    /** The database dialect: GOOGLE_STANDARD_SQL or POSTGRESQL. */
    public static final ConfigOption<Dialect> DIALECT =
            ConfigOptions.key("dialect")
                    .enumType(Dialect.class)
                    .defaultValue(Dialect.GOOGLE_STANDARD_SQL)
                    .withDescription("The database dialect: GOOGLE_STANDARD_SQL or POSTGRESQL.");

    /** The host:port of a Spanner emulator. */
    public static final ConfigOption<String> EMULATOR_ENDPOINT =
            ConfigOptions.key("emulator-endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The host:port of a Spanner emulator.");

    /** The service-account JSON key-file path available to each runtime process. */
    public static final ConfigOption<String> SERVICE_ACCOUNT_KEY_FILE =
            ConfigOptions.key("service-account-key-file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The service-account JSON key-file path available to each runtime process.");

    /** Field paths whose STRING values use Spanner JSON. */
    public static final ConfigOption<List<String>> SCHEMA_JSON_FIELD_PATHS =
            ConfigOptions.key("schema.json-field-paths")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription("Field paths whose STRING values use Spanner JSON.");

    /** Field paths whose STRING values use native Spanner UUID. */
    public static final ConfigOption<List<String>> SCHEMA_UUID_FIELD_PATHS =
            ConfigOptions.key("schema.uuid-field-paths")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription("Field paths whose STRING values use native Spanner UUID.");

    /** Field paths to fully qualified Spanner PROTO type names. */
    public static final ConfigOption<Map<String, String>> SCHEMA_PROTO_TYPE_NAMES =
            ConfigOptions.key("schema.proto-type-names")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("Field paths to fully qualified Spanner PROTO type names.");

    /** Field paths to fully qualified Spanner ENUM type names. */
    public static final ConfigOption<Map<String, String>> SCHEMA_ENUM_TYPE_NAMES =
            ConfigOptions.key("schema.enum-type-names")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("Field paths to fully qualified Spanner ENUM type names.");

    /** The secondary index used by bounded table scans. */
    public static final ConfigOption<String> SCAN_INDEX =
            ConfigOptions.key("scan.index")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The secondary index used by bounded table scans.");

    /**
     * The source mode: bounded reads the current table, while change-stream reads its CDC records.
     */
    public static final ConfigOption<ScanMode> SCAN_MODE =
            ConfigOptions.key("scan.mode")
                    .enumType(ScanMode.class)
                    .defaultValue(ScanMode.BOUNDED)
                    .withDescription(
                            "The source mode: bounded reads the current table, while change-stream reads its CDC records.");

    /** The Spanner change stream read in change-stream mode. */
    public static final ConfigOption<String> SCAN_CHANGE_STREAM_NAME =
            ConfigOptions.key("scan.change-stream.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Spanner change stream read in change-stream mode.");

    /**
     * The emitted changelog: full includes UPDATE_BEFORE rows, while upsert emits insert,
     * update-after, and key-only delete rows.
     */
    public static final ConfigOption<ChangeStreamChangelogMode> SCAN_CHANGE_STREAM_CHANGELOG_MODE =
            ConfigOptions.key("scan.change-stream.changelog-mode")
                    .enumType(ChangeStreamChangelogMode.class)
                    .noDefaultValue()
                    .withDescription(
                            "The emitted changelog: full includes UPDATE_BEFORE rows, while upsert emits insert, update-after, and key-only delete rows.");

    /** Where a new change-stream source starts: earliest, latest, or timestamp. */
    public static final ConfigOption<ChangeStreamStartMode> SCAN_STARTUP_MODE =
            ConfigOptions.key("scan.startup.mode")
                    .enumType(ChangeStreamStartMode.class)
                    .defaultValue(ChangeStreamStartMode.LATEST)
                    .withDescription(
                            "Where a new change-stream source starts: earliest, latest, or timestamp.");

    /** The Unix epoch timestamp in milliseconds used with scan.startup.mode=timestamp. */
    public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP_MILLIS =
            ConfigOptions.key("scan.startup.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "The Unix epoch timestamp in milliseconds used with scan.startup.mode=timestamp.");

    /** Where to resume when restored state has expired; unset fails the source instead. */
    public static final ConfigOption<ChangeStreamStartMode> SCAN_RESUME_FALLBACK_MODE =
            ConfigOptions.key("scan.resume-fallback.mode")
                    .enumType(ChangeStreamStartMode.class)
                    .noDefaultValue()
                    .withDescription(
                            "Where to resume when restored state has expired; unset fails the source instead.");

    /** The Unix epoch timestamp in milliseconds used with scan.resume-fallback.mode=timestamp. */
    public static final ConfigOption<Long> SCAN_RESUME_FALLBACK_TIMESTAMP_MILLIS =
            ConfigOptions.key("scan.resume-fallback.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "The Unix epoch timestamp in milliseconds used with scan.resume-fallback.mode=timestamp.");

    /** The retention assumed when the change stream has no explicit retention option. */
    public static final ConfigOption<Duration> SCAN_CHANGE_STREAM_ABSENT_RETENTION_FALLBACK =
            ConfigOptions.key("scan.change-stream.absent-retention-fallback")
                    .durationType()
                    .defaultValue(
                            SpannerChangeStreamSourceBuilder.DEFAULT_ABSENT_RETENTION_FALLBACK)
                    .withDescription(
                            "The retention assumed when the change stream has no explicit retention option.");

    /** The service heartbeat interval for change-stream queries. */
    public static final ConfigOption<Duration> SCAN_CHANGE_STREAM_HEARTBEAT_INTERVAL =
            ConfigOptions.key("scan.change-stream.heartbeat-interval")
                    .durationType()
                    .defaultValue(SpannerChangeStreamSourceBuilder.DEFAULT_HEARTBEAT_INTERVAL)
                    .withDescription("The service heartbeat interval for change-stream queries.");

    /** The maximum concurrent change-stream partition queries opened by one source subtask. */
    public static final ConfigOption<Integer> SCAN_MAX_CONCURRENT_QUERIES_PER_SUBTASK =
            ConfigOptions.key("scan.max-concurrent-queries-per-subtask")
                    .intType()
                    .defaultValue(
                            SpannerChangeStreamSourceBuilder
                                    .DEFAULT_MAX_CONCURRENT_QUERIES_PER_SUBTASK)
                    .withDescription(
                            "The maximum concurrent change-stream partition queries opened by one source subtask.");

    /** The desired maximum number of read partitions. */
    public static final ConfigOption<Long> SCAN_PARTITION_MAX_PARTITIONS =
            ConfigOptions.key("scan.partition.max-partitions")
                    .longType()
                    .noDefaultValue()
                    .withDescription("The desired maximum number of read partitions.");

    /** The desired size of one read partition. */
    public static final ConfigOption<MemorySize> SCAN_PARTITION_SIZE_BYTES =
            ConfigOptions.key("scan.partition.size-bytes")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription("The desired size of one read partition.");

    /** Whether reads use Spanner Data Boost compute. */
    public static final ConfigOption<Boolean> SCAN_DATA_BOOST_ENABLED =
            ConfigOptions.key("scan.data-boost-enabled")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("Whether reads use Spanner Data Boost compute.");

    /** The LOW, MEDIUM, or HIGH priority of scan RPCs. */
    public static final ConfigOption<SpannerRpcPriority> SCAN_RPC_PRIORITY =
            ConfigOptions.key("scan.rpc-priority")
                    .enumType(SpannerRpcPriority.class)
                    .noDefaultValue()
                    .withDescription("The LOW, MEDIUM, or HIGH priority of scan RPCs.");

    /** An RFC 3339 timestamp at which to read the table. */
    public static final ConfigOption<String> SCAN_TIMESTAMP_BOUND_READ_TIMESTAMP =
            ConfigOptions.key("scan.timestamp-bound.read-timestamp")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("An RFC 3339 timestamp at which to read the table.");

    /** How stale the read snapshot must be. */
    public static final ConfigOption<Duration> SCAN_TIMESTAMP_BOUND_EXACT_STALENESS =
            ConfigOptions.key("scan.timestamp-bound.exact-staleness")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("How stale the read snapshot must be.");

    /** Whether point lookups use Spanner's asynchronous read API. */
    public static final ConfigOption<Boolean> LOOKUP_ASYNC =
            ConfigOptions.key("lookup.async")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether point lookups use Spanner's asynchronous read API.");

    /** The maximum mutation cells in one BatchWrite request. */
    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_CELLS =
            ConfigOptions.key("sink.buffer-flush.max-cells")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The maximum mutation cells in one BatchWrite request.");

    /** The maximum mutations in one BatchWrite request. */
    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_MUTATIONS =
            ConfigOptions.key("sink.buffer-flush.max-mutations")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The maximum mutations in one BatchWrite request.");

    /** The maximum estimated mutation bytes in one request. */
    public static final ConfigOption<MemorySize> SINK_BUFFER_FLUSH_MAX_SIZE =
            ConfigOptions.key("sink.buffer-flush.max-size")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription("The maximum estimated mutation bytes in one request.");

    /** The commit delay Spanner may use to group writes. */
    public static final ConfigOption<Duration> SINK_BUFFER_FLUSH_MAX_COMMIT_DELAY =
            ConfigOptions.key("sink.buffer-flush.max-commit-delay")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The commit delay Spanner may use to group writes.");

    /** The LOW, MEDIUM, or HIGH priority of sink RPCs. */
    public static final ConfigOption<SpannerRpcPriority> SINK_RPC_PRIORITY =
            ConfigOptions.key("sink.rpc-priority")
                    .enumType(SpannerRpcPriority.class)
                    .noDefaultValue()
                    .withDescription("The LOW, MEDIUM, or HIGH priority of sink RPCs.");

    /** The timeout for one complete BatchWrite RPC attempt. */
    public static final ConfigOption<Duration> SINK_BATCH_WRITE_TIMEOUT =
            ConfigOptions.key("sink.batch-write.timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The timeout for one complete BatchWrite RPC attempt.");

    /** The first delay in the sink's transient-failure retry loop. */
    public static final ConfigOption<Duration> SINK_RECOVERY_INITIAL_BACKOFF =
            ConfigOptions.key("sink.recovery.initial-backoff")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The first delay in the sink's transient-failure retry loop.");

    /** The maximum delay in the sink's retry loop. */
    public static final ConfigOption<Duration> SINK_RECOVERY_MAX_BACKOFF =
            ConfigOptions.key("sink.recovery.max-backoff")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The maximum delay in the sink's retry loop.");

    /** The maximum BatchWrite attempts before the job fails. */
    public static final ConfigOption<Integer> SINK_RECOVERY_MAX_ATTEMPTS =
            ConfigOptions.key("sink.recovery.max-attempts")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The maximum BatchWrite attempts before the job fails.");

    private SpannerConnectorOptions() {}
}
