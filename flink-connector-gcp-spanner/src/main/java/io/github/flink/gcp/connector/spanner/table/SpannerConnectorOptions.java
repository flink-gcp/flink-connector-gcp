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

package io.github.flink.gcp.connector.spanner.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

import com.google.cloud.spanner.Dialect;
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/** The {@code WITH} options of the {@code spanner} table connector. */
@PublicEvolving
public final class SpannerConnectorOptions {

    public static final ConfigOption<String> PROJECT =
            ConfigOptions.key("project")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Google Cloud project containing the Spanner instance.");
    public static final ConfigOption<String> INSTANCE =
            ConfigOptions.key("instance")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Spanner instance containing the database.");
    public static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Spanner database containing the table.");
    public static final ConfigOption<String> TABLE =
            ConfigOptions.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Spanner table receiving the rows.");
    public static final ConfigOption<Dialect> DIALECT =
            ConfigOptions.key("dialect")
                    .enumType(Dialect.class)
                    .defaultValue(Dialect.GOOGLE_STANDARD_SQL)
                    .withDescription("The database dialect: GOOGLE_STANDARD_SQL or POSTGRESQL.");
    public static final ConfigOption<String> EMULATOR_ENDPOINT =
            ConfigOptions.key("emulator-endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The host:port of a Spanner emulator; unset uses the service.");

    public static final ConfigOption<List<String>> SCHEMA_JSON_FIELD_PATHS =
            ConfigOptions.key("schema.json-field-paths")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription("Field paths whose STRING values use Spanner JSON.");
    public static final ConfigOption<List<String>> SCHEMA_UUID_FIELD_PATHS =
            ConfigOptions.key("schema.uuid-field-paths")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription("Field paths whose STRING values use native Spanner UUID.");
    public static final ConfigOption<Map<String, String>> SCHEMA_PROTO_TYPE_NAMES =
            ConfigOptions.key("schema.proto-type-names")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("Field paths to fully qualified Spanner PROTO type names.");
    public static final ConfigOption<Map<String, String>> SCHEMA_ENUM_TYPE_NAMES =
            ConfigOptions.key("schema.enum-type-names")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("Field paths to fully qualified Spanner ENUM type names.");

    public static final ConfigOption<Long> SCAN_PARTITION_MAX_PARTITIONS =
            ConfigOptions.key("scan.partition.max-partitions")
                    .longType()
                    .noDefaultValue()
                    .withDescription("The desired maximum number of read partitions.");
    public static final ConfigOption<MemorySize> SCAN_PARTITION_SIZE =
            ConfigOptions.key("scan.partition.size")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription("The desired size of one read partition.");
    public static final ConfigOption<Boolean> SCAN_DATA_BOOST_ENABLED =
            ConfigOptions.key("scan.data-boost-enabled")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("Whether reads use Spanner Data Boost compute.");
    public static final ConfigOption<SpannerRpcPriority> SCAN_RPC_PRIORITY =
            ConfigOptions.key("scan.rpc-priority")
                    .enumType(SpannerRpcPriority.class)
                    .noDefaultValue()
                    .withDescription("The LOW, MEDIUM, or HIGH priority of scan RPCs.");
    public static final ConfigOption<String> SCAN_TIMESTAMP_BOUND_READ_TIMESTAMP =
            ConfigOptions.key("scan.timestamp-bound.read-timestamp")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("An RFC 3339 timestamp at which to read the table.");
    public static final ConfigOption<Duration> SCAN_TIMESTAMP_BOUND_EXACT_STALENESS =
            ConfigOptions.key("scan.timestamp-bound.exact-staleness")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("How stale the read snapshot must be; unset means strong.");

    public static final ConfigOption<Boolean> LOOKUP_ASYNC =
            ConfigOptions.key("lookup.async")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether point lookups use Spanner's asynchronous read API.");

    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_CELLS =
            ConfigOptions.key("sink.buffer-flush.max-cells")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The maximum mutation cells in one BatchWrite request.");
    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_MUTATIONS =
            ConfigOptions.key("sink.buffer-flush.max-mutations")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The maximum mutations in one BatchWrite request.");
    public static final ConfigOption<MemorySize> SINK_BUFFER_FLUSH_MAX_SIZE =
            ConfigOptions.key("sink.buffer-flush.max-size")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription("The maximum estimated mutation bytes in one request.");
    public static final ConfigOption<Duration> SINK_BUFFER_FLUSH_MAX_COMMIT_DELAY =
            ConfigOptions.key("sink.buffer-flush.max-commit-delay")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The commit delay Spanner may use to group writes.");
    public static final ConfigOption<SpannerRpcPriority> SINK_RPC_PRIORITY =
            ConfigOptions.key("sink.rpc-priority")
                    .enumType(SpannerRpcPriority.class)
                    .noDefaultValue()
                    .withDescription("The LOW, MEDIUM, or HIGH priority of sink RPCs.");
    public static final ConfigOption<Duration> SINK_RETRY_INITIAL_BACKOFF =
            ConfigOptions.key("sink.retry.initial-backoff")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The first delay in the sink's transient-failure retry loop.");
    public static final ConfigOption<Duration> SINK_RETRY_MAX_BACKOFF =
            ConfigOptions.key("sink.retry.max-backoff")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The maximum delay in the sink's retry loop.");
    public static final ConfigOption<Integer> SINK_RETRY_MAX_ATTEMPTS =
            ConfigOptions.key("sink.retry.max-attempts")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The maximum BatchWrite attempts before the job fails.");

    private SpannerConnectorOptions() {}
}
