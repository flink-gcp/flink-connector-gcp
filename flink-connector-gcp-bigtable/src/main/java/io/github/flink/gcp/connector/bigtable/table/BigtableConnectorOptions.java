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

package io.github.flink.gcp.connector.bigtable.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

import io.github.flink.gcp.connector.bigtable.sink.CreateDisposition;

import java.time.Duration;
import java.util.List;

/**
 * The {@code WITH} options of the {@code bigtable} table connector.
 *
 * <p>Four rules this class follows, the first three shared with the sibling connectors' table
 * layers and the fourth this connector's own:
 *
 * <ol>
 *   <li><b>One option per builder setter.</b> The DataStream API is the source of truth; this layer
 *       only maps onto it, and {@code BigtableOptionParityTest} asserts the two sets match across
 *       every surface a DDL can reach.
 *   <li><b>Every option is declared without a default</b>, and the factory applies it with {@code
 *       getOptional(...).ifPresent(...)}, so "absent from the DDL" and "left at the connector's
 *       default" are the same state. The exception is an option the <em>table layer itself</em>
 *       owns, which has no connector default to be a second copy of: {@link #NULL_STRING_LITERAL},
 *       {@link #SCAN_ROW_KEY_ENCODING}, {@link #LOOKUP_ASYNC}, {@link
 *       #SINK_CELL_TIMESTAMP_TRUNCATE_TO_MILLIS}, {@link #SINK_INSERT_ONLY_INPUT_MODE} and {@link
 *       #SCAN_MODE} are the six here, and the parity test asserts that partition exactly rather
 *       than tolerating a default anywhere.
 *   <li><b>Byte-valued options are {@code MemorySize}</b>, converted to a {@code long} in the
 *       mapper that applies them, so the type never reaches the connector's public API.
 *   <li><b>There is no {@code format} option.</b> A Bigtable row is a schema this DDL describes —
 *       the rowkey column, one {@code ROW<...>} column per column family, a nested field per
 *       qualifier — and the cell bytes use the HBase ecosystem's encodings, so there is nothing for
 *       a format factory to decide.
 * </ol>
 *
 * <p>An enum-valued option accepts the spelling its enum's {@code toString()} returns. Connector
 * enums override it when the DDL needs hyphenated lower case; otherwise they retain the constant's
 * spelling, as imported Flink lookup enums do. Flink matches enum options case-insensitively and
 * normalizes nothing else.
 */
@PublicEvolving
public final class BigtableConnectorOptions {

    // ------------------------------------------------------------------------
    //  Destination
    // ------------------------------------------------------------------------

    public static final ConfigOption<String> PROJECT =
            ConfigOptions.key("project")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Google Cloud project owning the Bigtable instance. Given as a"
                                    + " bare project id, not a resource path.");

    public static final ConfigOption<String> INSTANCE =
            ConfigOptions.key("instance")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Bigtable instance, as a bare instance id.");

    public static final ConfigOption<String> TABLE =
            ConfigOptions.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The table, as a bare table id. One table per SQL table: per-record"
                                    + " routing has no SQL surface and stays on the DataStream"
                                    + " API.");

    public static final ConfigOption<String> EMULATOR_ENDPOINT =
            ConfigOptions.key("emulator-endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A Bigtable emulator's endpoint as 'host:port'. Connects over"
                                    + " plaintext without credentials, so it is for testing"
                                    + " only.");

    public static final ConfigOption<String> SERVICE_ACCOUNT_KEY_FILE =
            ConfigOptions.key("service-account-key-file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A service-account JSON key-file path read by each Bigtable runtime"
                                    + " component that opens a client. Uses application-default"
                                    + " credentials when unset and cannot be combined with"
                                    + " emulator-endpoint.");

    // ------------------------------------------------------------------------
    //  Cell encoding
    // ------------------------------------------------------------------------

    public static final ConfigOption<String> NULL_STRING_LITERAL =
            ConfigOptions.key("null-string-literal")
                    .stringType()
                    .defaultValue("null")
                    .withDescription(
                            "The cell value that stands for a null in a character-string column."
                                    + " Every other type writes a null as an empty cell, which a"
                                    + " string cannot use because an empty string is a legitimate"
                                    + " value. This option and its default are the HBase"
                                    + " connector's, so a table written by either is readable by"
                                    + " the other.");

    // ------------------------------------------------------------------------
    //  Scan
    // ------------------------------------------------------------------------

    public static final ConfigOption<ScanMode> SCAN_MODE =
            ConfigOptions.key("scan.mode")
                    .enumType(ScanMode.class)
                    .defaultValue(ScanMode.BOUNDED)
                    .withDescription(
                            "Whether the source reads the current table through a bounded scan or"
                                    + " reads mutations through Bigtable Change Streams.");

    public static final ConfigOption<String> SCAN_APP_PROFILE_ID =
            ConfigOptions.key("scan.app-profile-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The app profile the scan reads under. Separate from"
                                    + " 'sink.app-profile-id' because a Data Boost profile reads"
                                    + " and cannot write, so one table legitimately scans and"
                                    + " writes under different profiles.");

    public static final ConfigOption<RowKeyEncoding> SCAN_ROW_KEY_ENCODING =
            ConfigOptions.key("scan.row-key-encoding")
                    .enumType(RowKeyEncoding.class)
                    .defaultValue(RowKeyEncoding.UTF8)
                    .withDescription(
                            "How scan row-key prefixes and range bounds are represented. UTF8"
                                    + " preserves the original text behavior. BASE64 accepts only"
                                    + " canonical padded RFC 4648 standard Base64 and decodes it"
                                    + " to the exact row-key bytes.");

    public static final ConfigOption<List<String>> SCAN_ROW_PREFIX =
            ConfigOptions.key("scan.row-prefix")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "Scan only the rows whose key starts with one of these prefixes."
                                    + " Repeatable — the list separator is ';' — and additive with"
                                    + " every configured range: overlapping selections are merged, so"
                                    + " no row is read twice. 'scan.row-key-encoding' controls"
                                    + " whether each element is UTF-8 text or Base64.");

    public static final ConfigOption<String> SCAN_ROW_RANGE_START_CLOSED =
            ConfigOptions.key("scan.row-range.start-closed")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Scan from this row key, inclusive. 'scan.row-key-encoding' controls"
                                    + " whether it is UTF-8 text or Base64. May be given without"
                                    + " 'scan.row-range.end-open', which leaves the range open"
                                    + " above. Additional ranges use 'scan.row-ranges'.");

    public static final ConfigOption<String> SCAN_ROW_RANGE_END_OPEN =
            ConfigOptions.key("scan.row-range.end-open")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Scan up to this row key, exclusive. 'scan.row-key-encoding' controls"
                                    + " whether it is UTF-8 text or Base64. May be given without"
                                    + " 'scan.row-range.start-closed', which leaves the range open"
                                    + " below.");

    public static final ConfigOption<String> SCAN_ROW_RANGES =
            ConfigOptions.key("scan.row-ranges")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A semicolon-separated union of closed-start, open-end row-key"
                                    + " ranges, for example '[a,m);[q,)'. Either endpoint may be"
                                    + " omitted, but not both. Backslash escapes '\\', ';', ',',"
                                    + " '[', ']', '(' and ')' inside an endpoint. The ranges are"
                                    + " additive with 'scan.row-prefix' and 'scan.row-range.*',"
                                    + " and 'scan.row-key-encoding' controls how each endpoint is"
                                    + " decoded.");

    public static final ConfigOption<ChangeStreamChangelogMode> SCAN_CHANGE_STREAM_CHANGELOG_MODE =
            ConfigOptions.key("scan.change-stream.changelog-mode")
                    .enumType(ChangeStreamChangelogMode.class)
                    .noDefaultValue()
                    .withDescription(
                            "The physical changelog representation for Change Streams."
                                    + " ENVELOPE emits one insert-only generic mutation"
                                    + " envelope per Bigtable mutation.");

    public static final ConfigOption<ChangeStreamStartMode> SCAN_STARTUP_MODE =
            ConfigOptions.key("scan.startup.mode")
                    .enumType(ChangeStreamStartMode.class)
                    .noDefaultValue()
                    .withDescription(
                            "Where a fresh Change Streams source starts. Unset leaves the"
                                    + " DataStream builder's latest position unchanged.");

    public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP_MILLIS =
            ConfigOptions.key("scan.startup.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "The epoch-millisecond instant used by scan.startup.mode = timestamp.");

    public static final ConfigOption<ChangeStreamStartMode> SCAN_RESUME_FALLBACK_MODE =
            ConfigOptions.key("scan.resume-fallback.mode")
                    .enumType(ChangeStreamStartMode.class)
                    .noDefaultValue()
                    .withDescription(
                            "An explicit start position used only when a restored Change Streams"
                                    + " continuation has expired. Unset fails the restore.");

    public static final ConfigOption<Long> SCAN_RESUME_FALLBACK_TIMESTAMP_MILLIS =
            ConfigOptions.key("scan.resume-fallback.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "The epoch-millisecond instant used by scan.resume-fallback.mode ="
                                    + " timestamp.");

    public static final ConfigOption<Long> SCAN_END_TIMESTAMP_MILLIS =
            ConfigOptions.key("scan.end-timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Stop a Change Streams source after this epoch-millisecond instant."
                                    + " Unset keeps the source continuous and unbounded.");

    public static final ConfigOption<Integer> SCAN_MAX_CONCURRENT_STREAMS_PER_SUBTASK =
            ConfigOptions.key("scan.max-concurrent-streams-per-subtask")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The maximum open Change Streams partition reads in each source"
                                    + " subtask. Unset keeps the DataStream builder default.");

    // ------------------------------------------------------------------------
    //  Lookup
    // ------------------------------------------------------------------------

    public static final ConfigOption<Boolean> LOOKUP_ASYNC =
            ConfigOptions.key("lookup.async")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether point lookups use Bigtable's asynchronous read API. Full"
                                    + " caching is scan-backed and therefore supports synchronous"
                                    + " lookup only.");

    // ------------------------------------------------------------------------
    //  Sink
    // ------------------------------------------------------------------------

    public static final ConfigOption<String> SINK_APP_PROFILE_ID =
            ConfigOptions.key("sink.app-profile-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The app profile the writes are attributed to. Named for the"
                                    + " sink rather than shared, because a Data Boost profile"
                                    + " reads and cannot write, so one table legitimately scans"
                                    + " and writes under different profiles.");

    public static final ConfigOption<CreateDisposition> SINK_CREATE_DISPOSITION =
            ConfigOptions.key("sink.create-disposition")
                    .enumType(CreateDisposition.class)
                    .noDefaultValue()
                    .withDescription(
                            "Whether a missing table is created with the column families the DDL"
                                    + " declares, or the write fails. Creating requires at least"
                                    + " one 'sink.table-create.gc-rule.*' key.");

    public static final ConfigOption<InsertOnlyInputMode> SINK_INSERT_ONLY_INPUT_MODE =
            ConfigOptions.key("sink.insert-only-input-mode")
                    .enumType(InsertOnlyInputMode.class)
                    .defaultValue(InsertOnlyInputMode.UPSERT)
                    .withDescription(
                            "What changelog mode the sink advertises when the requested input"
                                    + " contains inserts alone. UPSERT exposes Flink conflict"
                                    + " strategies and is the default. INSERT_ONLY keeps a plain"
                                    + " INSERT portable when an ON CONFLICT clause is unavailable,"
                                    + " but disables that clause for the statement.");

    public static final ConfigOption<Boolean> SINK_CELL_TIMESTAMP_TRUNCATE_TO_MILLIS =
            ConfigOptions.key("sink.cell-timestamp.truncate-to-millis")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether a writable 'timestamp' metadata value loses its"
                                    + " sub-millisecond part before being sent. Disabled by"
                                    + " default, so the connector never changes an explicit"
                                    + " timestamp without an opt-in; Bigtable then rejects a"
                                    + " value that does not match its millisecond granularity.");

    public static final ConfigOption<Integer> SINK_TABLE_CREATE_GC_RULE_MAX_VERSIONS =
            ConfigOptions.key("sink.table-create.gc-rule.max-versions")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The garbage-collection rule a created column family takes: keep at"
                                    + " most this many versions of a cell. Applied to every family"
                                    + " the DDL declares, and combined with"
                                    + " 'sink.table-create.gc-rule.max-age' as a union when both"
                                    + " are set. A rule tree of the shape the DataStream API's"
                                    + " GcRule can express has no flat DDL form; a family needing"
                                    + " one is created out of band.");

    public static final ConfigOption<Duration> SINK_TABLE_CREATE_GC_RULE_MAX_AGE =
            ConfigOptions.key("sink.table-create.gc-rule.max-age")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The garbage-collection rule a created column family takes: drop a"
                                    + " cell older than this. Applied to every family the DDL"
                                    + " declares, and combined with"
                                    + " 'sink.table-create.gc-rule.max-versions' as a union when"
                                    + " both are set.");

    public static final ConfigOption<Long> SINK_BATCHING_ELEMENT_COUNT =
            ConfigOptions.key("sink.batching.element-count")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "How many entries the client batches before sending a MutateRows"
                                    + " request. One entry is one row's mutations, not one"
                                    + " mutation.");

    public static final ConfigOption<MemorySize> SINK_BATCHING_BYTE_SIZE =
            ConfigOptions.key("sink.batching.byte-size")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "How many bytes of entries the client batches before sending a"
                                    + " MutateRows request.");

    public static final ConfigOption<Integer> SINK_IN_FLIGHT_MAX_ENTRIES =
            ConfigOptions.key("sink.in-flight.max-entries")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Caps the entries the writer holds unacknowledged. At the cap the"
                                    + " writer yields to the task mailbox rather than blocking, so"
                                    + " keeping it below the client's own flow-control budget is"
                                    + " what keeps checkpoints progressing.");

    public static final ConfigOption<MemorySize> SINK_IN_FLIGHT_MAX_BYTES =
            ConfigOptions.key("sink.in-flight.max-bytes")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription("Caps the bytes the writer holds unacknowledged.");

    public static final ConfigOption<Integer> SINK_MAX_CONSECUTIVE_REJECTIONS =
            ConfigOptions.key("sink.max-consecutive-rejections")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "How many confirmed row-level rejections in a row a dropping failure"
                                    + " policy tolerates before the job fails, or -1 for no bound."
                                    + " A SQL table has no failure-policy option, so its sink"
                                    + " always fails the job on the first confirmed rejection and"
                                    + " this option cannot take effect; it exists so that the"
                                    + " DDL surface stays one key per writer knob.");

    public static final ConfigOption<Duration> SINK_RECOVERY_INITIAL_BACKOFF =
            ConfigOptions.key("sink.recovery.initial-backoff")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The first wait of the budget the sink spends repairing a missing"
                                    + " table or column family. It does not retry what the client"
                                    + " already retries.");

    public static final ConfigOption<Duration> SINK_RECOVERY_MAX_BACKOFF =
            ConfigOptions.key("sink.recovery.max-backoff")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The longest wait that repair budget grows to.");

    public static final ConfigOption<Integer> SINK_RECOVERY_MAX_ATTEMPTS =
            ConfigOptions.key("sink.recovery.max-attempts")
                    .intType()
                    .noDefaultValue()
                    .withDescription("How many repair attempts that budget allows.");

    public static final ConfigOption<Duration> SINK_DESTINATION_IDLE_TIMEOUT =
            ConfigOptions.key("sink.destination-idle-timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "How long a table's batcher is kept after its last write before the"
                                    + " writer closes it.");

    public static final ConfigOption<Boolean> SINK_METRICS_PER_DESTINATION =
            ConfigOptions.key("sink.metrics.per-destination")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Whether the writer also reports its counters per destination table.");

    private BigtableConnectorOptions() {}
}
