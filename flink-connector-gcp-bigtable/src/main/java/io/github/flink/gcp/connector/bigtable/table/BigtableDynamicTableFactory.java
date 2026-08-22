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

package io.github.flink.gcp.connector.bigtable.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.table.sink.BigtableDynamicSink;
import io.github.flink.gcp.connector.bigtable.table.sink.TableCreateOptionsMapper;
import io.github.flink.gcp.connector.bigtable.table.sink.WriterOptionsMapper;
import io.github.flink.gcp.connector.bigtable.table.source.BigtableChangeStreamDynamicSource;
import io.github.flink.gcp.connector.bigtable.table.source.BigtableChangeStreamEnvelopeSchema;
import io.github.flink.gcp.connector.bigtable.table.source.BigtableDynamicSource;
import io.github.flink.gcp.connector.bigtable.table.source.ChangeStreamStartPositionMapper;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Creates the {@code bigtable} table sink and table source from a {@code CREATE TABLE} statement's
 * options.
 *
 * <p>This is the only place a DDL option becomes a value. Value bounds stay in the connector's own
 * builders, and a value they reject is renamed to its option key ({@code OptionSetters}, issue
 * #1030); what this class owns are the checks whose message has to name <em>option keys</em> or
 * <em>columns</em> outright, which a builder's cannot — among them the shape of the DDL schema, the
 * primary key, a scan bound the client would silently widen, an option belonging to the scan mode
 * this DDL did not select, a Change Streams table used as a write target, and the credential keys
 * that are mutually exclusive. A table-creation key set under a disposition that creates nothing is
 * rejected on the same grounds, by the option mapper this class calls.
 *
 * <p>The identifier {@code bigtable} is also google/flink-connector-gcp's. A classpath carrying
 * both fails factory discovery loudly, which is the acceptable outcome: the natural name wins.
 *
 * <p>Nothing here reads the session configuration. The Bigtable sink is at-least-once in both
 * execution modes, and neither it nor a source has a rule that depends on the runtime mode or the
 * checkpoint interval. A source selects the bounded HBase-compatible row shape, the exact generic
 * Change Streams mutation envelope, or the constrained selected-cell upsert shape; the first is
 * bounded and the other two are unbounded unless an end timestamp bounds them. A source rejects the
 * other mode's options instead of ignoring them, and a Change Streams table is source-only: an
 * {@code INSERT INTO} over one is rejected rather than served as an ordinary upsert sink that
 * discards those options, and a table being written to is refused them too. What a sink keeps are
 * the scan and lookup options of a table it is also read with.
 */
@Internal
public class BigtableDynamicTableFactory
        implements DynamicTableSinkFactory, DynamicTableSourceFactory {

    /** The value of {@code 'connector'} that selects this factory. */
    public static final String IDENTIFIER = "bigtable";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>(
                Arrays.asList(
                        BigtableConnectorOptions.PROJECT,
                        BigtableConnectorOptions.INSTANCE,
                        BigtableConnectorOptions.TABLE));
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>(
                Arrays.asList(
                        BigtableConnectorOptions.EMULATOR_ENDPOINT,
                        BigtableConnectorOptions.SERVICE_ACCOUNT_KEY_FILE,
                        BigtableConnectorOptions.NULL_STRING_LITERAL,
                        BigtableConnectorOptions.SCAN_MODE,
                        BigtableConnectorOptions.SCAN_APP_PROFILE_ID,
                        BigtableConnectorOptions.SCAN_ROW_KEY_ENCODING,
                        BigtableConnectorOptions.SCAN_ROW_PREFIX,
                        BigtableConnectorOptions.SCAN_ROW_RANGE_START_CLOSED,
                        BigtableConnectorOptions.SCAN_ROW_RANGE_END_OPEN,
                        BigtableConnectorOptions.SCAN_ROW_RANGES,
                        BigtableConnectorOptions.SCAN_CHANGE_STREAM_CHANGELOG_MODE,
                        BigtableConnectorOptions.SCAN_CHANGE_STREAM_SELECTED_CELL_FAMILY,
                        BigtableConnectorOptions.SCAN_CHANGE_STREAM_SELECTED_CELL_QUALIFIER_BASE64,
                        BigtableConnectorOptions.SCAN_CHANGE_STREAM_SELECTED_CELL_SOURCE_CLUSTER_ID,
                        BigtableConnectorOptions.VALUE_FORMAT,
                        BigtableConnectorOptions.SCAN_STARTUP_MODE,
                        BigtableConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS,
                        BigtableConnectorOptions.SCAN_RESUME_FALLBACK_MODE,
                        BigtableConnectorOptions.SCAN_RESUME_FALLBACK_TIMESTAMP_MILLIS,
                        BigtableConnectorOptions.SCAN_END_TIMESTAMP_MILLIS,
                        BigtableConnectorOptions.SCAN_MAX_CONCURRENT_STREAMS_PER_SUBTASK,
                        FactoryUtil.SOURCE_PARALLELISM,
                        BigtableConnectorOptions.LOOKUP_ASYNC,
                        LookupOptions.CACHE_TYPE,
                        LookupOptions.MAX_RETRIES,
                        LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS,
                        LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE,
                        LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY,
                        LookupOptions.PARTIAL_CACHE_MAX_ROWS,
                        LookupOptions.FULL_CACHE_RELOAD_STRATEGY,
                        LookupOptions.FULL_CACHE_PERIODIC_RELOAD_INTERVAL,
                        LookupOptions.FULL_CACHE_PERIODIC_RELOAD_SCHEDULE_MODE,
                        LookupOptions.FULL_CACHE_TIMED_RELOAD_ISO_TIME,
                        LookupOptions.FULL_CACHE_TIMED_RELOAD_INTERVAL_IN_DAYS,
                        BigtableConnectorOptions.SINK_APP_PROFILE_ID,
                        BigtableConnectorOptions.SINK_CREATE_DISPOSITION,
                        BigtableConnectorOptions.SINK_INSERT_ONLY_INPUT_MODE,
                        BigtableConnectorOptions.SINK_CELL_TIMESTAMP_TRUNCATE_TO_MILLIS,
                        BigtableConnectorOptions.SINK_TABLE_CREATE_GC_RULE_MAX_VERSIONS,
                        BigtableConnectorOptions.SINK_TABLE_CREATE_GC_RULE_MAX_AGE,
                        BigtableConnectorOptions.SINK_BATCHING_ELEMENT_COUNT,
                        BigtableConnectorOptions.SINK_BATCHING_BYTE_SIZE,
                        BigtableConnectorOptions.SINK_IN_FLIGHT_MAX_ENTRIES,
                        BigtableConnectorOptions.SINK_IN_FLIGHT_MAX_BYTES,
                        BigtableConnectorOptions.SINK_MAX_CONSECUTIVE_REJECTIONS,
                        BigtableConnectorOptions.SINK_RECOVERY_INITIAL_BACKOFF,
                        BigtableConnectorOptions.SINK_RECOVERY_MAX_BACKOFF,
                        BigtableConnectorOptions.SINK_RECOVERY_MAX_ATTEMPTS,
                        BigtableConnectorOptions.SINK_DESTINATION_IDLE_TIMEOUT,
                        BigtableConnectorOptions.SINK_METRICS_PER_DESTINATION,
                        FactoryUtil.SINK_PARALLELISM));
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // Read the scan mode before validating, as the source does: a selected-cell DDL carries
        // value.<format>.* keys this direction discovers no format for, so helper.validate() would
        // reject those first and bury the reason the table cannot be written to at all.
        ScanMode preliminaryScanMode = helper.getOptions().get(BigtableConnectorOptions.SCAN_MODE);
        checkNotAChangeStreamTable(preliminaryScanMode);
        helper.validate();

        ReadableConfig config = helper.getOptions();
        validateCredentialsMode(config);
        checkSinkHasNoChangeStreamOptions(context);
        // After the check that refuses an option outright; see validateEmulatorEndpoint.
        validateEmulatorEndpoint(config);
        DataType physicalDataType = context.getPhysicalRowDataType();
        BigtableTableSchema schema =
                BigtableTableSchema.of((RowType) physicalDataType.getLogicalType());
        checkPrimaryKeyIsTheRowKey(context, schema);
        checkSinkHasSomewhereToWrite(schema);

        return BigtableDynamicSink.builder()
                .schema(schema)
                .destination(
                        TableDestination.of(
                                config.get(BigtableConnectorOptions.PROJECT),
                                config.get(BigtableConnectorOptions.INSTANCE),
                                config.get(BigtableConnectorOptions.TABLE)))
                .nullStringLiteral(config.get(BigtableConnectorOptions.NULL_STRING_LITERAL))
                .appProfileId(
                        optionalNonBlank(config, BigtableConnectorOptions.SINK_APP_PROFILE_ID))
                .serviceAccountKeyFile(
                        config.getOptional(BigtableConnectorOptions.SERVICE_ACCOUNT_KEY_FILE)
                                .orElse(null))
                .writerOptions(WriterOptionsMapper.map(config))
                .createDisposition(
                        config.getOptional(BigtableConnectorOptions.SINK_CREATE_DISPOSITION)
                                .orElse(null))
                .insertOnlyInputMode(
                        config.get(BigtableConnectorOptions.SINK_INSERT_ONLY_INPUT_MODE))
                .truncateCellTimestampToMillis(
                        config.get(BigtableConnectorOptions.SINK_CELL_TIMESTAMP_TRUNCATE_TO_MILLIS))
                .tableCreateOptions(TableCreateOptionsMapper.map(config, schema))
                .emulatorEndpoint(
                        config.getOptional(BigtableConnectorOptions.EMULATOR_ENDPOINT).orElse(null))
                .parallelism(config.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null))
                // A declared primary key is the row-key column alone, checked above, so the
                // planner's upsert key is the row key and a key-only delete carries it (#470).
                .keyOnlyDeletesAreSafe(context.getPrimaryKeyIndexes().length > 0)
                .build();
    }

    /**
     * Builds the change-stream source. The decoding format is {@code null} unless the changelog
     * mode is selected-cell, which is the only mode that decodes a cell value through a format.
     */
    private static DynamicTableSource createChangeStreamSource(
            Context context,
            ReadableConfig config,
            DataType physicalDataType,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> decodingFormat) {
        ChangeStreamChangelogMode changelogMode =
                config.getOptional(BigtableConnectorOptions.SCAN_CHANGE_STREAM_CHANGELOG_MODE)
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                String.format(
                                                        "Option '%s' is required when '%s' ="
                                                                + " '%s'. Set it to one of: %s.",
                                                        BigtableConnectorOptions
                                                                .SCAN_CHANGE_STREAM_CHANGELOG_MODE
                                                                .key(),
                                                        BigtableConnectorOptions.SCAN_MODE.key(),
                                                        ScanMode.CHANGE_STREAM,
                                                        changelogModeValues())));
        validateChangeStreamChangelogOptions(context, changelogMode);
        String appProfileId =
                config.getOptional(BigtableConnectorOptions.SCAN_APP_PROFILE_ID)
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                String.format(
                                                        "Option '%s' is required when '%s' ="
                                                                + " '%s' because Bigtable Change"
                                                                + " Streams require a single-cluster"
                                                                + " application profile.",
                                                        BigtableConnectorOptions.SCAN_APP_PROFILE_ID
                                                                .key(),
                                                        BigtableConnectorOptions.SCAN_MODE.key(),
                                                        ScanMode.CHANGE_STREAM)));
        checkNotBlank(appProfileId, BigtableConnectorOptions.SCAN_APP_PROFILE_ID);
        Integer maxConcurrentStreams =
                config.getOptional(BigtableConnectorOptions.SCAN_MAX_CONCURRENT_STREAMS_PER_SUBTASK)
                        .orElse(null);
        if (maxConcurrentStreams != null && maxConcurrentStreams <= 0) {
            throw new ValidationException(
                    String.format(
                            "Option '%s' must be positive, but was %d.",
                            BigtableConnectorOptions.SCAN_MAX_CONCURRENT_STREAMS_PER_SUBTASK.key(),
                            maxConcurrentStreams));
        }

        TableDestination destination =
                TableDestination.of(
                        config.get(BigtableConnectorOptions.PROJECT),
                        config.get(BigtableConnectorOptions.INSTANCE),
                        config.get(BigtableConnectorOptions.TABLE));
        String serviceAccountKeyFile =
                config.getOptional(BigtableConnectorOptions.SERVICE_ACCOUNT_KEY_FILE).orElse(null);
        StartPosition startPosition =
                ChangeStreamStartPositionMapper.map(
                        config,
                        BigtableConnectorOptions.SCAN_STARTUP_MODE,
                        BigtableConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS);
        StartPosition resumeFallback =
                ChangeStreamStartPositionMapper.map(
                        config,
                        BigtableConnectorOptions.SCAN_RESUME_FALLBACK_MODE,
                        BigtableConnectorOptions.SCAN_RESUME_FALLBACK_TIMESTAMP_MILLIS);
        Instant endTime =
                config.getOptional(BigtableConnectorOptions.SCAN_END_TIMESTAMP_MILLIS)
                        .map(millis -> Instant.ofEpochMilli(millis.longValue()))
                        .orElse(null);
        Integer parallelism = config.getOptional(FactoryUtil.SOURCE_PARALLELISM).orElse(null);

        if (changelogMode == ChangeStreamChangelogMode.ENVELOPE) {
            BigtableChangeStreamEnvelopeSchema.validate(physicalDataType);
            if (context.getPrimaryKeyIndexes().length > 0) {
                throw new ValidationException(
                        "A 'bigtable' Change Streams envelope is an insert-only mutation log and"
                                + " must not declare a primary key.");
            }
            return BigtableChangeStreamDynamicSource.builder()
                    .destination(destination)
                    .appProfileId(appProfileId)
                    .serviceAccountKeyFile(serviceAccountKeyFile)
                    .startPosition(startPosition)
                    .resumeFallback(resumeFallback)
                    .endTime(endTime)
                    .maxConcurrentStreamsPerSubtask(maxConcurrentStreams)
                    .parallelism(parallelism)
                    .physicalDataType(physicalDataType)
                    .build();
        }

        if (!decodingFormat.getChangelogMode().equals(ChangelogMode.insertOnly())) {
            throw new ValidationException(
                    String.format(
                            "Format '%s' must be insert-only in Bigtable selected-cell mode; the"
                                    + " connector derives UPDATE_AFTER and DELETE from the atomic"
                                    + " mutation protocol.",
                            config.get(BigtableConnectorOptions.VALUE_FORMAT)));
        }
        SelectedCellTableSchema schema =
                SelectedCellTableSchema.of(physicalDataType, context.getPrimaryKeyIndexes());
        String family =
                requireNonBlank(
                        config, BigtableConnectorOptions.SCAN_CHANGE_STREAM_SELECTED_CELL_FAMILY);
        String qualifier =
                requireConfigured(
                        config,
                        BigtableConnectorOptions.SCAN_CHANGE_STREAM_SELECTED_CELL_QUALIFIER_BASE64);
        String sourceClusterId =
                requireNonBlank(
                        config,
                        BigtableConnectorOptions
                                .SCAN_CHANGE_STREAM_SELECTED_CELL_SOURCE_CLUSTER_ID);
        return BigtableChangeStreamDynamicSource.builder()
                .destination(destination)
                .appProfileId(appProfileId)
                .serviceAccountKeyFile(serviceAccountKeyFile)
                .startPosition(startPosition)
                .resumeFallback(resumeFallback)
                .endTime(endTime)
                .maxConcurrentStreamsPerSubtask(maxConcurrentStreams)
                .parallelism(parallelism)
                .selectedCellSchema(schema)
                .decodingFormat(decodingFormat)
                .selectedCellFamily(family)
                .selectedCellQualifier(
                        RowKeyDecoder.decodeBase64(
                                BigtableConnectorOptions
                                        .SCAN_CHANGE_STREAM_SELECTED_CELL_QUALIFIER_BASE64,
                                qualifier))
                .selectedCellSourceClusterId(sourceClusterId)
                .build();
    }

    /**
     * Renders every changelog mode as a quoted, comma-separated list.
     *
     * <p>Derived from {@link ChangeStreamChangelogMode#values()} so a mode added later reaches the
     * message without it being edited. The quoted spellings are {@code toString()}'s, which is what
     * a DDL must carry — the constant names are not parseable.
     *
     * @return the modes, in declaration order
     */
    private static String changelogModeValues() {
        return Arrays.stream(ChangeStreamChangelogMode.values())
                .map(mode -> "'" + mode + "'")
                .collect(Collectors.joining(", "));
    }

    private static void validateChangeStreamChangelogOptions(
            Context context, ChangeStreamChangelogMode changelogMode) {
        if (changelogMode == ChangeStreamChangelogMode.SELECTED_CELL) {
            return;
        }
        List<String> present = presentOptionKeys(context, selectedCellOptions());
        if (!present.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Options %s are valid only when '%s' = '%s'.",
                            String.join(", ", present),
                            BigtableConnectorOptions.SCAN_CHANGE_STREAM_CHANGELOG_MODE.key(),
                            ChangeStreamChangelogMode.SELECTED_CELL));
        }
    }

    /**
     * Returns the quoted keys of the given options that the {@code CREATE TABLE} statement set, in
     * the order the options are listed. Each caller keeps its own sentence: the two validators
     * reject a different set for a different reason, and only the scan they share lives here.
     */
    private static List<String> presentOptionKeys(Context context, List<ConfigOption<?>> options) {
        Set<String> configured = context.getCatalogTable().getOptions().keySet();
        List<String> present = new ArrayList<>();
        for (ConfigOption<?> option : options) {
            if (configured.contains(option.key())) {
                present.add("'" + option.key() + "'");
            }
        }
        return present;
    }

    private static List<ConfigOption<?>> selectedCellOptions() {
        return Arrays.asList(
                BigtableConnectorOptions.SCAN_CHANGE_STREAM_SELECTED_CELL_FAMILY,
                BigtableConnectorOptions.SCAN_CHANGE_STREAM_SELECTED_CELL_QUALIFIER_BASE64,
                BigtableConnectorOptions.SCAN_CHANGE_STREAM_SELECTED_CELL_SOURCE_CLUSTER_ID,
                BigtableConnectorOptions.VALUE_FORMAT);
    }

    private static String requireConfigured(ReadableConfig config, ConfigOption<String> option) {
        return config.getOptional(option)
                .orElseThrow(
                        () ->
                                new ValidationException(
                                        String.format(
                                                "Option '%s' is required in Bigtable"
                                                        + " selected-cell mode.",
                                                option.key())));
    }

    /**
     * Returns an option that was not required, rejecting it when it was set but blank.
     *
     * <p>The counterpart to {@link #requireNonBlank} for an optional option; Cloud Tasks' {@code
     * HttpTargetSpec} arrived at the same shape and name independently. Without it the value
     * reaches the {@code @Internal} builder unchecked, and where it goes from there differs by
     * path: the scan and sink providers hand it to the DataStream builder, which refuses it with a
     * message naming a Java setter rather than the option the user wrote, while the lookup provider
     * never builds one — it passes the value to {@code BigtableDataClients}, whose only guard is a
     * null check, so a blank profile reached the service per lookup.
     */
    @Nullable
    private static String optionalNonBlank(ReadableConfig config, ConfigOption<String> option) {
        String value = config.getOptional(option).orElse(null);
        if (value != null) {
            checkNotBlank(value, option);
        }
        return value;
    }

    private static String requireNonBlank(ReadableConfig config, ConfigOption<String> option) {
        String value = requireConfigured(config, option);
        checkNotBlank(value, option);
        return value;
    }

    /**
     * Rejects a blank value for an option that was set. Separate from the presence check above
     * because the three options this guards state their required-ness differently — one is required
     * by its own message, one by the selected-cell message, and one is optional altogether — while
     * the blankness rejection is the same sentence for all three.
     */
    private static void checkNotBlank(String value, ConfigOption<String> option) {
        if (value.isBlank()) {
            throw new ValidationException(
                    String.format("Option '%s' must not be blank.", option.key()));
        }
    }

    /**
     * Rejects a Change Streams table used as an {@code INSERT INTO} target.
     *
     * <p>Without this the sink builds over the named table and discards the DDL's Change Streams
     * options. A schema check refuses the shapes both published modes use, but for the wrong
     * reason: {@link BigtableTableSchema#of(RowType)} names column shape and never the scan mode.
     * Any DDL it does admit — one atomic column and a {@code ROW<...>} family — builds a working
     * sink.
     */
    private static void checkNotAChangeStreamTable(ScanMode scanMode) {
        if (scanMode != ScanMode.CHANGE_STREAM) {
            return;
        }
        throw new ValidationException(
                String.format(
                        "A 'bigtable' table with '%s' = '%s' is source-only and cannot be written"
                                + " to, because Change Streams is a mutation log rather than a"
                                + " table. Remove the option, or write to a table declared without"
                                + " it.",
                        BigtableConnectorOptions.SCAN_MODE.key(), ScanMode.CHANGE_STREAM));
    }

    /**
     * Rejects the Change Streams options on a table being written to. Its own sentence rather than
     * {@link #validateSourceModeOptions}'s, whose remedy — select the matching scan mode — is a
     * dead end here, because selecting it is what {@link #checkNotAChangeStreamTable} refuses.
     */
    private static void checkSinkHasNoChangeStreamOptions(Context context) {
        List<String> present = presentOptionKeys(context, changeStreamSourceOptions());
        if (!present.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Options %s are not valid on a 'bigtable' table that is written to:"
                                    + " they configure the Change Streams source, and a Change"
                                    + " Streams table is source-only. Remove them.",
                            String.join(", ", present)));
        }
    }

    private static void validateSourceModeOptions(Context context, ScanMode scanMode) {
        List<ConfigOption<?>> incompatible =
                scanMode == ScanMode.CHANGE_STREAM
                        ? boundedSourceOptions()
                        : changeStreamSourceOptions();
        List<String> present = presentOptionKeys(context, incompatible);
        if (!present.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Options %s are not valid when '%s' = '%s'. Remove them or select the"
                                    + " matching scan mode.",
                            String.join(", ", present),
                            BigtableConnectorOptions.SCAN_MODE.key(),
                            scanMode));
        }
    }

    private static List<ConfigOption<?>> changeStreamSourceOptions() {
        List<ConfigOption<?>> options = new ArrayList<>();
        options.add(BigtableConnectorOptions.SCAN_CHANGE_STREAM_CHANGELOG_MODE);
        options.addAll(selectedCellOptions());
        options.addAll(
                Arrays.asList(
                        BigtableConnectorOptions.SCAN_STARTUP_MODE,
                        BigtableConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS,
                        BigtableConnectorOptions.SCAN_RESUME_FALLBACK_MODE,
                        BigtableConnectorOptions.SCAN_RESUME_FALLBACK_TIMESTAMP_MILLIS,
                        BigtableConnectorOptions.SCAN_END_TIMESTAMP_MILLIS,
                        BigtableConnectorOptions.SCAN_MAX_CONCURRENT_STREAMS_PER_SUBTASK));
        return options;
    }

    private static List<ConfigOption<?>> boundedSourceOptions() {
        return Arrays.asList(
                BigtableConnectorOptions.EMULATOR_ENDPOINT,
                BigtableConnectorOptions.NULL_STRING_LITERAL,
                BigtableConnectorOptions.SCAN_ROW_KEY_ENCODING,
                BigtableConnectorOptions.SCAN_ROW_PREFIX,
                BigtableConnectorOptions.SCAN_ROW_RANGE_START_CLOSED,
                BigtableConnectorOptions.SCAN_ROW_RANGE_END_OPEN,
                BigtableConnectorOptions.SCAN_ROW_RANGES,
                BigtableConnectorOptions.LOOKUP_ASYNC,
                LookupOptions.CACHE_TYPE,
                LookupOptions.MAX_RETRIES,
                LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS,
                LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE,
                LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY,
                LookupOptions.PARTIAL_CACHE_MAX_ROWS,
                LookupOptions.FULL_CACHE_RELOAD_STRATEGY,
                LookupOptions.FULL_CACHE_PERIODIC_RELOAD_INTERVAL,
                LookupOptions.FULL_CACHE_PERIODIC_RELOAD_SCHEDULE_MODE,
                LookupOptions.FULL_CACHE_TIMED_RELOAD_ISO_TIME,
                LookupOptions.FULL_CACHE_TIMED_RELOAD_INTERVAL_IN_DAYS);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig preliminaryConfig = helper.getOptions();
        ScanMode preliminaryScanMode = preliminaryConfig.get(BigtableConnectorOptions.SCAN_MODE);
        ChangeStreamChangelogMode preliminaryChangelogMode =
                preliminaryConfig
                        .getOptional(BigtableConnectorOptions.SCAN_CHANGE_STREAM_CHANGELOG_MODE)
                        .orElse(null);
        DecodingFormat<DeserializationSchema<RowData>> decodingFormat = null;
        if (preliminaryScanMode == ScanMode.CHANGE_STREAM
                && preliminaryChangelogMode == ChangeStreamChangelogMode.SELECTED_CELL) {
            decodingFormat =
                    helper.discoverDecodingFormat(
                            DeserializationFormatFactory.class,
                            BigtableConnectorOptions.VALUE_FORMAT);
        }
        helper.validate();

        ReadableConfig config = helper.getOptions();
        validateCredentialsMode(config);
        DataType physicalDataType = context.getPhysicalRowDataType();
        ScanMode scanMode = config.get(BigtableConnectorOptions.SCAN_MODE);
        validateSourceModeOptions(context, scanMode);
        // After the check that refuses an option outright; see validateEmulatorEndpoint.
        validateEmulatorEndpoint(config);
        if (scanMode == ScanMode.CHANGE_STREAM) {
            return createChangeStreamSource(context, config, physicalDataType, decodingFormat);
        }
        BigtableTableSchema schema =
                BigtableTableSchema.of((RowType) physicalDataType.getLogicalType());
        checkPrimaryKeyIsTheRowKey(context, schema);
        // checkSinkHasSomewhereToWrite is deliberately not applied: a row-key-only table is a
        // legitimate thing to read, served by a keys-only filter chain.
        RowKeyEncoding rowKeyEncoding = config.get(BigtableConnectorOptions.SCAN_ROW_KEY_ENCODING);
        List<ByteString> prefixes = decodePrefixes(context, config, rowKeyEncoding);
        ByteString rangeStartClosed =
                config.getOptional(BigtableConnectorOptions.SCAN_ROW_RANGE_START_CLOSED)
                        .map(
                                value ->
                                        RowKeyDecoder.decode(
                                                BigtableConnectorOptions
                                                        .SCAN_ROW_RANGE_START_CLOSED,
                                                rowKeyEncoding,
                                                value))
                        .orElse(null);
        ByteString rangeEndOpen =
                config.getOptional(BigtableConnectorOptions.SCAN_ROW_RANGE_END_OPEN)
                        .map(
                                value ->
                                        RowKeyDecoder.decode(
                                                BigtableConnectorOptions.SCAN_ROW_RANGE_END_OPEN,
                                                rowKeyEncoding,
                                                value))
                        .orElse(null);
        List<ByteStringRange> rowRanges =
                config.getOptional(BigtableConnectorOptions.SCAN_ROW_RANGES)
                        .map(value -> RowRangeParser.parse(rowKeyEncoding, value))
                        .orElse(Collections.emptyList());

        return BigtableDynamicSource.builder()
                .schema(schema)
                .destination(
                        TableDestination.of(
                                config.get(BigtableConnectorOptions.PROJECT),
                                config.get(BigtableConnectorOptions.INSTANCE),
                                config.get(BigtableConnectorOptions.TABLE)))
                .nullStringLiteral(config.get(BigtableConnectorOptions.NULL_STRING_LITERAL))
                .appProfileId(
                        optionalNonBlank(config, BigtableConnectorOptions.SCAN_APP_PROFILE_ID))
                .serviceAccountKeyFile(
                        config.getOptional(BigtableConnectorOptions.SERVICE_ACCOUNT_KEY_FILE)
                                .orElse(null))
                .prefixes(prefixes)
                .rangeStartClosed(rangeStartClosed)
                .rangeEndOpen(rangeEndOpen)
                .rowRanges(rowRanges)
                .emulatorEndpoint(
                        config.getOptional(BigtableConnectorOptions.EMULATOR_ENDPOINT).orElse(null))
                .parallelism(config.getOptional(FactoryUtil.SOURCE_PARALLELISM).orElse(null))
                .lookupOptions(BigtableLookupConfig.from(config))
                .producedDataType(physicalDataType)
                .build();
    }

    private static void validateCredentialsMode(ReadableConfig config) {
        config.getOptional(BigtableConnectorOptions.SERVICE_ACCOUNT_KEY_FILE)
                .ifPresent(
                        path ->
                                checkNotBlank(
                                        path, BigtableConnectorOptions.SERVICE_ACCOUNT_KEY_FILE));
        if (config.getOptional(BigtableConnectorOptions.SERVICE_ACCOUNT_KEY_FILE).isPresent()
                && config.getOptional(BigtableConnectorOptions.EMULATOR_ENDPOINT).isPresent()) {
            throw new ValidationException(
                    String.format(
                            "Options '%s' and '%s' cannot be combined: an emulator uses a"
                                    + " plaintext channel with no credentials. Remove one of the"
                                    + " two options.",
                            BigtableConnectorOptions.SERVICE_ACCOUNT_KEY_FILE.key(),
                            BigtableConnectorOptions.EMULATOR_ENDPOINT.key()));
        }
    }

    /**
     * Reaches the lookup path, which {@link BigtableDynamicSource}'s runtimes left until they
     * opened on a TaskManager (issue #1009, {@code docs/adr/0127}).
     *
     * <p>Call it after every check that refuses an option outright, never before one: a DDL told to
     * remove {@code emulator-endpoint} is not helped by an answer about its shape.
     *
     * <p>The rejection is left as the {@code IllegalArgumentException} the parse throws, which
     * {@code FactoryUtil} wraps, matching {@code TableDestination.of} above rather than the {@code
     * ValidationException} the option-shape checks in this class raise directly.
     */
    private static void validateEmulatorEndpoint(ReadableConfig config) {
        config.getOptional(BigtableConnectorOptions.EMULATOR_ENDPOINT)
                .ifPresent(
                        value ->
                                EmulatorEndpoint.parse(
                                        value, BigtableConnectorOptions.EMULATOR_ENDPOINT.key()));
    }

    private static List<ByteString> decodePrefixes(
            Context context, ReadableConfig config, RowKeyEncoding rowKeyEncoding) {
        String raw =
                context.getCatalogTable()
                        .getOptions()
                        .get(BigtableConnectorOptions.SCAN_ROW_PREFIX.key());
        if (raw != null) {
            String trimmed = raw.trim();
            // Flink's list parser retains an empty middle element but discards an empty element at
            // either edge. Reject those edges before parsing so ';', ';a', and 'a;' cannot silently
            // become an absent or narrower bound.
            if (trimmed.isEmpty()
                    || trimmed.charAt(0) == ';'
                    || trimmed.charAt(trimmed.length() - 1) == ';') {
                throw RowKeyDecoder.emptyRowKey(BigtableConnectorOptions.SCAN_ROW_PREFIX);
            }
        }
        List<String> configured =
                config.getOptional(BigtableConnectorOptions.SCAN_ROW_PREFIX)
                        .orElse(Collections.emptyList());
        List<ByteString> decoded = new ArrayList<>(configured.size());
        for (String value : configured) {
            decoded.add(
                    RowKeyDecoder.decode(
                            BigtableConnectorOptions.SCAN_ROW_PREFIX, rowKeyEncoding, value));
        }
        return decoded;
    }

    /**
     * Rejects a primary key that is anything other than the row-key column.
     *
     * <p>Declaring none is allowed, exactly as it is in Flink's HBase connector, so a DDL moves
     * between the two unchanged: a Bigtable write is keyed on the row key whether or not the DDL
     * says so. What cannot be allowed is a primary key naming something else, because the planner
     * would then key its upserts on a column this sink does not write rows by.
     */
    private static void checkPrimaryKeyIsTheRowKey(Context context, BigtableTableSchema schema) {
        int[] primaryKeyIndexes = context.getPrimaryKeyIndexes();
        if (primaryKeyIndexes.length == 0) {
            return;
        }
        if (primaryKeyIndexes.length > 1 || primaryKeyIndexes[0] != schema.getRowKeyIndex()) {
            throw new ValidationException(
                    String.format(
                            "The primary key of a 'bigtable' table must be its row-key column"
                                    + " alone, which here is '%s'. A Bigtable row is addressed by"
                                    + " its key and by nothing else.",
                            schema.getRowKeyName()));
        }
    }

    private static void checkSinkHasSomewhereToWrite(BigtableTableSchema schema) {
        // Qualifiers, not families: ROW<> parses, so a table can declare a family that holds
        // nothing, and every one of its records would then produce a mutation with no cell in it.
        // A row-key-only table is a legitimate thing to read; it is not a thing to write, because
        // Bigtable refuses an entry that mutates nothing.
        for (BigtableTableSchema.Family family : schema.getFamilies()) {
            if (!family.getQualifiers().isEmpty()) {
                return;
            }
        }
        throw new ValidationException(
                String.format(
                        "A 'bigtable' table written to needs at least one column family with a"
                                + " qualifier in it, and this one declares none besides its row key"
                                + " '%s'. A family is a ROW<...> column whose fields are its"
                                + " qualifiers.",
                        schema.getRowKeyName()));
    }
}
