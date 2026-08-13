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
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.table.sink.BigtableDynamicSink;
import io.github.flink.gcp.connector.bigtable.table.sink.TableCreateOptionsMapper;
import io.github.flink.gcp.connector.bigtable.table.sink.WriterOptionsMapper;
import io.github.flink.gcp.connector.bigtable.table.source.BigtableChangeStreamDynamicSource;
import io.github.flink.gcp.connector.bigtable.table.source.BigtableChangeStreamEnvelopeSchema;
import io.github.flink.gcp.connector.bigtable.table.source.BigtableDynamicSource;
import io.github.flink.gcp.connector.bigtable.table.source.ChangeStreamStartPositionMapper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Creates the {@code bigtable} table sink and table source from a {@code CREATE TABLE} statement's
 * options.
 *
 * <p>This is the only place a DDL option becomes a value. Value validation stays in the connector's
 * own builders, so a SQL user gets the same message a DataStream user does; what this class owns
 * are the checks whose message has to name <em>option keys</em> or <em>columns</em>, which a
 * builder's cannot: the shape of the DDL schema, the primary key, a table-creation key set under a
 * disposition that creates nothing, and a scan bound the client would silently widen.
 *
 * <p>The identifier {@code bigtable} is also google/flink-connector-gcp's. A classpath carrying
 * both fails factory discovery loudly, which is the acceptable outcome: the natural name wins.
 *
 * <p>Nothing here reads the session configuration. The Bigtable sink is at-least-once in both
 * execution modes, the source is a bounded scan, and neither has a rule that depends on the runtime
 * mode or the checkpoint interval. A source selects the bounded HBase-compatible row shape, the
 * exact generic Change Streams mutation envelope, or the constrained selected-cell upsert shape;
 * options for the other mode are rejected instead of ignored.
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
        helper.validate();

        ReadableConfig config = helper.getOptions();
        validateCredentialsMode(config);
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
                        config.getOptional(BigtableConnectorOptions.SINK_APP_PROFILE_ID)
                                .orElse(null))
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

    private static DynamicTableSource createChangeStreamSource(
            Context context,
            ReadableConfig config,
            DataType physicalDataType,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat) {
        ChangeStreamChangelogMode changelogMode =
                config.getOptional(BigtableConnectorOptions.SCAN_CHANGE_STREAM_CHANGELOG_MODE)
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                String.format(
                                                        "Option '%s' is required when '%s' ="
                                                                + " '%s'. Set it to '%s'.",
                                                        BigtableConnectorOptions
                                                                .SCAN_CHANGE_STREAM_CHANGELOG_MODE
                                                                .key(),
                                                        BigtableConnectorOptions.SCAN_MODE.key(),
                                                        ScanMode.CHANGE_STREAM,
                                                        ChangeStreamChangelogMode.ENVELOPE)));
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
        if (appProfileId.isBlank()) {
            throw new ValidationException(
                    String.format(
                            "Option '%s' must not be blank.",
                            BigtableConnectorOptions.SCAN_APP_PROFILE_ID.key()));
        }
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
            return new BigtableChangeStreamDynamicSource(
                    destination,
                    appProfileId,
                    serviceAccountKeyFile,
                    startPosition,
                    resumeFallback,
                    endTime,
                    maxConcurrentStreams,
                    parallelism,
                    physicalDataType);
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
        return new BigtableChangeStreamDynamicSource(
                destination,
                appProfileId,
                serviceAccountKeyFile,
                startPosition,
                resumeFallback,
                endTime,
                maxConcurrentStreams,
                parallelism,
                schema,
                decodingFormat,
                family,
                RowKeyDecoder.decodeBase64(
                        BigtableConnectorOptions.SCAN_CHANGE_STREAM_SELECTED_CELL_QUALIFIER_BASE64,
                        qualifier),
                sourceClusterId);
    }

    private static void validateChangeStreamChangelogOptions(
            Context context, ChangeStreamChangelogMode changelogMode) {
        if (changelogMode == ChangeStreamChangelogMode.SELECTED_CELL) {
            return;
        }
        Set<String> configured = context.getCatalogTable().getOptions().keySet();
        List<String> present = new ArrayList<>();
        for (ConfigOption<?> option : selectedCellOptions()) {
            if (configured.contains(option.key())) {
                present.add("'" + option.key() + "'");
            }
        }
        if (!present.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Options %s are valid only when '%s' = '%s'.",
                            String.join(", ", present),
                            BigtableConnectorOptions.SCAN_CHANGE_STREAM_CHANGELOG_MODE.key(),
                            ChangeStreamChangelogMode.SELECTED_CELL));
        }
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

    private static String requireNonBlank(ReadableConfig config, ConfigOption<String> option) {
        String value = requireConfigured(config, option);
        if (value.isBlank()) {
            throw new ValidationException(
                    String.format("Option '%s' must not be blank.", option.key()));
        }
        return value;
    }

    private static void validateSourceModeOptions(Context context, ScanMode scanMode) {
        Set<String> configured = context.getCatalogTable().getOptions().keySet();
        List<ConfigOption<?>> incompatible =
                scanMode == ScanMode.CHANGE_STREAM
                        ? boundedSourceOptions()
                        : changeStreamSourceOptions();
        List<String> present = new ArrayList<>();
        for (ConfigOption<?> option : incompatible) {
            if (configured.contains(option.key())) {
                present.add("'" + option.key() + "'");
            }
        }
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
                        config.getOptional(BigtableConnectorOptions.SCAN_APP_PROFILE_ID)
                                .orElse(null))
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
                        path -> {
                            if (path.isBlank()) {
                                throw new ValidationException(
                                        String.format(
                                                "Option '%s' must not be blank.",
                                                BigtableConnectorOptions.SERVICE_ACCOUNT_KEY_FILE
                                                        .key()));
                            }
                        });
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
