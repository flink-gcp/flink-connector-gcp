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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.table.sink.BigtableDynamicSink;
import io.github.flink.gcp.connector.bigtable.table.sink.TableCreateOptionsMapper;
import io.github.flink.gcp.connector.bigtable.table.sink.WriterOptionsMapper;
import io.github.flink.gcp.connector.bigtable.table.source.BigtableDynamicSource;

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
 * mode or the checkpoint interval.
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
                        BigtableConnectorOptions.SCAN_APP_PROFILE_ID,
                        BigtableConnectorOptions.SCAN_ROW_KEY_ENCODING,
                        BigtableConnectorOptions.SCAN_ROW_PREFIX,
                        BigtableConnectorOptions.SCAN_ROW_RANGE_START_CLOSED,
                        BigtableConnectorOptions.SCAN_ROW_RANGE_END_OPEN,
                        BigtableConnectorOptions.SCAN_ROW_RANGES,
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

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        ReadableConfig config = helper.getOptions();
        validateCredentialsMode(config);
        DataType physicalDataType = context.getPhysicalRowDataType();
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
