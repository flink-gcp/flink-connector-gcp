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

package io.github.flink.gcp.connector.spanner.source.changestream.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads and reports the Change Stream configuration without exposing the Spanner client to tests.
 */
@Internal
final class SpannerChangeStreamMetadataAdapter implements SpannerChangeStreamCoordinatorClient {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultSpannerChangeStreamCoordinatorClientFactory.class);

    private static final String RETENTION_PERIOD = "retention_period";
    private static final String PARTITION_MODE = "partition_mode";
    private static final String VALUE_CAPTURE_TYPE = "value_capture_type";
    private static final String EXCLUDE_TTL_DELETES = "exclude_ttl_deletes";
    private static final String EXCLUDE_INSERT = "exclude_insert";
    private static final String EXCLUDE_UPDATE = "exclude_update";
    private static final String EXCLUDE_DELETE = "exclude_delete";
    private static final String ALLOW_TXN_EXCLUSION = "allow_txn_exclusion";

    private static final String IMMUTABLE_KEY_RANGE = "IMMUTABLE_KEY_RANGE";
    private static final String MUTABLE_KEY_RANGE = "MUTABLE_KEY_RANGE";
    private static final String OLD_AND_NEW_VALUES = "OLD_AND_NEW_VALUES";
    private static final Pattern DURATION = Pattern.compile("([1-9][0-9]*)([dhms])");

    private final String database;
    private final String changeStreamName;
    private final Duration absentRetentionFallback;
    private final Supplier<Dialect> dialect;
    private final Function<Statement, ResultSet> executeQuery;
    private final Runnable closeService;

    @Nullable private Duration initializedRetention;

    SpannerChangeStreamMetadataAdapter(
            String database,
            String changeStreamName,
            Duration absentRetentionFallback,
            Supplier<Dialect> dialect,
            Function<Statement, ResultSet> executeQuery,
            Runnable closeService) {
        this.database = Preconditions.checkNotNull(database, "database must not be null");
        this.changeStreamName =
                Preconditions.checkNotNull(changeStreamName, "changeStreamName must not be null");
        this.absentRetentionFallback =
                Preconditions.checkNotNull(
                        absentRetentionFallback, "absentRetentionFallback must not be null");
        this.dialect = Preconditions.checkNotNull(dialect, "dialect must not be null");
        this.executeQuery =
                Preconditions.checkNotNull(executeQuery, "executeQuery must not be null");
        this.closeService =
                Preconditions.checkNotNull(closeService, "closeService must not be null");
    }

    @Override
    public synchronized Duration initialize() throws IOException {
        if (initializedRetention != null) {
            return initializedRetention;
        }

        Dialect currentDialect = readDialect();
        boolean watchesAll = readWatchesAll(currentDialect);
        List<String> explicitColumnTables = readExplicitColumnTables(currentDialect);
        Map<String, String> options = readOptions(currentDialect);

        String partitionMode = optionOrDefault(options, PARTITION_MODE, IMMUTABLE_KEY_RANGE);
        validatePartitionMode(partitionMode);
        String valueCaptureType = optionOrDefault(options, VALUE_CAPTURE_TYPE, OLD_AND_NEW_VALUES);
        Duration retention =
                options.containsKey(RETENTION_PERIOD) && options.get(RETENTION_PERIOD) != null
                        ? parseDuration(options.get(RETENTION_PERIOD))
                        : absentRetentionFallback;

        boolean excludeTtlDeletes = booleanOption(options, EXCLUDE_TTL_DELETES);
        boolean excludeInserts = booleanOption(options, EXCLUDE_INSERT);
        boolean excludeUpdates = booleanOption(options, EXCLUDE_UPDATE);
        boolean excludeDeletes = booleanOption(options, EXCLUDE_DELETE);
        boolean allowTransactionExclusion = booleanOption(options, ALLOW_TXN_EXCLUSION);

        LOG.info(
                "Spanner change stream {} in {} initialized with scope={}, retention={},"
                        + " partitionMode={}, valueCaptureType={}, excludeTtlDeletes={},"
                        + " excludeInserts={}, excludeUpdates={}, excludeDeletes={},"
                        + " allowTransactionExclusion={}.",
                changeStreamName,
                database,
                watchesAll ? "ALL" : "TABLES",
                retention,
                partitionMode,
                valueCaptureType,
                excludeTtlDeletes,
                excludeInserts,
                excludeUpdates,
                excludeDeletes,
                allowTransactionExclusion);
        if (!explicitColumnTables.isEmpty()) {
            LOG.warn(
                    "Spanner change stream {} in {} watches an explicit column list for table(s)"
                            + " {}. Columns added later are not watched automatically; alter the"
                            + " change stream when its intended schema changes.",
                    changeStreamName,
                    database,
                    explicitColumnTables);
        }

        initializedRetention = retention;
        return retention;
    }

    private Dialect readDialect() throws IOException {
        try {
            return dialect.get();
        } catch (SpannerException e) {
            throw new IOException("Failed to read the dialect of " + database + ".", e);
        }
    }

    private boolean readWatchesAll(Dialect currentDialect) throws IOException {
        try (ResultSet rows = executeQuery.apply(streamQuery(currentDialect, changeStreamName))) {
            if (!rows.next()) {
                throw new IOException(
                        "Spanner change stream "
                                + changeStreamName
                                + " does not exist or is not visible in INFORMATION_SCHEMA for "
                                + database
                                + ".");
            }
            boolean watchesAll =
                    currentDialect == Dialect.GOOGLE_STANDARD_SQL
                            ? rows.getBoolean(0)
                            : yesNo(rows.getString(0), "change_streams.all");
            if (rows.next()) {
                throw new IOException(
                        "INFORMATION_SCHEMA returned more than one definition for Spanner change"
                                + " stream "
                                + changeStreamName
                                + " in "
                                + database
                                + ".");
            }
            return watchesAll;
        } catch (SpannerException e) {
            throw metadataFailure("definition", e);
        }
    }

    private List<String> readExplicitColumnTables(Dialect currentDialect) throws IOException {
        Set<String> tables = new LinkedHashSet<>();
        try (ResultSet rows = executeQuery.apply(tableQuery(currentDialect, changeStreamName))) {
            while (rows.next()) {
                boolean allColumns =
                        currentDialect == Dialect.GOOGLE_STANDARD_SQL
                                ? rows.getBoolean(2)
                                : yesNo(rows.getString(2), "change_stream_tables.all_columns");
                if (!allColumns) {
                    String schema = rows.isNull(0) ? "" : rows.getString(0);
                    String table = rows.getString(1);
                    tables.add(schema == null || schema.isEmpty() ? table : schema + "." + table);
                }
            }
            return new ArrayList<>(tables);
        } catch (SpannerException e) {
            throw metadataFailure("watched tables", e);
        }
    }

    private Map<String, String> readOptions(Dialect currentDialect) throws IOException {
        Map<String, String> options = new LinkedHashMap<>();
        try (ResultSet rows = executeQuery.apply(optionsQuery(currentDialect, changeStreamName))) {
            while (rows.next()) {
                String name = rows.getString(0).toLowerCase(Locale.ROOT);
                if (options.containsKey(name)) {
                    throw new IOException(
                            "INFORMATION_SCHEMA returned more than one "
                                    + name
                                    + " row for Spanner change stream "
                                    + changeStreamName
                                    + " in "
                                    + database
                                    + ".");
                }
                options.put(name, rows.isNull(1) ? null : rows.getString(1));
            }
            return options;
        } catch (SpannerException e) {
            throw metadataFailure("options", e);
        }
    }

    private IOException metadataFailure(String part, SpannerException cause) {
        return new IOException(
                "Failed to read "
                        + part
                        + " for Spanner change stream "
                        + changeStreamName
                        + " in "
                        + database
                        + " from INFORMATION_SCHEMA.",
                cause);
    }

    private void validatePartitionMode(String mode) {
        if (IMMUTABLE_KEY_RANGE.equals(mode)) {
            return;
        }
        if (MUTABLE_KEY_RANGE.equals(mode)) {
            throw new IllegalArgumentException(
                    "Spanner change stream "
                            + changeStreamName
                            + " uses MUTABLE_KEY_RANGE partition mode. This source supports only"
                            + " immutable-key-range records and does not partially consume its"
                            + " partition start, end, move-in, or move-out records.");
        }
        throw new IllegalArgumentException(
                "Spanner change stream "
                        + changeStreamName
                        + " uses unsupported partition mode '"
                        + mode
                        + "'.");
    }

    private static String optionOrDefault(
            Map<String, String> options, String optionName, String defaultValue) {
        String value = options.get(optionName);
        return value == null ? defaultValue : value;
    }

    private static boolean booleanOption(Map<String, String> options, String optionName) {
        String value = options.get(optionName);
        if (value == null) {
            return false;
        }
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new IllegalArgumentException(
                "Spanner change-stream option "
                        + optionName
                        + " must be true or false, but was '"
                        + value
                        + "'.");
    }

    private static boolean yesNo(String value, String column) {
        if ("YES".equals(value)) {
            return true;
        }
        if ("NO".equals(value)) {
            return false;
        }
        throw new IllegalArgumentException(
                "Spanner PostgreSQL INFORMATION_SCHEMA column "
                        + column
                        + " must be YES or NO, but was '"
                        + value
                        + "'.");
    }

    static Statement streamQuery(Dialect dialect, String streamName) {
        switch (dialect) {
            case GOOGLE_STANDARD_SQL:
                return Statement.newBuilder(
                                "SELECT `ALL` FROM INFORMATION_SCHEMA.CHANGE_STREAMS"
                                        + " WHERE CHANGE_STREAM_CATALOG = ''"
                                        + " AND CHANGE_STREAM_SCHEMA = ''"
                                        + " AND CHANGE_STREAM_NAME = @stream_name")
                        .bind("stream_name")
                        .to(streamName)
                        .build();
            case POSTGRESQL:
                return Statement.newBuilder(
                                "SELECT \"all\" FROM information_schema.change_streams"
                                        + " WHERE change_stream_schema = 'public'"
                                        + " AND change_stream_name = $1")
                        .bind("p1")
                        .to(streamName)
                        .build();
            default:
                throw unsupportedDialect(dialect);
        }
    }

    static Statement tableQuery(Dialect dialect, String streamName) {
        switch (dialect) {
            case GOOGLE_STANDARD_SQL:
                return Statement.newBuilder(
                                "SELECT TABLE_SCHEMA, TABLE_NAME, ALL_COLUMNS"
                                        + " FROM INFORMATION_SCHEMA.CHANGE_STREAM_TABLES"
                                        + " WHERE CHANGE_STREAM_CATALOG = ''"
                                        + " AND CHANGE_STREAM_SCHEMA = ''"
                                        + " AND CHANGE_STREAM_NAME = @stream_name"
                                        + " ORDER BY TABLE_SCHEMA, TABLE_NAME")
                        .bind("stream_name")
                        .to(streamName)
                        .build();
            case POSTGRESQL:
                return Statement.newBuilder(
                                "SELECT table_schema, table_name, all_columns"
                                        + " FROM information_schema.change_stream_tables"
                                        + " WHERE change_stream_schema = 'public'"
                                        + " AND change_stream_name = $1"
                                        + " ORDER BY table_schema, table_name")
                        .bind("p1")
                        .to(streamName)
                        .build();
            default:
                throw unsupportedDialect(dialect);
        }
    }

    static Statement optionsQuery(Dialect dialect, String streamName) {
        switch (dialect) {
            case GOOGLE_STANDARD_SQL:
                return Statement.newBuilder(
                                "SELECT OPTION_NAME, OPTION_VALUE"
                                        + " FROM INFORMATION_SCHEMA.CHANGE_STREAM_OPTIONS"
                                        + " WHERE CHANGE_STREAM_CATALOG = ''"
                                        + " AND CHANGE_STREAM_SCHEMA = ''"
                                        + " AND CHANGE_STREAM_NAME = @stream_name"
                                        + " ORDER BY OPTION_NAME")
                        .bind("stream_name")
                        .to(streamName)
                        .build();
            case POSTGRESQL:
                return Statement.newBuilder(
                                "SELECT option_name, option_value"
                                        + " FROM information_schema.change_stream_options"
                                        + " WHERE change_stream_schema = 'public'"
                                        + " AND change_stream_name = $1"
                                        + " ORDER BY option_name")
                        .bind("p1")
                        .to(streamName)
                        .build();
            default:
                throw unsupportedDialect(dialect);
        }
    }

    private static IllegalStateException unsupportedDialect(Dialect dialect) {
        return new IllegalStateException(
                "Unsupported Spanner dialect "
                        + dialect
                        + "; change-stream metadata supports GOOGLE_STANDARD_SQL and POSTGRESQL"
                        + " only.");
    }

    static Duration parseDuration(String value) {
        Preconditions.checkNotNull(value, "value must not be null");
        String normalized = value.toLowerCase(Locale.ROOT);
        Matcher matcher = DURATION.matcher(normalized);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    "Spanner change-stream retention must be a positive integer followed by"
                            + " d, h, m, or s, but was '"
                            + value
                            + "'.");
        }
        long amount;
        try {
            amount = Long.parseLong(matcher.group(1));
            switch (matcher.group(2)) {
                case "d":
                    return Duration.ofDays(amount);
                case "h":
                    return Duration.ofHours(amount);
                case "m":
                    return Duration.ofMinutes(amount);
                case "s":
                    return Duration.ofSeconds(amount);
                default:
                    throw new AssertionError("duration regex accepted an unknown unit");
            }
        } catch (ArithmeticException | NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Spanner change-stream retention '" + value + "' is too large.", e);
        }
    }

    @Override
    public void close() {
        closeService.run();
    }
}
