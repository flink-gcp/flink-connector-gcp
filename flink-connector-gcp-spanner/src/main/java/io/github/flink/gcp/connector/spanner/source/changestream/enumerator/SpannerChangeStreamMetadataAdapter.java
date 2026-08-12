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

package io.github.flink.gcp.connector.spanner.source.changestream.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Reads retention and partition-mode options without exposing the Spanner client to tests. */
@Internal
final class SpannerChangeStreamMetadataAdapter implements SpannerChangeStreamCoordinatorClient {

    private static final String RETENTION_PERIOD = "retention_period";
    private static final String PARTITION_MODE = "partition_mode";
    private static final String IMMUTABLE_KEY_RANGE = "IMMUTABLE_KEY_RANGE";
    private static final String MUTABLE_KEY_RANGE = "MUTABLE_KEY_RANGE";
    private static final Pattern DURATION = Pattern.compile("([1-9][0-9]*)([dhms])");

    private final String database;
    private final String changeStreamName;
    private final Duration absentRetentionFallback;
    private final Supplier<Dialect> dialect;
    private final Function<Statement, ResultSet> executeQuery;
    private final Runnable closeService;

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
    public void validatePartitionMode() throws IOException {
        String mode = readOption(PARTITION_MODE);
        if (mode == null || IMMUTABLE_KEY_RANGE.equals(mode)) {
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

    @Override
    public Duration retention() throws IOException {
        String value = readOption(RETENTION_PERIOD);
        return value == null ? absentRetentionFallback : parseDuration(value);
    }

    @Nullable
    private String readOption(String optionName) throws IOException {
        Dialect currentDialect;
        try {
            currentDialect = dialect.get();
        } catch (SpannerException e) {
            throw new IOException("Failed to read the dialect of " + database + ".", e);
        }
        Statement statement = optionQuery(currentDialect, changeStreamName, optionName);
        try (ResultSet rows = executeQuery.apply(statement)) {
            String value = null;
            int rowCount = 0;
            while (rows.next()) {
                rowCount++;
                if (rowCount > 1) {
                    throw new IOException(
                            "INFORMATION_SCHEMA returned more than one "
                                    + optionName
                                    + " row for Spanner change stream "
                                    + changeStreamName
                                    + ".");
                }
                if (!rows.isNull(0)) {
                    value = rows.getString(0);
                }
            }
            return value;
        } catch (SpannerException e) {
            throw new IOException(
                    "Failed to read "
                            + optionName
                            + " for Spanner change stream "
                            + changeStreamName
                            + " in "
                            + database
                            + " from INFORMATION_SCHEMA.",
                    e);
        }
    }

    static Statement optionQuery(Dialect dialect, String streamName, String optionName) {
        String sql;
        String streamParameter;
        String optionParameter;
        switch (dialect) {
            case GOOGLE_STANDARD_SQL:
                sql =
                        "SELECT OPTION_VALUE FROM INFORMATION_SCHEMA.CHANGE_STREAM_OPTIONS"
                                + " WHERE CHANGE_STREAM_CATALOG = ''"
                                + " AND CHANGE_STREAM_SCHEMA = ''"
                                + " AND CHANGE_STREAM_NAME = @stream_name"
                                + " AND OPTION_NAME = @option_name";
                streamParameter = "stream_name";
                optionParameter = "option_name";
                break;
            case POSTGRESQL:
                sql =
                        "SELECT option_value FROM information_schema.change_stream_options"
                                + " WHERE change_stream_schema = 'public'"
                                + " AND change_stream_name = $1"
                                + " AND option_name = $2";
                streamParameter = "p1";
                optionParameter = "p2";
                break;
            default:
                throw new IllegalStateException(
                        "Unsupported Spanner dialect "
                                + dialect
                                + "; change-stream metadata supports GOOGLE_STANDARD_SQL and"
                                + " POSTGRESQL only.");
        }
        return Statement.newBuilder(sql)
                .bind(streamParameter)
                .to(streamName)
                .bind(optionParameter)
                .to(optionName)
                .build();
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
