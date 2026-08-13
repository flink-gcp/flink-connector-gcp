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

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpannerChangeStreamMetadataAdapterTest {

    private static final Type GOOGLE_STREAM_ROW =
            Type.struct(Type.StructField.of("all", Type.bool()));
    private static final Type POSTGRES_STREAM_ROW =
            Type.struct(Type.StructField.of("all", Type.string()));
    private static final Type GOOGLE_TABLE_ROW =
            Type.struct(
                    Type.StructField.of("table_schema", Type.string()),
                    Type.StructField.of("table_name", Type.string()),
                    Type.StructField.of("all_columns", Type.bool()));
    private static final Type POSTGRES_TABLE_ROW =
            Type.struct(
                    Type.StructField.of("table_schema", Type.string()),
                    Type.StructField.of("table_name", Type.string()),
                    Type.StructField.of("all_columns", Type.string()));
    private static final Type OPTION_ROW =
            Type.struct(
                    Type.StructField.of("option_name", Type.string()),
                    Type.StructField.of("option_value", Type.string()));

    @Test
    void buildsDialectSpecificParameterizedQueries() {
        Statement googleStream =
                SpannerChangeStreamMetadataAdapter.streamQuery(
                        Dialect.GOOGLE_STANDARD_SQL, "orders");
        Statement postgresStream =
                SpannerChangeStreamMetadataAdapter.streamQuery(Dialect.POSTGRESQL, "orders");
        Statement googleTables =
                SpannerChangeStreamMetadataAdapter.tableQuery(
                        Dialect.GOOGLE_STANDARD_SQL, "orders");
        Statement postgresOptions =
                SpannerChangeStreamMetadataAdapter.optionsQuery(Dialect.POSTGRESQL, "orders");

        assertThat(googleStream.getSql())
                .contains("INFORMATION_SCHEMA.CHANGE_STREAMS")
                .contains("@stream_name");
        assertThat(postgresStream.getSql())
                .contains("information_schema.change_streams")
                .contains("change_stream_schema = 'public'")
                .contains("$1");
        assertThat(googleTables.getSql()).contains("CHANGE_STREAM_TABLES");
        assertThat(postgresOptions.getSql()).contains("change_stream_options");
        assertThat(googleStream.getParameters().get("stream_name").getString()).isEqualTo("orders");
        assertThat(postgresOptions.getParameters().get("p1").getString()).isEqualTo("orders");
    }

    @Test
    void initializesGoogleSqlDefaultsOnceAndLogsTheEffectiveConfiguration() throws Exception {
        AtomicInteger queries = new AtomicInteger();
        SpannerChangeStreamMetadataAdapter adapter =
                adapter(
                        Dialect.GOOGLE_STANDARD_SQL,
                        answers(queries, googleStreamRows(true), googleTableRows(), optionRows()));

        try (LogCapture capture =
                LogCapture.of(
                        DefaultSpannerChangeStreamCoordinatorClientFactory.class,
                        LogCapture.Level.INFO)) {
            assertThat(adapter.initialize()).isEqualTo(Duration.ofDays(7));
            assertThat(adapter.initialize()).isEqualTo(Duration.ofDays(7));

            assertThat(queries.get()).isEqualTo(3);
            assertThat(capture.getMessages())
                    .singleElement()
                    .asString()
                    .contains("scope=ALL")
                    .contains("retention=PT168H")
                    .contains("partitionMode=IMMUTABLE_KEY_RANGE")
                    .contains("valueCaptureType=OLD_AND_NEW_VALUES")
                    .contains("excludeTtlDeletes=false")
                    .contains("allowTransactionExclusion=false");
        }
    }

    @Test
    void initializesPostgreSqlOptionsAndWarnsAboutExplicitColumnLists() throws Exception {
        SpannerChangeStreamMetadataAdapter adapter =
                adapter(
                        Dialect.POSTGRESQL,
                        answers(
                                new AtomicInteger(),
                                postgresStreamRows("NO"),
                                postgresTableRows(
                                        table("public", "all_columns", "YES"),
                                        table("sales", "orders", "NO")),
                                optionRows(
                                        "retention_period",
                                        "36h",
                                        "partition_mode",
                                        "IMMUTABLE_KEY_RANGE",
                                        "value_capture_type",
                                        "NEW_ROW",
                                        "exclude_ttl_deletes",
                                        "TRUE",
                                        "exclude_insert",
                                        "true",
                                        "exclude_update",
                                        "false",
                                        "exclude_delete",
                                        "true",
                                        "allow_txn_exclusion",
                                        "true")));

        try (LogCapture capture =
                LogCapture.of(
                        DefaultSpannerChangeStreamCoordinatorClientFactory.class,
                        LogCapture.Level.INFO)) {
            assertThat(adapter.initialize()).isEqualTo(Duration.ofHours(36));

            assertThat(capture.getMessages())
                    .anySatisfy(
                            message ->
                                    assertThat(message)
                                            .asString()
                                            .contains("scope=TABLES")
                                            .contains("retention=PT36H")
                                            .contains("valueCaptureType=NEW_ROW")
                                            .contains("excludeTtlDeletes=true")
                                            .contains("excludeInserts=true")
                                            .contains("excludeUpdates=false")
                                            .contains("excludeDeletes=true")
                                            .contains("allowTransactionExclusion=true"))
                    .anySatisfy(
                            message ->
                                    assertThat(message)
                                            .asString()
                                            .contains("explicit column list")
                                            .contains("sales.orders")
                                            .contains("not watched automatically"));
        }
    }

    @Test
    void wholeTableWatchingDoesNotWarn() throws Exception {
        SpannerChangeStreamMetadataAdapter adapter =
                adapter(
                        Dialect.POSTGRESQL,
                        answers(
                                new AtomicInteger(),
                                postgresStreamRows("NO"),
                                postgresTableRows(table("public", "orders", "YES")),
                                optionRows()));

        try (LogCapture capture =
                LogCapture.of(DefaultSpannerChangeStreamCoordinatorClientFactory.class)) {
            adapter.initialize();
            assertThat(capture.getMessages()).isEmpty();
        }
    }

    @Test
    void parsesEveryDocumentedRetentionUnit() {
        assertThat(SpannerChangeStreamMetadataAdapter.parseDuration("7d"))
                .isEqualTo(Duration.ofDays(7));
        assertThat(SpannerChangeStreamMetadataAdapter.parseDuration("36H"))
                .isEqualTo(Duration.ofHours(36));
        assertThat(SpannerChangeStreamMetadataAdapter.parseDuration("90m"))
                .isEqualTo(Duration.ofMinutes(90));
        assertThat(SpannerChangeStreamMetadataAdapter.parseDuration("3600s"))
                .isEqualTo(Duration.ofHours(1));
        assertThatThrownBy(() -> SpannerChangeStreamMetadataAdapter.parseDuration("PT24H"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive integer");
    }

    @Test
    void rejectsMutableUnknownAndMalformedOptionValues() {
        assertThatThrownBy(
                        () ->
                                adapterWithOptions("partition_mode", "MUTABLE_KEY_RANGE")
                                        .initialize())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("MUTABLE_KEY_RANGE")
                .hasMessageContaining("partition start, end, move-in, or move-out");
        assertThatThrownBy(() -> adapterWithOptions("partition_mode", "NEW_MODE").initialize())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("NEW_MODE");
        assertThatThrownBy(() -> adapterWithOptions("exclude_insert", "YES").initialize())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exclude_insert")
                .hasMessageContaining("true or false");
    }

    @Test
    void rejectsMissingOrDuplicateDefinitionsAndOptions() {
        assertThatThrownBy(
                        () ->
                                adapter(
                                                Dialect.GOOGLE_STANDARD_SQL,
                                                answers(
                                                        new AtomicInteger(),
                                                        googleStreamRows(),
                                                        googleTableRows(),
                                                        optionRows()))
                                        .initialize())
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("does not exist or is not visible")
                .hasMessageContaining("orders");
        assertThatThrownBy(
                        () ->
                                adapter(
                                                Dialect.GOOGLE_STANDARD_SQL,
                                                answers(
                                                        new AtomicInteger(),
                                                        googleStreamRows(true, false),
                                                        googleTableRows(),
                                                        optionRows()))
                                        .initialize())
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("more than one definition");
        assertThatThrownBy(
                        () ->
                                adapterWithOptions(
                                                "retention_period", "7d", "retention_period", "36h")
                                        .initialize())
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("more than one retention_period row");
    }

    @Test
    void rejectsUnexpectedPostgreSqlYesNoAndCloseReleasesTheHandle() {
        AtomicInteger closes = new AtomicInteger();
        SpannerChangeStreamMetadataAdapter malformed =
                new SpannerChangeStreamMetadataAdapter(
                        "db",
                        "orders",
                        Duration.ofDays(7),
                        () -> Dialect.POSTGRESQL,
                        answers(
                                new AtomicInteger(),
                                postgresStreamRows("MAYBE"),
                                postgresTableRows(),
                                optionRows()),
                        closes::incrementAndGet);

        assertThatThrownBy(malformed::initialize)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("YES or NO");
        malformed.close();
        assertThat(closes.get()).isEqualTo(1);
    }

    private static SpannerChangeStreamMetadataAdapter adapterWithOptions(String... options) {
        return adapter(
                Dialect.GOOGLE_STANDARD_SQL,
                answers(
                        new AtomicInteger(),
                        googleStreamRows(true),
                        googleTableRows(),
                        optionRows(options)));
    }

    private static SpannerChangeStreamMetadataAdapter adapter(
            Dialect dialect, Function<Statement, ResultSet> query) {
        return new SpannerChangeStreamMetadataAdapter(
                "projects/p/instances/i/databases/d",
                "orders",
                Duration.ofDays(7),
                () -> dialect,
                query,
                () -> {});
    }

    private static Function<Statement, ResultSet> answers(
            AtomicInteger queries, ResultSet stream, ResultSet tables, ResultSet options) {
        return statement -> {
            queries.incrementAndGet();
            String sql = statement.getSql().toLowerCase(java.util.Locale.ROOT);
            if (sql.contains("change_stream_options")) {
                return options;
            }
            if (sql.contains("change_stream_tables")) {
                return tables;
            }
            if (sql.contains("change_streams")) {
                return stream;
            }
            throw new AssertionError("Unexpected metadata query: " + statement.getSql());
        };
    }

    private static ResultSet googleStreamRows(boolean... values) {
        List<Struct> rows = new ArrayList<>();
        for (boolean value : values) {
            rows.add(Struct.newBuilder().set("all").to(value).build());
        }
        return ResultSets.forRows(GOOGLE_STREAM_ROW, rows);
    }

    private static ResultSet postgresStreamRows(String... values) {
        List<Struct> rows = new ArrayList<>();
        for (String value : values) {
            rows.add(Struct.newBuilder().set("all").to(value).build());
        }
        return ResultSets.forRows(POSTGRES_STREAM_ROW, rows);
    }

    private static ResultSet googleTableRows(Struct... rows) {
        return ResultSets.forRows(GOOGLE_TABLE_ROW, java.util.Arrays.asList(rows));
    }

    private static ResultSet postgresTableRows(Struct... rows) {
        return ResultSets.forRows(POSTGRES_TABLE_ROW, java.util.Arrays.asList(rows));
    }

    private static Struct table(String schema, String table, String allColumns) {
        return Struct.newBuilder()
                .set("table_schema")
                .to(schema)
                .set("table_name")
                .to(table)
                .set("all_columns")
                .to(allColumns)
                .build();
    }

    private static ResultSet optionRows(String... namesAndValues) {
        if (namesAndValues.length % 2 != 0) {
            throw new IllegalArgumentException("option names and values must be paired");
        }
        List<Struct> rows = new ArrayList<>();
        for (int i = 0; i < namesAndValues.length; i += 2) {
            rows.add(
                    Struct.newBuilder()
                            .set("option_name")
                            .to(namesAndValues[i])
                            .set("option_value")
                            .to(namesAndValues[i + 1])
                            .build());
        }
        return ResultSets.forRows(OPTION_ROW, rows);
    }
}
