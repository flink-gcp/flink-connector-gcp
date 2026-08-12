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

package io.github.flink.gcp.connector.spanner.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.spanner.AbstractSpannerEmulatorITCase;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;
import io.github.flink.gcp.connector.spanner.source.changestream.ValueCaptureType;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerChangeStreamDeserializationSchema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Runs the complete Change Streams source against both dialects of the Spanner emulator. */
class SpannerChangeStreamSourceEmulatorITCase extends AbstractSpannerEmulatorITCase {

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void readsAcrossSchemaAndValueCaptureChanges(Dialect dialect) throws Exception {
        SpannerDatabase database =
                createDatabase(
                        dialect, singersDdl(dialect), "CREATE CHANGE STREAM changes FOR singers");
        Timestamp first =
                client(database)
                        .write(
                                Collections.singletonList(
                                        Mutation.newInsertBuilder("singers")
                                                .set("id")
                                                .to(1L)
                                                .set("name")
                                                .to("Ada")
                                                .build()));

        updateDdl(database, addAgeDdl(dialect), valueCaptureDdl(dialect));
        Timestamp second =
                client(database)
                        .write(
                                Collections.singletonList(
                                        Mutation.newUpdateBuilder("singers")
                                                .set("id")
                                                .to(1L)
                                                .set("name")
                                                .to("Ada Lovelace")
                                                .set("age")
                                                .to(36L)
                                                .build()));

        List<DataChangeRecord> records = run(database, instant(first), instant(second));

        assertThat(records)
                .extracting(DataChangeRecord::getCommitTimestamp)
                .containsExactly(instant(first), instant(second));
        assertThat(records)
                .extracting(DataChangeRecord::getValueCaptureType)
                .containsExactly(ValueCaptureType.OLD_AND_NEW_VALUES, ValueCaptureType.NEW_ROW);
        assertThat(records.get(0).getColumnTypes())
                .extracting(DataChangeRecord.ColumnType::getName)
                .containsExactly("id", "name");
        assertThat(records.get(1).getColumnTypes())
                .extracting(DataChangeRecord.ColumnType::getName)
                .containsExactly("id", "name", "age");
        assertThat(records)
                .flatExtracting(DataChangeRecord::getMods)
                .allSatisfy(mod -> assertThat(mod.getKeysJson()).contains("1"));
        assertThat(records.get(0).getMods().get(0).getNewValuesJson())
                .hasValueSatisfying(json -> assertThat(json).contains("Ada"));
        assertThat(records.get(1).getMods().get(0).getNewValuesJson())
                .hasValueSatisfying(
                        json -> assertThat(json).contains("Ada Lovelace").contains("36"));
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void projectsColumnsBeforeDeserialization(Dialect dialect) throws Exception {
        SpannerDatabase database =
                createDatabase(
                        dialect,
                        singersWithSecretDdl(dialect),
                        "CREATE CHANGE STREAM changes FOR singers");
        Timestamp commit =
                client(database)
                        .write(
                                Collections.singletonList(
                                        Mutation.newInsertBuilder("singers")
                                                .set("id")
                                                .to(1L)
                                                .set("name")
                                                .to("Ada")
                                                .set("secret")
                                                .to("hidden")
                                                .build()));
        Timestamp end =
                client(database)
                        .write(
                                Collections.singletonList(
                                        Mutation.newUpdateBuilder("singers")
                                                .set("id")
                                                .to(1L)
                                                .set("name")
                                                .to("Ada Lovelace")
                                                .set("secret")
                                                .to("still hidden")
                                                .build()));

        List<DataChangeRecord> records =
                run(
                        database,
                        instant(commit),
                        instant(end),
                        builder ->
                                builder.columnIncludeList(
                                        Collections.singletonList("singers\\.name")));

        assertThat(records).hasSize(2);
        assertThat(records)
                .allSatisfy(
                        record -> {
                            assertThat(record.getColumnTypes())
                                    .extracting(DataChangeRecord.ColumnType::getName)
                                    .containsExactly("id", "name");
                            JsonObject keys =
                                    JsonParser.parseString(record.getMods().get(0).getKeysJson())
                                            .getAsJsonObject();
                            assertThat(keys.keySet()).containsExactly("id");
                            assertThat(keys.get("id").getAsString()).isEqualTo("1");
                            assertThat(record.getMods().get(0).getNewValuesJson())
                                    .hasValueSatisfying(
                                            json ->
                                                    assertThat(json)
                                                            .contains("Ada")
                                                            .doesNotContain("hidden"));
                        });
    }

    private static List<DataChangeRecord> run(SpannerDatabase database, Instant start, Instant end)
            throws Exception {
        return run(database, start, end, ignored -> {});
    }

    private static List<DataChangeRecord> run(
            SpannerDatabase database,
            Instant start,
            Instant end,
            Consumer<SpannerChangeStreamSourceBuilder<DataChangeRecord>> configure)
            throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(2);

        SpannerChangeStreamSourceBuilder<DataChangeRecord> builder =
                SpannerChangeStreamSource.<DataChangeRecord>builder()
                        .database(database)
                        .changeStreamName("changes")
                        .deserializer(new IdentityDeserializer())
                        .startPosition(StartPosition.at(start))
                        .heartbeatInterval(Duration.ofSeconds(1))
                        .maxConcurrentQueriesPerSubtask(2)
                        .emulatorEndpoint(emulatorEndpoint())
                        .endTimestamp(end);
        configure.accept(builder);
        SpannerChangeStreamSource<DataChangeRecord> source = builder.build();

        List<DataChangeRecord> records = new ArrayList<>();
        try (CloseableIterator<DataChangeRecord> collected =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "spanner-change-stream")
                        .executeAndCollect()) {
            collected.forEachRemaining(records::add);
        }
        return records.stream()
                .sorted(java.util.Comparator.comparing(DataChangeRecord::getCommitTimestamp))
                .collect(Collectors.toList());
    }

    private static String singersDdl(Dialect dialect) {
        return dialect == Dialect.POSTGRESQL
                ? "CREATE TABLE singers (id bigint NOT NULL PRIMARY KEY, name varchar(64))"
                : "CREATE TABLE singers (id INT64 NOT NULL, name STRING(64)) PRIMARY KEY (id)";
    }

    private static String addAgeDdl(Dialect dialect) {
        return dialect == Dialect.POSTGRESQL
                ? "ALTER TABLE singers ADD COLUMN age bigint"
                : "ALTER TABLE singers ADD COLUMN age INT64";
    }

    private static String singersWithSecretDdl(Dialect dialect) {
        return dialect == Dialect.POSTGRESQL
                ? "CREATE TABLE singers (id bigint NOT NULL PRIMARY KEY, name varchar(64), secret varchar(64))"
                : "CREATE TABLE singers (id INT64 NOT NULL, name STRING(64), secret STRING(64)) PRIMARY KEY (id)";
    }

    private static String valueCaptureDdl(Dialect dialect) {
        return dialect == Dialect.POSTGRESQL
                ? "ALTER CHANGE STREAM changes SET (value_capture_type = 'NEW_ROW')"
                : "ALTER CHANGE STREAM changes SET OPTIONS (value_capture_type = 'NEW_ROW')";
    }

    private static Instant instant(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    private static final class IdentityDeserializer
            implements SpannerChangeStreamDeserializationSchema<DataChangeRecord> {

        private static final long serialVersionUID = 1L;

        @Override
        public DataChangeRecord deserialize(DataChangeRecord record) {
            return record;
        }

        @Override
        public TypeInformation<DataChangeRecord> getProducedType() {
            return TypeInformation.of(DataChangeRecord.class);
        }
    }
}
