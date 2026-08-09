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

package io.github.flink.gcp.connector.bigquery.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializer;
import io.github.flink.gcp.connector.bigquery.source.split.BigQueryReadStreamSplit;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The source read end to end against the emulator, through the production {@code createEnumerator}
 * and {@code createReader} a job takes.
 *
 * <p>What is deliberately not tested here is offset resume: the emulator ignores {@code
 * ReadRowsRequest.offset} and answers every call from row zero, so a test asserting a resume would
 * pass while proving the opposite. {@code BigQueryEmulatorReadDeviationITCase} pins that behaviour,
 * {@code BigQuerySourceReaderTest} covers the resume against a server that honours offsets, and the
 * gated real-GCP case covers it against BigQuery itself.
 */
class BigQuerySourceEmulatorITCase extends AbstractBigQuerySourceEmulatorITCase {

    private static final String TABLE = "people";

    /**
     * A reader schema written by hand, as a user writes one: it names the columns it wants with
     * their natural types, and Avro's schema resolution maps the session's schema onto it.
     */
    private static final String READER_SCHEMA =
            "{\"type\":\"record\",\"name\":\"Person\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"long\"},"
                    + "{\"name\":\"name\",\"type\":\"string\"}]}";

    @BeforeAll
    static void seed() throws Exception {
        createTable(
                TABLE,
                Field.newBuilder("id", StandardSQLTypeName.INT64)
                        .setMode(Field.Mode.REQUIRED)
                        .build(),
                Field.newBuilder("name", StandardSQLTypeName.STRING)
                        .setMode(Field.Mode.REQUIRED)
                        .build());
        insert(TABLE, "id, name", "(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')");
    }

    @Test
    void readsEveryRow() throws Exception {
        List<String> names = run(sourceBuilder().build(), 1);

        assertThat(names).containsExactlyInAnyOrder("a", "b", "c", "d", "e");
    }

    @Test
    void appliesProjectionAndRestriction() throws Exception {
        Source<GenericRecord, BigQueryReadStreamSplit, ?> source =
                sourceBuilder()
                        .selectedFields("name")
                        .rowRestriction("id >= 4")
                        .deserializer(
                                BigQueryRowDeserializer.genericRecord(
                                        "{\"type\":\"record\",\"name\":\"Person\",\"fields\":["
                                                + "{\"name\":\"name\",\"type\":\"string\"}]}"))
                        .build();

        assertThat(run(source, 1)).containsExactlyInAnyOrder("d", "e");
    }

    @Test
    void aSubtaskWithoutAStreamFinishesTheJob() throws Exception {
        // The emulator answers with exactly one stream, so at parallelism two one subtask is told
        // there are no more splits. The job finishing is what says that signal arrived.
        assertThat(run(sourceBuilder().build(), 2))
                .containsExactlyInAnyOrder("a", "b", "c", "d", "e");
    }

    @Test
    void aFetchCapSmallerThanTheTableStillReadsEveryRow() throws Exception {
        // The emulator answers the whole table in one response block, so a cap of two proves the
        // cursor resumes inside a block rather than per response.
        assertThat(run(sourceBuilder().maxRecordsPerFetch(2).build(), 1))
                .containsExactlyInAnyOrder("a", "b", "c", "d", "e");
    }

    private static BigQuerySourceBuilder<GenericRecord> sourceBuilder() {
        return BigQuerySource.<GenericRecord>builder()
                .table(destination(TABLE))
                .deserializer(BigQueryRowDeserializer.genericRecord(READER_SCHEMA))
                .emulatorEndpoint(grpcEndpoint());
    }

    /** Runs the source as a job and returns the {@code name} column of every record it produced. */
    private static List<String> run(Source<GenericRecord, ?, ?> source, int parallelism)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        List<String> names = new ArrayList<>();
        try (org.apache.flink.util.CloseableIterator<GenericRecord> records =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "bigquery")
                        .executeAndCollect()) {
            Iterator<GenericRecord> iterator = records;
            while (iterator.hasNext()) {
                names.add(String.valueOf(iterator.next().get("name")));
            }
        }
        return names;
    }
}
