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

package io.github.flink.gcp.connector.bigquery.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import io.github.flink.gcp.connector.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.bigquery.source.enumerator.ReadClientSessionCreator;
import io.github.flink.gcp.connector.bigquery.source.enumerator.ReadSessionCreator;
import io.github.flink.gcp.connector.bigquery.source.reader.ReadClientRowStreamOpener;
import io.github.flink.gcp.connector.bigquery.source.reader.RowStream;
import io.github.flink.gcp.connector.bigquery.source.reader.RowStreamOpener;
import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializer;
import io.github.flink.gcp.connector.testutils.TestNames;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The source against BigQuery itself.
 *
 * <p>Deliberately narrow: it covers what the emulator cannot stand in for, because the emulator
 * ignores {@code ReadRowsRequest.offset} and answers every call from row zero. Everything a fake or
 * the emulator can hold is held there instead — this suite costs money and a run.
 *
 * <p>Two of the three cases are measurements of the service the design rests on, kept as tests so a
 * change in either shows up here rather than as a data-loss report: that a resumed read continues
 * where it left off, and that a read at exactly the stream's row count is an empty stream rather
 * than the error the proto's "requesting a larger offset is undefined" would allow.
 *
 * <p>Multiple streams are out of scope: BigQuery decides the stream count from the table's size,
 * and a table large enough to be split costs more than the assignment logic is worth here — that is
 * covered by the enumerator's unit tests.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@Timeout(300)
class BigQuerySourceRealGcpITCase {

    private static final String TABLE = TestNames.unique("source_read");
    private static final int ROWS = 20;

    private static final String READER_SCHEMA =
            "{\"type\":\"record\",\"name\":\"Row\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"long\"}]}";

    @BeforeAll
    static void seed() throws Exception {
        RealBigQuery.createTable(
                TABLE,
                Schema.of(
                        Field.newBuilder("id", StandardSQLTypeName.INT64)
                                .setMode(Field.Mode.REQUIRED)
                                .build()));
        String values =
                IntStream.range(0, ROWS)
                        .mapToObj(id -> "(" + id + ")")
                        .collect(Collectors.joining(", "));
        RealBigQuery.queryRows(
                "INSERT INTO " + RealBigQuery.tablePath(TABLE) + " (id) VALUES " + values);
    }

    @AfterAll
    static void cleanUp() {
        RealBigQuery.deleteTables(TABLE);
    }

    @Test
    void readsEveryRowThroughAJob() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        List<Long> ids = new ArrayList<>();
        try (CloseableIterator<GenericRecord> records =
                env.fromSource(
                                BigQuerySource.<GenericRecord>builder()
                                        .table(RealBigQuery.destination(TABLE))
                                        .deserializer(
                                                BigQueryRowDeserializer.genericRecord(
                                                        READER_SCHEMA))
                                        .build(),
                                WatermarkStrategy.noWatermarks(),
                                "bigquery")
                        .executeAndCollect()) {
            records.forEachRemaining(row -> ids.add((Long) row.get("id")));
        }

        assertThat(ids)
                .containsExactlyInAnyOrderElementsOf(
                        IntStream.range(0, ROWS)
                                .mapToObj(Long::valueOf)
                                .collect(Collectors.toList()));
    }

    @Test
    void resumesAStreamAtTheRequestedOffset() throws Exception {
        ReadSession session = createSession();

        try (RowStreamOpener opener =
                new ReadClientRowStreamOpener(
                        null, BigQuerySourceBuilder.DEFAULT_RETRY_MAX_ATTEMPTS)) {
            List<Long> whole = read(opener, session, 0);
            assertThat(whole).hasSize(ROWS);

            // The mechanism the whole design rests on: everything from the offset on, once each,
            // in the same order. Compared against the stream's own order rather than against the
            // ids, because a stream's rows arrive in BigQuery's storage order and not the table's
            // (measured 2026-08-09: a read at offset 7 returned 13 rows whose ids were neither
            // sorted nor the ids 7..19).
            assertThat(read(opener, session, 7)).containsExactlyElementsOf(whole.subList(7, ROWS));
        }
    }

    @Test
    void answersAReadAtTheStreamsRowCountWithAnEmptyStream() throws Exception {
        // The proto says requesting an offset past the last row read is undefined, and a checkpoint
        // can land on exactly the row count. Measured 2026-08-09: BigQuery ends the stream with no
        // rows and no error, which is what the reader treats as a finished split.
        ReadSession session = createSession();

        try (RowStreamOpener opener =
                new ReadClientRowStreamOpener(
                        null, BigQuerySourceBuilder.DEFAULT_RETRY_MAX_ATTEMPTS)) {
            assertThat(read(opener, session, 0)).hasSize(ROWS);
            assertThat(read(opener, session, ROWS)).isEmpty();
        }
    }

    private static ReadSession createSession() throws Exception {
        try (ReadSessionCreator creator = new ReadClientSessionCreator(null)) {
            return creator.create(
                    CreateReadSessionRequest.newBuilder()
                            .setParent("projects/" + RealBigQuery.project())
                            .setMaxStreamCount(1)
                            .setReadSession(
                                    ReadSession.newBuilder()
                                            .setTable(RealBigQuery.destination(TABLE).toTablePath())
                                            .setDataFormat(DataFormat.AVRO)
                                            .build())
                            .build());
        }
    }

    private static List<Long> read(RowStreamOpener opener, ReadSession session, long offset)
            throws Exception {
        String stream = session.getStreams(0).getName();
        List<Long> ids = new ArrayList<>();
        // Decoded with the session's own schema, as the connector does: the reader schema below is
        // what a job declares, and the two need not be identical.
        org.apache.avro.Schema writerSchema =
                new org.apache.avro.Schema.Parser().parse(session.getAvroSchema().getSchema());
        org.apache.avro.Schema readerSchema =
                new org.apache.avro.Schema.Parser().parse(READER_SCHEMA);
        GenericDatumReader<GenericRecord> reader =
                new GenericDatumReader<>(writerSchema, readerSchema);
        BinaryDecoder decoder = null;
        try (RowStream rows = opener.open(stream, offset)) {
            ReadRowsResponse response;
            while ((response = rows.next()) != null) {
                decoder =
                        DecoderFactory.get()
                                .binaryDecoder(
                                        response.getAvroRows().getSerializedBinaryRows().newInput(),
                                        decoder);
                for (long row = 0; row < response.getRowCount(); row++) {
                    ids.add((Long) reader.read(null, decoder).get("id"));
                }
            }
        }
        return ids;
    }
}
