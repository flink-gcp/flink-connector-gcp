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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AvroRecordSerializer;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AvroSchemaOptions;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.tables.BigQueryTableAdmin;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for {@link AvroRecordSerializer} against the BigQuery emulator
 * (goccy/bigquery-emulator): Avro records written through the {@link BigQuerySink} facade into a
 * table created from the serializer's own derived schema.
 *
 * <p>Covers the column types the emulator round-trips faithfully — {@code REQUIRED}/{@code
 * NULLABLE} scalars, {@code TIMESTAMP}, {@code DATE}, {@code BYTES}, an Avro enum as {@code
 * STRING}, a {@code REPEATED} field, a nested {@code STRUCT}, an Avro map as {@code REPEATED
 * STRUCT<key, value>}, and a {@code JSON} column. {@code TIME}, {@code DATETIME} and {@code
 * NUMERIC} are deliberately excluded: emulator 0.8.1 implements neither the packed civil-time
 * encoding nor the decimal byte encoding, so it reads those columns back as unrelated values
 * whatever the connector writes. Their encodings are pinned by unit tests against {@code
 * CivilTimeEncoder} and {@code BigDecimalByteStringEncoder}, and belong to the real-GCP suite.
 *
 * <p>Only one flush happens here, for the emulator reason recorded on {@link
 * BigQueryDefaultStreamWriterITCase}: on a connection opened after an earlier one has closed, only
 * the first {@code AppendRows} request is durably applied.
 */
class BigQueryAvroSerializerITCase extends AbstractBigQueryEmulatorITCase {

    private static final String SCHEMA_JSON =
            "{\"type\":\"record\",\"name\":\"Event\",\"namespace\":\"it\",\"fields\":["
                    + "{\"name\":\"name\",\"type\":\"string\"},"
                    + "{\"name\":\"count\",\"type\":[\"null\",\"long\"]},"
                    + "{\"name\":\"seen_at\",\"type\":"
                    + "{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}},"
                    + "{\"name\":\"seen_on\",\"type\":"
                    + "{\"type\":\"int\",\"logicalType\":\"date\"}},"
                    + "{\"name\":\"blob\",\"type\":\"bytes\"},"
                    + "{\"name\":\"level\",\"type\":{\"type\":\"enum\",\"name\":\"Level\","
                    + "\"symbols\":[\"INFO\",\"WARN\"]}},"
                    + "{\"name\":\"payload\",\"type\":\"string\"},"
                    + "{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},"
                    + "{\"name\":\"labels\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},"
                    + "{\"name\":\"origin\",\"type\":{\"type\":\"record\",\"name\":\"Origin\","
                    + "\"fields\":[{\"name\":\"host\",\"type\":\"string\"}]}}]}";

    private static final Instant SEEN_AT = Instant.parse("2026-07-26T01:02:03.456789Z");

    /** 2026-07-26, as days since the epoch. */
    private static final int SEEN_ON = 20660;

    private static GenericRecord event(Schema schema, String name, Long count, String host) {
        GenericRecord origin = new GenericData.Record(schema.getField("origin").schema());
        origin.put("host", host);
        GenericRecord event = new GenericData.Record(schema);
        event.put("name", name);
        event.put("count", count);
        event.put("seen_at", SEEN_AT.getEpochSecond() * 1_000_000L + SEEN_AT.getNano() / 1_000L);
        event.put("seen_on", SEEN_ON);
        event.put("blob", ByteBuffer.wrap(name.getBytes(StandardCharsets.UTF_8)));
        event.put("level", new GenericData.EnumSymbol(schema.getField("level").schema(), "WARN"));
        event.put("payload", "{\"k\":1}");
        event.put("tags", Arrays.asList("a", "b"));
        event.put("labels", Collections.singletonMap("env", "prod"));
        event.put("origin", origin);
        return event;
    }

    @Test
    void writesAvroRecordsThroughTheFacade() throws Exception {
        Schema schema = new Schema.Parser().parse(SCHEMA_JSON);
        AvroRecordSerializer serializer =
                AvroRecordSerializer.of(
                        schema, AvroSchemaOptions.builder().jsonFieldPath("payload").build());
        createTable("avro_writes", serializer.getTableSchema(null));

        BigQueryDefaultStreamSink<GenericRecord> sink =
                (BigQueryDefaultStreamSink<GenericRecord>)
                        BigQuerySink.<GenericRecord>builder()
                                .destination(TableDestination.of(PROJECT, DATASET, "avro_writes"))
                                .serializer(serializer)
                                .build();
        SinkWriter<GenericRecord> writer =
                sink.createWriter(
                        new EmulatorAppenderFactory(grpcEndpoint()),
                        new BigQueryTableAdmin(restClient));
        try {
            writer.write(event(schema, "alice", 3L, "host-a"), CONTEXT);
            writer.write(event(schema, "bob", null, "host-b"), CONTEXT);
            writer.flush(true);
        } finally {
            writer.close();
        }

        assertThat(rows())
                .containsExactly(
                        "alice|3|"
                                + SEEN_AT
                                + "|2026-07-26|alice|WARN|{\"k\":1}|a,b|env=prod|host-a",
                        "bob|null|"
                                + SEEN_AT
                                + "|2026-07-26|bob|WARN|{\"k\":1}|a,b|env=prod|host-b");
    }

    /** Returns one line per row, joining every column so a wrong conversion shows up. */
    private static List<String> rows() throws InterruptedException {
        List<String> rows = new ArrayList<>();
        restClient
                .query(
                        QueryJobConfiguration.newBuilder(
                                        "SELECT name, count, seen_at, seen_on, blob, level,"
                                                + " payload, ARRAY_TO_STRING(tags, ','),"
                                                + " (SELECT STRING_AGG(CONCAT(l.key, '=', l.value),"
                                                + " ',') FROM UNNEST(labels) AS l),"
                                                + " origin.host FROM `"
                                                + PROJECT
                                                + "."
                                                + DATASET
                                                + ".avro_writes` ORDER BY name")
                                .build())
                .iterateAll()
                .forEach((FieldValueList row) -> rows.add(join(row)));
        return rows;
    }

    private static String join(FieldValueList row) {
        StringBuilder line = new StringBuilder();
        line.append(row.get(0).getStringValue())
                .append('|')
                .append(row.get(1).isNull() ? "null" : row.get(1).getLongValue())
                .append('|')
                .append(row.get(2).getTimestampInstant())
                .append('|')
                .append(row.get(3).getStringValue())
                .append('|')
                .append(new String(row.get(4).getBytesValue(), StandardCharsets.UTF_8));
        for (FieldValue value : row.subList(5, row.size())) {
            line.append('|').append(value.getStringValue());
        }
        return line.toString();
    }
}
