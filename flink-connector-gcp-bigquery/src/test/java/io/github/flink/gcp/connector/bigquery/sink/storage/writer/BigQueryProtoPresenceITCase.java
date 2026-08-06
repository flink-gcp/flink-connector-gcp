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

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.proto.ProtoMessageSerializer;
import io.github.flink.gcp.connector.bigquery.sink.serializer.proto.ProtoSchemaOptions;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.tables.BigQueryTableAdmin;
import io.github.flink.gcp.connector.bigquery.testproto.Presence;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/**
 * Integration test for {@link ProtoMessageSerializer} with {@link
 * ProtoSchemaOptions.Builder#deriveRequiredColumns()} against the BigQuery emulator
 * (goccy/bigquery-emulator): protobuf messages written through the {@link BigQuerySink} facade into
 * a table created from the serializer's own derived schema.
 *
 * <p>What this adds over the unit tests is the half they cannot reach — that a table can actually
 * be created with the {@code REQUIRED} columns the option derives, and that the values then read
 * back the way presence says they should. The client-side half (a {@code REQUIRED} column becomes a
 * {@code LABEL_REQUIRED} field the row builder enforces) is pinned in {@code
 * ProtoRowConverterTest}.
 *
 * <p>The distinction under test is the whole point of the option: a field <b>without</b> presence
 * reads back as its protobuf default and never as NULL, while {@code optional} and the unselected
 * {@code oneof} branch read back as NULL.
 *
 * <p>Only one flush happens here, for the emulator reason recorded on {@link
 * BigQueryDefaultStreamWriterITCase}: on a connection opened after an earlier one has closed, only
 * the first {@code AppendRows} request is durably applied.
 */
class BigQueryProtoPresenceITCase extends AbstractBigQueryEmulatorITCase {

    @Test
    void writesProtobufMessagesWithPresenceDerivedModes() throws Exception {
        ProtoMessageSerializer<Presence> serializer =
                ProtoMessageSerializer.of(
                        Presence.class,
                        ProtoSchemaOptions.builder().deriveRequiredColumns().build());
        createTable("proto_presence", serializer.getTableSchema(null));

        BigQueryDefaultStreamSink<Presence> sink =
                (BigQueryDefaultStreamSink<Presence>)
                        BigQuerySink.<Presence>builder()
                                .destination(
                                        TableDestination.of(PROJECT, DATASET, "proto_presence"))
                                .serializer(serializer)
                                .build();
        SinkWriter<Presence> writer =
                sink.createWriter(
                        emulatorAppenderFactory(),
                        new BigQueryTableAdmin(restClient),
                        TestSinkWriterMetricGroup.create());
        try {
            writer.write(
                    Presence.newBuilder()
                            .setPImplicit("full")
                            .setPImplicitInt(1L)
                            .setPChoiceA("chosen")
                            .setPOptional("present")
                            .addPRep("a")
                            .addPRep("b")
                            .build(),
                    CONTEXT);
            // Everything else left unset: the presence-less fields must still arrive as "" and 0.
            writer.write(Presence.newBuilder().setPChoiceB(7L).build(), CONTEXT);
            writer.flush(true);
        } finally {
            writer.close();
        }

        // Read the created table back. Without this the whole test would pass identically with the
        // option turned off: the value path does not consult it — only schema derivation does — so
        // the rows below are not evidence about modes at all.
        Schema created =
                restClient
                        .getTable(TableId.of(PROJECT, DATASET, "proto_presence"))
                        .getDefinition()
                        .getSchema();
        assertThat(created).isNotNull();
        assertThat(created.getFields())
                .extracting(Field::getName, Field::getMode)
                .contains(
                        tuple("p_implicit", Field.Mode.REQUIRED),
                        tuple("p_implicit_int", Field.Mode.REQUIRED),
                        tuple("p_optional", Field.Mode.NULLABLE),
                        tuple("p_choice_a", Field.Mode.NULLABLE),
                        tuple("p_nested", Field.Mode.NULLABLE),
                        tuple("p_rep", Field.Mode.REPEATED));
        // The recursion reaches the server too: a REQUIRED leaf inside a NULLABLE STRUCT.
        assertThat(created.getFields().get("p_nested").getSubFields())
                .extracting(Field::getName, Field::getMode)
                .contains(
                        tuple("c_implicit", Field.Mode.REQUIRED),
                        tuple("c_optional", Field.Mode.NULLABLE));

        assertThat(rows())
                .containsExactly(
                        // The sparse record: the presence-less columns carry protobuf defaults and
                        // not NULL, which is what makes REQUIRED honest for them.
                        "|0|null|7|null|0", "full|1|chosen|null|present|2");
    }

    /**
     * Returns one line per row, joining every column so a wrong conversion shows up.
     *
     * <p>Two emulator 0.8.1 deviations shape this query, both around an <em>empty</em> repeated
     * column, and neither about the schema under test. {@code ARRAY_TO_STRING} — which {@link
     * BigQueryAvroSerializerITCase} uses on a populated array — panics the emulator with a nil
     * pointer dereference, so the length is read instead; and {@code ARRAY_LENGTH} of an empty
     * array comes back NULL where BigQuery returns 0, so the {@code IFNULL} is there to make the
     * expected value the same on both. On real BigQuery an empty {@code REPEATED} column is an
     * empty array and never NULL, so the wrapper is a no-op there.
     */
    private static List<String> rows() throws InterruptedException {
        List<String> rows = new ArrayList<>();
        restClient
                .query(
                        QueryJobConfiguration.newBuilder(
                                        "SELECT p_implicit, p_implicit_int, p_choice_a, p_choice_b,"
                                                + " p_optional, IFNULL(ARRAY_LENGTH(p_rep), 0) FROM"
                                                + " `"
                                                + PROJECT
                                                + "."
                                                + DATASET
                                                + ".proto_presence` ORDER BY p_implicit")
                                .build())
                .iterateAll()
                .forEach((FieldValueList row) -> rows.add(join(row)));
        return rows;
    }

    private static String join(FieldValueList row) {
        List<String> cells = new ArrayList<>(row.size());
        for (FieldValue value : row) {
            cells.add(value.isNull() ? "null" : value.getStringValue());
        }
        return String.join("|", cells);
    }
}
