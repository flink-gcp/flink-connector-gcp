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

package io.github.flink.gcp.connector.pubsub.table.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import com.google.pubsub.v1.PubsubMessage;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** Tests for {@link RowDataSerializationSchema}. */
class RowDataSerializationSchemaTest {

    /**
     * Records the rows it is handed, so the projection handed to the format can be inspected.
     *
     * <p>Renders any field type rather than assuming {@code STRING}: a schema that mistakenly hands
     * over the metadata suffix must fail on the assertion about what the format saw, not on a class
     * cast inside this encoder — a cast error would kill the same mutants for the wrong reason.
     */
    private static final class RecordingEncoder implements SerializationSchema<RowData> {

        private static final long serialVersionUID = 1L;

        private final transient List<String> seen = new ArrayList<>();

        @Override
        public byte[] serialize(RowData element) {
            StringBuilder rendered = new StringBuilder();
            for (int i = 0; i < element.getArity(); i++) {
                rendered.append(i == 0 ? "" : "|");
                rendered.append(render(element, i));
            }
            seen.add(rendered.toString());
            return rendered.toString().getBytes(StandardCharsets.UTF_8);
        }

        private static String render(RowData element, int pos) {
            if (element.isNullAt(pos)) {
                return "null";
            }
            try {
                return element.getString(pos).toString();
            } catch (ClassCastException e) {
                return "<non-string>";
            }
        }
    }

    private static GenericRowData row(Object... fields) {
        GenericRowData row = new GenericRowData(fields.length);
        for (int i = 0; i < fields.length; i++) {
            row.setField(i, fields[i]);
        }
        return row;
    }

    private static GenericMapData attributes(Map<String, String> entries) {
        Map<StringData, StringData> data = new HashMap<>();
        entries.forEach((k, v) -> data.put(str(k), str(v)));
        return new GenericMapData(data);
    }

    private static StringData str(String value) {
        return value == null ? null : StringData.fromString(value);
    }

    @Test
    void theFormatSeesOnlyThePhysicalPrefix() throws Exception {
        RecordingEncoder encoder = new RecordingEncoder();
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(
                        encoder,
                        2,
                        new WritableMetadata[] {
                            WritableMetadata.ATTRIBUTES, WritableMetadata.ORDERING_KEY
                        });

        PubsubMessage message =
                schema.serialize(
                        row(
                                str("a"),
                                str("b"),
                                attributes(java.util.Collections.singletonMap("k", "v")),
                                str("key-1")));

        assertThat(encoder.seen).containsExactly("a|b");
        assertThat(message.getData().toStringUtf8()).isEqualTo("a|b");
        assertThat(message.getAttributesMap()).containsExactly(entry("k", "v"));
        assertThat(message.getOrderingKey()).isEqualTo("key-1");
    }

    @Test
    void withoutMetadataTheRowReachesTheFormatUnprojected() throws Exception {
        RecordingEncoder encoder = new RecordingEncoder();
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(encoder, 2, new WritableMetadata[0]);

        PubsubMessage message = schema.serialize(row(str("a"), str("b")));

        assertThat(encoder.seen).containsExactly("a|b");
        assertThat(message.getAttributesMap()).isEmpty();
        assertThat(message.getOrderingKey()).isEmpty();
    }

    @Test
    void nullMetadataColumnsAreNoOps() throws Exception {
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(
                        new RecordingEncoder(),
                        1,
                        new WritableMetadata[] {
                            WritableMetadata.ATTRIBUTES, WritableMetadata.ORDERING_KEY
                        });

        PubsubMessage message = schema.serialize(row(str("a"), null, null));

        assertThat(message.getAttributesMap()).isEmpty();
        assertThat(message.getOrderingKey()).isEmpty();
    }

    // There is deliberately no test for an *empty* ordering key. proto3 gives a singular string no
    // presence, so setOrderingKey("") builds a message equal to one where it was never set — the
    // guard in WritableMetadata is documentation, and any test of it would be unfalsifiable.

    @Test
    void anEmptyAttributesMapAddsNothing() throws Exception {
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(
                        new RecordingEncoder(),
                        1,
                        new WritableMetadata[] {WritableMetadata.ATTRIBUTES});

        assertThat(schema.serialize(row(str("a"), attributes(new HashMap<>()))).getAttributesMap())
                .isEmpty();
    }

    @Test
    void severalAttributesAreAllWritten() throws Exception {
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(
                        new RecordingEncoder(),
                        1,
                        new WritableMetadata[] {WritableMetadata.ATTRIBUTES});
        Map<String, String> entries = new HashMap<>();
        entries.put("one", "1");
        entries.put("two", "2");

        assertThat(schema.serialize(row(str("a"), attributes(entries))).getAttributesMap())
                .containsOnly(entry("one", "1"), entry("two", "2"));
    }

    @Test
    void anAttributeWithANullKeyFailsTheWrite() {
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(
                        new RecordingEncoder(),
                        1,
                        new WritableMetadata[] {WritableMetadata.ATTRIBUTES});
        Map<StringData, StringData> entries = new HashMap<>();
        entries.put(null, str("v"));

        assertThatThrownBy(() -> schema.serialize(row(str("a"), new GenericMapData(entries))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("null key");
    }

    @Test
    void aNullFromTheFormatFailsTheWriteRatherThanSkippingTheRow() {
        // The DataStream serializers read null as "skip the record"; a format's null is not that.
        // Flink's SerializationSchema contract has no null in it, and SQL has no way to ask for a
        // skip, so the row is reported as a serialization failure naming the format.
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(
                        new NullReturningEncoder(), 1, new WritableMetadata[0]);

        assertThatThrownBy(() -> schema.serialize(row(str("a"))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(NullReturningEncoder.class.getName())
                .hasMessageContaining("returned null");
    }

    @Test
    void anAttributeWithANullValueFailsTheWrite() {
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(
                        new RecordingEncoder(),
                        1,
                        new WritableMetadata[] {WritableMetadata.ATTRIBUTES});
        Map<StringData, StringData> entries = new HashMap<>();
        entries.put(str("k"), null);

        assertThatThrownBy(() -> schema.serialize(row(str("a"), new GenericMapData(entries))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("null value");
    }

    @Test
    void metadataIsReadFromThePositionItsOrderGivesIt() throws Exception {
        // Only the ordering key is selected, so it sits directly after the physical columns —
        // reading it from ATTRIBUTES' declaration position instead would take the wrong column.
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(
                        new RecordingEncoder(),
                        2,
                        new WritableMetadata[] {WritableMetadata.ORDERING_KEY});

        assertThat(schema.serialize(row(str("a"), str("b"), str("key-1"))).getOrderingKey())
                .isEqualTo("key-1");
    }

    @Test
    void aReusedProjectionShowsEachRecordRatherThanTheFirst() throws Exception {
        RecordingEncoder encoder = new RecordingEncoder();
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(
                        encoder, 1, new WritableMetadata[] {WritableMetadata.ORDERING_KEY});

        assertThat(schema.serialize(row(str("a"), str("k1"))).getData().toStringUtf8())
                .isEqualTo("a");
        assertThat(schema.serialize(row(str("b"), str("k2"))).getData().toStringUtf8())
                .isEqualTo("b");
        assertThat(encoder.seen).containsExactly("a", "b");
    }

    /** A format breaking Flink's contract by returning no bytes. */
    private static final class NullReturningEncoder implements SerializationSchema<RowData> {

        private static final long serialVersionUID = 1L;

        @Override
        public byte[] serialize(RowData element) {
            return null;
        }
    }
}
