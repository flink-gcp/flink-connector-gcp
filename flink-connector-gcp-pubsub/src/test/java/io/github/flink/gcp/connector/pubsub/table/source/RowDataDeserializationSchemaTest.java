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

package io.github.flink.gcp.connector.pubsub.table.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RowDataDeserializationSchema}. */
class RowDataDeserializationSchemaTest {

    private static final SubscriptionDestination SUBSCRIPTION =
            SubscriptionDestination.of("my-project", "my-sub");

    private static final TypeInformation<RowData> PRODUCED_TYPE =
            InternalTypeInfo.of(RowType.of(new VarCharType(VarCharType.MAX_LENGTH)));

    /** Splits the payload on ',' and emits one row per part, so a message can produce 0..n rows. */
    private static class SplittingDecoder implements DeserializationSchema<RowData> {

        private static final long serialVersionUID = 1L;

        private final RowKind rowKind;

        private SplittingDecoder(RowKind rowKind) {
            this.rowKind = rowKind;
        }

        @Override
        public void deserialize(byte[] message, Collector<RowData> out) {
            String payload = new String(message, StandardCharsets.UTF_8);
            if (payload.isEmpty()) {
                return;
            }
            for (String part : payload.split(",")) {
                GenericRowData row = new GenericRowData(1);
                row.setRowKind(rowKind);
                row.setField(0, StringData.fromString(part));
                out.collect(row);
            }
        }

        @Override
        public RowData deserialize(byte[] message) {
            throw new UnsupportedOperationException("the collector overload is the one used");
        }

        @Override
        public boolean isEndOfStream(RowData nextElement) {
            return false;
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            return PRODUCED_TYPE;
        }
    }

    private static PubsubMessage message(String payload) {
        return PubsubMessage.newBuilder()
                .setMessageId("msg-1")
                .setData(ByteString.copyFrom(payload, StandardCharsets.UTF_8))
                .setPublishTime(
                        Timestamp.newBuilder().setSeconds(1_700_000_000L).setNanos(123_456_789))
                .putAttributes("k", "v")
                .setOrderingKey("key-1")
                .build();
    }

    private static List<RowData> collect(RowDataDeserializationSchema schema, PubsubMessage message)
            throws Exception {
        List<RowData> rows = new ArrayList<>();
        schema.deserialize(
                message,
                SUBSCRIPTION,
                new Collector<RowData>() {
                    @Override
                    public void collect(RowData record) {
                        rows.add(record);
                    }

                    @Override
                    public void close() {}
                });
        return rows;
    }

    private static RowDataDeserializationSchema schema(ReadableMetadata... metadata) {
        return new RowDataDeserializationSchema(
                new SplittingDecoder(RowKind.INSERT), metadata, PRODUCED_TYPE);
    }

    @Test
    void metadataCrossesTheJobGraphAsEnumConstantsRatherThanConverters() throws Exception {
        // Each ReadableMetadata constant holds a converter lambda, and this schema travels in the
        // job graph. What keeps those lambdas out of the bytes is that the schema holds the
        // constants themselves: an enum serializes as its own name, while a lambda would be
        // rebound by a synthetic-method name the compiler picks — and lambdas sharing an enclosing
        // declaration and a descriptor share one name hash, leaving only a trailing index between
        // them, so adding a converter would silently rebind a restored one to another column's.
        //
        // Asserted through the constant names rather than through the absence of
        // "SerializedLambda": the produced TypeInformation carries Flink's own RowData field
        // getters, which are lambdas this project neither mints nor controls. Hoisting the
        // converters into a MetadataConverter[] field would drop these names.
        RowDataDeserializationSchema schema =
                schema(ReadableMetadata.MESSAGE_ID, ReadableMetadata.ATTRIBUTES);
        PubsubMessage message = message("a");

        byte[] serialized = InstantiationUtil.serializeObject(schema);

        assertThat(new String(serialized, StandardCharsets.ISO_8859_1))
                .contains("MESSAGE_ID")
                .contains("ATTRIBUTES");
        RowDataDeserializationSchema restored =
                InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
        assertThat(collect(restored, message)).isEqualTo(collect(schema, message));
    }

    @Test
    void appendsEveryMetadataColumnAfterThePhysicalOnes() throws Exception {
        List<RowData> rows =
                collect(
                        schema(
                                ReadableMetadata.MESSAGE_ID,
                                ReadableMetadata.PUBLISH_TIME,
                                ReadableMetadata.ATTRIBUTES,
                                ReadableMetadata.ORDERING_KEY,
                                ReadableMetadata.SUBSCRIPTION),
                        message("a"));

        assertThat(rows).hasSize(1);
        RowData row = rows.get(0);
        assertThat(row.getArity()).isEqualTo(6);
        assertThat(row.getString(0)).hasToString("a");
        assertThat(row.getString(1)).hasToString("msg-1");
        assertThat(row.getTimestamp(2, 3))
                .isEqualTo(TimestampData.fromEpochMillis(1_700_000_000_123L));
        MapData attributes = row.getMap(3);
        assertThat(attributes.keyArray().getString(0)).hasToString("k");
        assertThat(attributes.valueArray().getString(0)).hasToString("v");
        assertThat(row.getString(4)).hasToString("key-1");
        assertThat(row.getString(5)).hasToString("projects/my-project/subscriptions/my-sub");
    }

    @Test
    void metadataFollowsTheSelectionOrderRatherThanTheDeclarationOrder() throws Exception {
        List<RowData> rows =
                collect(
                        schema(ReadableMetadata.SUBSCRIPTION, ReadableMetadata.MESSAGE_ID),
                        message("a"));

        assertThat(rows.get(0).getString(1))
                .hasToString("projects/my-project/subscriptions/my-sub");
        assertThat(rows.get(0).getString(2)).hasToString("msg-1");
    }

    @Test
    void withoutMetadataTheRowsAreWhateverTheFormatEmitted() throws Exception {
        List<RowData> rows = collect(schema(), message("a,b"));

        assertThat(rows).hasSize(2);
        assertThat(rows).allSatisfy(row -> assertThat(row.getArity()).isEqualTo(1));
    }

    @Test
    void everyRowOfAMultiRowMessageCarriesTheSameMetadata() throws Exception {
        List<RowData> rows = collect(schema(ReadableMetadata.MESSAGE_ID), message("a,b,c"));

        assertThat(rows).hasSize(3);
        assertThat(rows).extracting(r -> r.getString(0).toString()).containsExactly("a", "b", "c");
        assertThat(rows).allSatisfy(row -> assertThat(row.getString(1)).hasToString("msg-1"));
    }

    @Test
    void aMessageTheFormatEmitsNothingForProducesNoRows() throws Exception {
        assertThat(collect(schema(ReadableMetadata.MESSAGE_ID), message(""))).isEmpty();
    }

    @Test
    void rejectsANullRowFromAFormatBeforeAppendingMetadata() {
        DeserializationSchema<RowData> physical =
                new SplittingDecoder(RowKind.INSERT) {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void deserialize(byte[] message, Collector<RowData> out) {
                        out.collect(null);
                    }
                };
        RowDataDeserializationSchema schema =
                new RowDataDeserializationSchema(
                        physical,
                        new ReadableMetadata[] {ReadableMetadata.MESSAGE_ID},
                        PRODUCED_TYPE);

        assertThatThrownBy(() -> collect(schema, message("payload")))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("source deserializer must not collect null")
                .hasMessageContaining("collecting no records");
    }

    @Test
    void refusesAMetadataCollectorRetainedIntoTheNextMessage() throws Exception {
        AtomicReference<Collector<RowData>> retained = new AtomicReference<>();
        AtomicInteger calls = new AtomicInteger();
        DeserializationSchema<RowData> physical =
                new SplittingDecoder(RowKind.INSERT) {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void deserialize(byte[] message, Collector<RowData> out) {
                        if (calls.getAndIncrement() == 0) {
                            retained.set(out);
                        } else {
                            retained.get().collect(new GenericRowData(1));
                        }
                    }
                };
        RowDataDeserializationSchema schema =
                new RowDataDeserializationSchema(
                        physical,
                        new ReadableMetadata[] {ReadableMetadata.MESSAGE_ID},
                        PRODUCED_TYPE);
        collect(schema, message("first"));

        assertThatThrownBy(() -> collect(schema, message("second")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("only during its synchronous deserialize call");
    }

    @Test
    void theRowKindOfTheFormatSurvivesTheAppend() throws Exception {
        // The source delegates its changelog mode to the format, so a format emitting updates has
        // to keep emitting them through the metadata wrapper.
        RowDataDeserializationSchema schema =
                new RowDataDeserializationSchema(
                        new SplittingDecoder(RowKind.UPDATE_BEFORE),
                        new ReadableMetadata[] {ReadableMetadata.MESSAGE_ID},
                        PRODUCED_TYPE);

        assertThat(collect(schema, message("a")))
                .singleElement()
                .satisfies(row -> assertThat(row.getRowKind()).isEqualTo(RowKind.UPDATE_BEFORE));
    }

    @Test
    void anAbsentOrderingKeyBecomesNullRatherThanAnEmptyString() throws Exception {
        PubsubMessage withoutKey = message("a").toBuilder().clearOrderingKey().build();

        List<RowData> rows = collect(schema(ReadableMetadata.ORDERING_KEY), withoutKey);

        assertThat(rows.get(0).isNullAt(1)).isTrue();
    }

    @Test
    void aMessageWithoutAttributesGetsAnEmptyMapRatherThanNull() throws Exception {
        PubsubMessage withoutAttributes = message("a").toBuilder().clearAttributes().build();

        List<RowData> rows = collect(schema(ReadableMetadata.ATTRIBUTES), withoutAttributes);

        assertThat(rows.get(0).isNullAt(1)).isFalse();
        assertThat(rows.get(0).getMap(1).size()).isZero();
    }

    @Test
    void thePublishTimeIsTruncatedToMillisecondsRatherThanRounded() throws Exception {
        // 1_700_000_000.123456789 s -> 1_700_000_000_123 ms: the sub-millisecond remainder is
        // dropped, never carried into the next millisecond.
        List<RowData> rows = collect(schema(ReadableMetadata.PUBLISH_TIME), message("a"));

        assertThat(rows.get(0).getTimestamp(1, 3).getMillisecond()).isEqualTo(1_700_000_000_123L);
    }

    @Test
    void theProducedTypeIsTheOneItWasGiven() {
        assertThat(schema(ReadableMetadata.MESSAGE_ID).getProducedType()).isEqualTo(PRODUCED_TYPE);
    }

    @Test
    void theMetadataListedIsTheMetadataDeclared() {
        assertThat(ReadableMetadata.listAll().keySet())
                .containsExactlyElementsOf(
                        Arrays.asList(
                                "message-id",
                                "publish-time",
                                "attributes",
                                "ordering-key",
                                "subscription"));
    }
}
