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

package io.github.flink.gcp.connector.bigquery.sink.serializer.custom;

import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalField;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFieldNullPolicy;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFieldType;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFields;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializationSchema;
import io.github.flink.gcp.connector.bigquery.sink.serializer.ProtoRowMessageProvider;
import io.github.flink.gcp.connector.bigquery.sink.serializer.SerializedRow;
import io.github.flink.gcp.connector.bigquery.sink.serializer.proto.ProtoMessageSerializationSchema;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A serializer defined outside the connector's serializer package takes the message path by
 * implementing {@link ProtoRowMessageProvider} (#2047): here, one that routes each element to a
 * built-in serializer by its message type and forwards both {@code serialize} and {@code
 * serializeToMessage} to it.
 *
 * <p>These cases show the public contract working end to end for such a wrapper. Whether a
 * handed-over message is walked before a tail follows it is held by {@code SerializedRowTest} and
 * {@code ProtoRowAugmentingSerializerTest}: the well-known types routed here never carry an
 * appended number, so walking and trusting their messages produce the same row.
 */
class RoutingMessageSerializerTest {

    private static final TableDestination TIMESTAMPS =
            TableDestination.of("project", "dataset", "timestamps");
    private static final TableDestination DURATIONS =
            TableDestination.of("project", "dataset", "durations");

    @Test
    void handsTheDelegatesMessageOver() throws Exception {
        RoutingSerializer serializer = new RoutingSerializer(true);
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(12L).build();
        Duration duration = Duration.newBuilder().setSeconds(34L).build();

        SerializedRow timestampRow = SerializedRow.serialize(serializer, timestamp);
        Message timestampMessage = serializer.last;
        SerializedRow durationRow = SerializedRow.serialize(serializer, duration);
        Message durationMessage = serializer.last;

        assertThat(timestampRow.getBase(serializer.getDescriptor(TIMESTAMPS)))
                .isSameAs(timestampMessage);
        assertThat(durationRow.getBase(serializer.getDescriptor(DURATIONS)))
                .isSameAs(durationMessage);
        assertThat(timestampRow.toByteString()).isEqualTo(serializer.serialize(timestamp));
        assertThat(durationRow.toByteString()).isEqualTo(serializer.serialize(duration));
    }

    @Test
    void handsTheDelegatesMessageOverBeneathAdditionalFields() throws Exception {
        RoutingSerializer serializer = new RoutingSerializer(true);
        BigQuerySinkConfig<Message> config = config(serializer);
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(12L).build();

        SerializedRow row = config.serializeRow(timestamp, TIMESTAMPS);

        assertThat(row.getBase(serializer.getDescriptor(TIMESTAMPS))).isSameAs(serializer.last);
        assertAdditionalField(config, row, 12L);
    }

    /**
     * A wrapper that leaves {@link BigQueryProtoSerializationSchema#getDescriptor} to its default
     * derives another descriptor instance on every call, so its delegate's message no longer has
     * the descriptor the sink holds: the row still reads as the delegate's row, from its bytes.
     */
    @Test
    void fallsBackToTheMessagesBytesForAnotherDescriptorInstance() throws Exception {
        RoutingSerializer serializer = new RoutingSerializer(false);
        BigQuerySinkConfig<Message> config = config(serializer);
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(56L).build();
        Descriptors.Descriptor held = serializer.getDescriptor(TIMESTAMPS);

        SerializedRow plain = SerializedRow.serialize(serializer, timestamp);
        Message base = plain.getBase(held);

        assertThat(base).isNotSameAs(serializer.last);
        assertThat(base.getDescriptorForType()).isSameAs(held);
        assertThat(base).isEqualTo(DynamicMessage.parseFrom(held, serializer.serialize(timestamp)));
        assertAdditionalField(config, config.serializeRow(timestamp, TIMESTAMPS), 56L);
    }

    private static BigQuerySinkConfig<Message> config(RoutingSerializer serializer) {
        @SuppressWarnings("unchecked")
        BigQueryDefaultStreamSink<Message> sink =
                (BigQueryDefaultStreamSink<Message>)
                        BigQuerySink.<Message>builder()
                                .table(TIMESTAMPS)
                                .serializer(serializer)
                                .additionalFields(
                                        AdditionalFields.<Message>builder()
                                                .field(
                                                        AdditionalField.of(
                                                                "__type",
                                                                AdditionalFieldType.STRING,
                                                                AdditionalFieldNullPolicy.REQUIRED,
                                                                record ->
                                                                        record.getDescriptorForType()
                                                                                .getFullName()))
                                                .build())
                                .build();
        return sink.getConfig();
    }

    private static void assertAdditionalField(
            BigQuerySinkConfig<Message> config, SerializedRow row, long seconds)
            throws IOException {
        Descriptors.Descriptor descriptor = config.getWriteDescriptor(TIMESTAMPS);
        DynamicMessage decoded = DynamicMessage.parseFrom(descriptor, row.toByteString());

        assertThat(decoded.getField(descriptor.findFieldByName("seconds"))).isEqualTo(seconds);
        assertThat(decoded.getField(descriptor.findFieldByName("__type")))
                .isEqualTo("google.protobuf.Timestamp");
        assertThat(row.getSerializedSize()).isEqualTo(row.toByteString().size());
    }

    /**
     * Routes each message to the built-in serializer of its type, and each destination to the
     * serializer of the type written there.
     */
    private static final class RoutingSerializer extends BigQueryProtoSerializationSchema<Message>
            implements ProtoRowMessageProvider<Message> {

        private static final long serialVersionUID = 1L;

        private final ProtoMessageSerializationSchema<Timestamp> timestamps =
                ProtoMessageSerializationSchema.of(Timestamp.class);
        private final ProtoMessageSerializationSchema<Duration> durations =
                ProtoMessageSerializationSchema.of(Duration.class);
        private final boolean forwardDescriptor;
        private transient Message last;

        private RoutingSerializer(boolean forwardDescriptor) {
            this.forwardDescriptor = forwardDescriptor;
        }

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return delegate(destination).getTableSchema(destination);
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            return forwardDescriptor
                    ? delegate(destination).getDescriptor(destination)
                    : super.getDescriptor(destination);
        }

        @Override
        @Nullable
        public ByteString serialize(Message element) throws IOException {
            if (element instanceof Timestamp) {
                return timestamps.serialize((Timestamp) element);
            }
            return durations.serialize((Duration) element);
        }

        @Override
        @Nullable
        public Message serializeToMessage(Message element) throws IOException {
            last =
                    element instanceof Timestamp
                            ? timestamps.serializeToMessage((Timestamp) element)
                            : durations.serializeToMessage((Duration) element);
            return last;
        }

        private BigQueryProtoSerializationSchema<?> delegate(TableDestination destination) {
            return destination.equals(TIMESTAMPS) ? timestamps : durations;
        }
    }
}
