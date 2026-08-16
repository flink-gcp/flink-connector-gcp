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

package io.github.flink.gcp.connector.pubsub.source.serializer;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.util.Collector;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PubSubDeserializationSchema#dataOnly}. */
class DataOnlyDeserializationSchemaTest {

    @Test
    void deserializesThePayloadAndIgnoresEveryOtherMessageField() throws IOException {
        PubSubDeserializationSchema<String> schema =
                PubSubDeserializationSchema.dataOnly(new SimpleStringSchema());
        PubsubMessage message =
                PubsubMessage.newBuilder()
                        .setData(ByteString.copyFrom("payload", StandardCharsets.UTF_8))
                        .setOrderingKey("key")
                        .putAttributes("attribute", "value")
                        .build();

        List<String> collected = collect(schema, message);

        assertThat(collected).containsExactly("payload");
        assertThat(schema.getProducedType()).isEqualTo(Types.STRING);
    }

    @Test
    void dropsARecordThePayloadSchemaDeserializesToNull() throws IOException {
        PubSubDeserializationSchema<String> schema =
                PubSubDeserializationSchema.dataOnly(new NullDeserializationSchema());

        assertThat(collect(schema, PubsubMessage.getDefaultInstance())).isEmpty();
    }

    @Test
    void opensTheWrappedPayloadSchema() throws Exception {
        RecordingDeserializationSchema wrapped = new RecordingDeserializationSchema();

        PubSubDeserializationSchema.dataOnly(wrapped).open(null);

        assertThat(wrapped.opened).isTrue();
    }

    private static List<String> collect(
            PubSubDeserializationSchema<String> schema, PubsubMessage message) throws IOException {
        List<String> collected = new ArrayList<>();
        schema.deserialize(
                message,
                SubscriptionDestination.of("p", "s"),
                new Collector<String>() {
                    @Override
                    public void collect(String record) {
                        collected.add(record);
                    }

                    @Override
                    public void close() {}
                });
        return Collections.unmodifiableList(collected);
    }

    /** Deserializes everything to {@code null}, which the collector overload drops. */
    private static final class NullDeserializationSchema implements DeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public String deserialize(byte[] message) {
            return null;
        }

        @Override
        public boolean isEndOfStream(String nextElement) {
            return false;
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }

    /** Records whether {@code open} reached the wrapped schema. */
    private static final class RecordingDeserializationSchema
            implements DeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        private boolean opened;

        @Override
        public void open(InitializationContext context) {
            opened = true;
        }

        @Override
        public String deserialize(byte[] message) {
            return new String(message, StandardCharsets.UTF_8);
        }

        @Override
        public boolean isEndOfStream(String nextElement) {
            return false;
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }
}
