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

package io.github.flink.gcp.connector.pubsub.sink.serializer;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import com.google.pubsub.v1.PubsubMessage;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link PubSubSerializationSchema#payload(SerializationSchema)} adapter. */
class PayloadSerializationSchemaTest {

    @Test
    void producesPayloadOnlyMessages() throws IOException {
        PubSubSerializationSchema<String> schema =
                PubSubSerializationSchema.payload(new SimpleStringSchema());

        PubsubMessage message = schema.serialize("hello");

        assertThat(message.getData().toString(StandardCharsets.UTF_8)).isEqualTo("hello");
        assertThat(message.getAttributesMap()).isEmpty();
        assertThat(message.getOrderingKey()).isEmpty();
    }

    @Test
    void reportsANullPayloadAsAFailureRatherThanASkip() {
        // Flink's SerializationSchema contract has no null in it, so a null payload is a broken
        // format, not the sink's skip convention — reading it as a skip would silently drop every
        // record such a format failed on.
        PubSubSerializationSchema<String> schema =
                PubSubSerializationSchema.payload(new NullReturningSchema());

        assertThatThrownBy(() -> schema.serialize("hello"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(NullReturningSchema.class.getName())
                .hasMessageContaining("returned null");
    }

    @Test
    void openForwardsToTheWrappedSchema() throws Exception {
        OpenRecordingSchema wrapped = new OpenRecordingSchema();
        PubSubSerializationSchema<String> schema = PubSubSerializationSchema.payload(wrapped);

        schema.open(null);

        assertThat(wrapped.openCalls).isEqualTo(1);
    }

    /** A payload schema recording {@code open} invocations. */
    private static final class OpenRecordingSchema implements SerializationSchema<String> {

        private static final long serialVersionUID = 1L;

        int openCalls;

        @Override
        public void open(InitializationContext context) {
            openCalls++;
        }

        @Override
        public byte[] serialize(String element) {
            return element.getBytes(StandardCharsets.UTF_8);
        }
    }

    /** A payload schema breaking Flink's contract by returning no bytes. */
    private static final class NullReturningSchema implements SerializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public byte[] serialize(String element) {
            return null;
        }
    }
}
