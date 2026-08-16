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
import org.apache.flink.util.InstantiationUtil;

import com.google.pubsub.v1.PubsubMessage;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the {@link PubSubSerializationSchema#withAttributes} / {@link
 * PubSubSerializationSchema#withOrderingKey} composition.
 */
class MetadataSerializationSchemaTest {

    private static PubSubSerializationSchema<String> dataOnly() {
        return PubSubSerializationSchema.dataOnly(new SimpleStringSchema());
    }

    @Test
    void addsExtractedAttributes() throws Exception {
        PubSubSerializationSchema<String> schema =
                dataOnly().withAttributes(element -> Collections.singletonMap("source", element));

        PubsubMessage message = schema.serialize("hello");

        assertThat(message.getData().toString(StandardCharsets.UTF_8)).isEqualTo("hello");
        assertThat(message.getAttributesMap()).containsExactly(Map.entry("source", "hello"));
        assertThat(message.getOrderingKey()).isEmpty();
    }

    @Test
    void setsExtractedOrderingKey() throws Exception {
        PubSubSerializationSchema<String> schema =
                dataOnly().withOrderingKey(element -> "key-" + element);

        PubsubMessage message = schema.serialize("hello");

        assertThat(message.getOrderingKey()).isEqualTo("key-hello");
        assertThat(message.getAttributesMap()).isEmpty();
    }

    @Test
    void nullOrEmptyExtractionsAddNothing() throws Exception {
        PubSubSerializationSchema<String> schema =
                dataOnly()
                        .withAttributes(element -> null)
                        .withOrderingKey(element -> "")
                        .withAttributes(element -> Collections.emptyMap())
                        .withOrderingKey(element -> null);

        PubsubMessage message = schema.serialize("hello");

        assertThat(message.getAttributesMap()).isEmpty();
        assertThat(message.getOrderingKey()).isEmpty();
        assertThat(message.getData().toString(StandardCharsets.UTF_8)).isEqualTo("hello");
    }

    @Test
    void outerLayerWinsOnChaining() throws Exception {
        PubSubSerializationSchema<String> schema =
                dataOnly()
                        .withAttributes(element -> Map.of("shared", "inner", "inner-only", "kept"))
                        .withOrderingKey(element -> "inner-key")
                        .withAttributes(element -> Collections.singletonMap("shared", "outer"))
                        .withOrderingKey(element -> "outer-key");

        PubsubMessage message = schema.serialize("hello");

        assertThat(message.getOrderingKey()).isEqualTo("outer-key");
        assertThat(message.getAttributesMap())
                .containsExactlyInAnyOrderEntriesOf(
                        Map.of("shared", "outer", "inner-only", "kept"));
    }

    @Test
    void openDelegatesToTheInnerSchema() throws Exception {
        OpenRecordingSchema wrapped = new OpenRecordingSchema();
        PubSubSerializationSchema<String> schema =
                PubSubSerializationSchema.dataOnly(wrapped)
                        .withAttributes(element -> null)
                        .withOrderingKey(element -> null);

        schema.open(null);

        assertThat(wrapped.openCalls).isEqualTo(1);
    }

    @Test
    void passesASkipThroughWhateverTheExtractorsWouldHaveAdded() throws Exception {
        // Both branches are asserted because only one of them rebuilds the message: an extraction
        // that fires is the branch a missing propagation dereferences, and one that does not fire
        // reaches the same return by another route. A skip must not depend on which.
        PubSubSerializationSchema<String> skipping = element -> null;

        assertThat(skipping.withAttributes(element -> Map.of("source", element)).serialize("hello"))
                .isNull();
        assertThat(skipping.withOrderingKey(element -> "key-" + element).serialize("hello"))
                .isNull();
        assertThat(skipping.withAttributes(element -> null).serialize("hello")).isNull();
        assertThat(skipping.withOrderingKey(element -> null).serialize("hello")).isNull();
        assertThat(
                        skipping.withAttributes(element -> Map.of("source", element))
                                .withOrderingKey(element -> "key-" + element)
                                .serialize("hello"))
                .isNull();
    }

    @Test
    void doesNotCallTheExtractorsForASkippedRecord() throws Exception {
        // Not merely an optimisation: an extractor is user code, and running it for a record the
        // sink is not going to send would surface its failures as failures of that record.
        int[] calls = new int[2];
        PubSubSerializationSchema<String> schema =
                ((PubSubSerializationSchema<String>) element -> null)
                        .withAttributes(
                                element -> {
                                    calls[0]++;
                                    return Map.of("source", element);
                                })
                        .withOrderingKey(
                                element -> {
                                    calls[1]++;
                                    return "key";
                                });

        assertThat(schema.serialize("hello")).isNull();

        assertThat(calls).containsExactly(0, 0);
    }

    @Test
    void rejectsNullAttributeEntriesWithAClearMessage() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("valid", null);
        PubSubSerializationSchema<String> schema = dataOnly().withAttributes(element -> attributes);

        assertThatThrownBy(() -> schema.serialize("hello"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("null value")
                .hasMessageContaining("valid");
    }

    /**
     * The twin of the case above, which had no test: only the null-<em>value</em> check was ever
     * driven, so deleting the null-<em>key</em> one left the whole module suite passing. Without it
     * the protobuf builder raises the failure instead, and its whole message is {@code "map key"} —
     * measured, not supposed — which names neither the extractor that produced the entry nor the
     * fact that it was null. Hence the exact message assertion: both are {@code
     * NullPointerException}, so the type alone does not tell the connector's diagnostic from the
     * vendor's.
     */
    @Test
    void rejectsANullAttributeKeyWithAClearMessage() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(null, "value");
        PubSubSerializationSchema<String> schema = dataOnly().withAttributes(element -> attributes);

        assertThatThrownBy(() -> schema.serialize("hello"))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("The attributes extractor returned a null key.");
    }

    @Test
    void roundTripsJavaSerialization() throws Exception {
        PubSubSerializationSchema<String> schema =
                dataOnly()
                        .withAttributes(element -> Collections.singletonMap("source", "test"))
                        .withOrderingKey(element -> element);

        byte[] bytes = InstantiationUtil.serializeObject(schema);
        PubSubSerializationSchema<String> copy =
                InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());
        PubsubMessage message = copy.serialize("hello");

        assertThat(message.getAttributesMap()).containsExactly(Map.entry("source", "test"));
        assertThat(message.getOrderingKey()).isEqualTo("hello");
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
}
