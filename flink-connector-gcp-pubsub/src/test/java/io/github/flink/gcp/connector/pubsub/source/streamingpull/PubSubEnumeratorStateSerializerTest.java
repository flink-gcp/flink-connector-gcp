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

package io.github.flink.gcp.connector.pubsub.source.streamingpull;

import org.apache.flink.core.memory.DataOutputSerializer;

import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PubSubEnumeratorStateSerializer}. */
class PubSubEnumeratorStateSerializerTest {

    private final PubSubEnumeratorStateSerializer serializer =
            new PubSubEnumeratorStateSerializer();

    @Test
    void roundTripsTheSubscriptionList() throws IOException {
        PubSubEnumeratorState state =
                new PubSubEnumeratorState(
                        Arrays.asList(
                                SubscriptionDestination.of("project", "a"),
                                SubscriptionDestination.of("other-project", "b")),
                        false);

        PubSubEnumeratorState restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(state));

        assertThat(restored).isEqualTo(state);
    }

    @Test
    void roundTripsAnAppliedStartPosition() throws IOException {
        PubSubEnumeratorState state =
                new PubSubEnumeratorState(
                        Collections.singletonList(SubscriptionDestination.of("project", "a")),
                        true);

        assertThat(serializer.deserialize(serializer.getVersion(), serializer.serialize(state)))
                .isEqualTo(state);
    }

    @Test
    void roundTripsAnEmptySubscriptionList() throws IOException {
        PubSubEnumeratorState state = new PubSubEnumeratorState(Collections.emptyList(), false);

        assertThat(serializer.deserialize(serializer.getVersion(), serializer.serialize(state)))
                .isEqualTo(state);
    }

    @Test
    void readsVersionOneStateAsHavingAppliedItsStartPosition() throws IOException {
        // Hand-built rather than produced by serialize(), which now writes version 2.
        DataOutputSerializer out = new DataOutputSerializer(64);
        out.writeInt(1);
        out.writeUTF("project");
        out.writeUTF("a");

        PubSubEnumeratorState restored = serializer.deserialize(1, out.getCopyOfBuffer());

        assertThat(restored.getSubscriptions())
                .containsExactly(SubscriptionDestination.of("project", "a"));
        // A job checkpointed before the start position existed is already consuming; seeking it
        // because the connector was upgraded would rewind its subscriptions.
        assertThat(restored.isStartPositionApplied()).isTrue();
    }

    @Test
    void rejectsAnUnknownVersion() {
        assertThatThrownBy(() -> serializer.deserialize(42, new byte[0]))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(
                        "Unsupported Pub/Sub enumerator state serialization version 42");
    }
}
