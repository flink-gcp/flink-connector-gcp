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
                                SubscriptionDestination.of("other-project", "b")));

        PubSubEnumeratorState restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(state));

        assertThat(restored).isEqualTo(state);
    }

    @Test
    void roundTripsAnEmptySubscriptionList() throws IOException {
        PubSubEnumeratorState state = new PubSubEnumeratorState(Collections.emptyList());

        assertThat(serializer.deserialize(serializer.getVersion(), serializer.serialize(state)))
                .isEqualTo(state);
    }

    @Test
    void rejectsAnUnknownVersion() {
        assertThatThrownBy(() -> serializer.deserialize(42, new byte[0]))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(
                        "Unsupported Pub/Sub enumerator state serialization version 42");
    }
}
