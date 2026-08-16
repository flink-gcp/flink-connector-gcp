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

package io.github.flink.gcp.connector.pubsub.source.streamingpull;

import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SubscriptionSplitSerializer}. */
class SubscriptionSplitSerializerTest {

    private final SubscriptionSplitSerializer serializer = new SubscriptionSplitSerializer();

    @Test
    void roundTripsASplit() throws IOException {
        SubscriptionSplit split =
                new SubscriptionSplit(SubscriptionDestination.of("project", "sub"), "3");

        SubscriptionSplit restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(split));

        assertThat(restored).isEqualTo(split);
        assertThat(restored.getSubscription()).isEqualTo(split.getSubscription());
        assertThat(restored.getUid()).isEqualTo("3");
        assertThat(restored.splitId()).isEqualTo("projects/project/subscriptions/sub#3");
    }

    @Test
    void rejectsAnUnknownVersion() {
        assertThatThrownBy(() -> serializer.deserialize(99, new byte[0]))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported subscription split serialization version 99");
    }
}
