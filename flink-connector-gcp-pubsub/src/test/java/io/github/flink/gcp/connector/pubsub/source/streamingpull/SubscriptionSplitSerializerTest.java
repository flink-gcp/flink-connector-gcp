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

import org.apache.flink.core.memory.DataOutputSerializer;

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
    void roundTripsALedgerChunk() throws IOException {
        SubscriptionSplit chunk =
                SubscriptionSplit.ledgerChunk(
                        SubscriptionDestination.of("project", "sub"),
                        "12-0-3",
                        new byte[] {1, 2, 3});

        SubscriptionSplit restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(chunk));

        assertThat(restored.isLedgerChunk()).isTrue();
        assertThat(restored.getLedgerChunk()).containsExactly(1, 2, 3);
        assertThat(restored.getSubscription()).isEqualTo(chunk.getSubscription());
        assertThat(restored.splitId())
                .isEqualTo("projects/project/subscriptions/sub#ledger:12-0-3");
    }

    @Test
    void aLedgerChunkNeverSharesASplitIdWithAConsumingSplit() {
        SubscriptionDestination sub = SubscriptionDestination.of("project", "sub");

        assertThat(SubscriptionSplit.ledgerChunk(sub, "0", new byte[0]).splitId())
                .isNotEqualTo(new SubscriptionSplit(sub, "0").splitId());
    }

    @Test
    void readsVersionOneAsAConsumingSplit() throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(64);
        out.writeUTF("project");
        out.writeUTF("sub");
        out.writeUTF("3");

        SubscriptionSplit restored = serializer.deserialize(1, out.getCopyOfBuffer());

        assertThat(restored.isLedgerChunk()).isFalse();
        assertThat(restored.splitId()).isEqualTo("projects/project/subscriptions/sub#3");
    }

    @Test
    void rejectsALedgerChunkLengthTheBytesCannotHold() throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(64);
        out.writeUTF("project");
        out.writeUTF("sub");
        out.writeUTF("1-0-0");
        out.writeByte(1);
        out.writeInt(-1);

        assertThatThrownBy(() -> serializer.deserialize(2, out.getCopyOfBuffer()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Malformed ledger chunk");
    }

    @Test
    void rejectsAnUnknownVersion() {
        assertThatThrownBy(() -> serializer.deserialize(99, new byte[0]))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported subscription split serialization version 99");
    }
}
