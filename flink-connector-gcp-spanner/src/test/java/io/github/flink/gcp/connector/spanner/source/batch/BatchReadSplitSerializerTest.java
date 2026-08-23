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

package io.github.flink.gcp.connector.spanner.source.batch;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.google.cloud.spanner.BatchTransactionId;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.TestPartitions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BatchReadSplitSerializer}. */
class BatchReadSplitSerializerTest {

    private final BatchReadSplitSerializer serializer = new BatchReadSplitSerializer();

    @Test
    void aQueryPartitionSurvivesTheRoundTrip() throws Exception {
        BatchReadSplit split =
                new BatchReadSplit(
                        "3",
                        TestPartitions.batchTransactionId("sessions/s", "txn-1", 42L),
                        TestPartitions.queryPartition("token-3", "SELECT id FROM singers"));

        BatchReadSplit restored = roundTrip(split);

        assertThat(restored.splitId()).isEqualTo("3");
        assertThat(restored).isEqualTo(split);
        // The token is the one thing a partition exposes, and it is what the service reads the
        // partition back by — so it is asserted directly rather than only through equals.
        assertThat(restored.getPartition().getPartitionToken().toStringUtf8()).isEqualTo("token-3");
        assertThat(restored.getBatchTransactionId())
                .isEqualTo(TestPartitions.batchTransactionId("sessions/s", "txn-1", 42L));
    }

    @Test
    void aTableReadPartitionSurvivesTheRoundTrip() throws Exception {
        // The read shape carries a table, a key set and a column list where the query shape carries
        // a statement, so a serializer that happened to work for one could still lose the other.
        BatchReadSplit split =
                new BatchReadSplit(
                        "0",
                        TestPartitions.batchTransactionId(),
                        TestPartitions.readPartition("token-0", "singers", "id", "name"));

        assertThat(roundTrip(split)).isEqualTo(split);
    }

    @Test
    void anUnknownVersionIsRefusedByName() throws Exception {
        byte[] bytes = serializer.serialize(split());

        assertThatThrownBy(() -> serializer.deserialize(serializer.getVersion() + 1, bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported Spanner partition split serialization version");
    }

    @Test
    void aNegativeLengthIsRefusedRatherThanAllocated() {
        // A length prefix is read before the bytes it describes; a negative one has to be refused
        // before it reaches an array allocation.
        byte[] corrupt = new byte[] {0, 1, 'x', (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};

        assertThatThrownBy(() -> serializer.deserialize(serializer.getVersion(), corrupt))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("negative batch transaction id length");
    }

    @Test
    void aValueOfTheWrongTypeIsRefused() throws Exception {
        // Java serialization will happily read back whatever the bytes hold, so the type is checked
        // rather than cast: a mismatch is a corrupt checkpoint, not a ClassCastException in a
        // stack frame that says nothing about what was being read.
        byte[] bytes = serializer.serialize(split());
        byte[] forged = forgeFirstObject(bytes, "not a batch transaction id");

        assertThatThrownBy(() -> serializer.deserialize(serializer.getVersion(), forged))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("expected the batch transaction id to be a");
    }

    @Test
    void theVersionIsTheOneTheSerializerWrites() {
        assertThat(serializer.getVersion()).isEqualTo(1);
    }

    private BatchReadSplit roundTrip(BatchReadSplit split) throws IOException {
        return serializer.deserialize(serializer.getVersion(), serializer.serialize(split));
    }

    private static BatchReadSplit split() {
        BatchTransactionId id = TestPartitions.batchTransactionId();
        Partition partition = TestPartitions.queryPartition("token", "SELECT 1");
        return new BatchReadSplit("0", id, partition);
    }

    /**
     * Replaces the first length-prefixed object in a serialized split with another one.
     *
     * <p>Written out rather than hand-crafted as a byte array, so that a change to the split's
     * framing fails this test loudly instead of leaving it asserting on bytes that no longer mean
     * what it thinks.
     */
    private static byte[] forgeFirstObject(byte[] bytes, Serializable replacement)
            throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(bytes);
        String splitId = in.readUTF();
        int firstLength = in.readInt();
        in.skipBytesToRead(firstLength);
        byte[] rest = new byte[in.available()];
        in.readFully(rest);

        ByteArrayOutputStream replacementBytes = new ByteArrayOutputStream();
        try (ObjectOutputStream objects = new ObjectOutputStream(replacementBytes)) {
            objects.writeObject(replacement);
        }
        DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeUTF(splitId);
        out.writeInt(replacementBytes.size());
        out.write(replacementBytes.toByteArray());
        out.write(rest);
        return out.getCopyOfBuffer();
    }
}
