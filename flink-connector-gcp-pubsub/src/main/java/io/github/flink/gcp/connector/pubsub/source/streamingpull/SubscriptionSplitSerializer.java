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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;

import java.io.IOException;

/**
 * Serializer for {@link SubscriptionSplit}.
 *
 * <p>Hand-written rather than generated: the split is three strings, so a protobuf schema and its
 * code-generation plugin would cost more than they save (the upstream connector generates one).
 *
 * <p>Version 2 adds a kind byte and the ledger-chunk payload. Version 1 holds a consuming split
 * only and is still read. Nothing in a checkpoint was ever written in version 1 — readers
 * checkpointed no splits until ledger chunks — so the bump does not change what a job that leaves
 * restore deduplication off writes.
 */
@Internal
public final class SubscriptionSplitSerializer
        implements SimpleVersionedSerializer<SubscriptionSplit> {

    private static final int VERSION = 2;

    private static final byte KIND_CONSUMING = 0;

    private static final byte KIND_LEDGER_CHUNK = 1;

    /** Enough for a subscription path and a uid; the serializer grows the buffer if needed. */
    private static final int INITIAL_BUFFER_SIZE = 256;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(SubscriptionSplit split) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(INITIAL_BUFFER_SIZE);
        out.writeUTF(split.getSubscription().getProject());
        out.writeUTF(split.getSubscription().getSubscription());
        out.writeUTF(split.getUid());
        if (split.isLedgerChunk()) {
            byte[] chunk = split.getLedgerChunk();
            out.writeByte(KIND_LEDGER_CHUNK);
            out.writeInt(chunk.length);
            out.write(chunk);
        } else {
            out.writeByte(KIND_CONSUMING);
        }
        return out.getCopyOfBuffer();
    }

    @Override
    public SubscriptionSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != 1 && version != VERSION) {
            throw new IOException(
                    "Unsupported subscription split serialization version "
                            + version
                            + "; this connector writes version "
                            + VERSION
                            + ".");
        }
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        String project = in.readUTF();
        String subscription = in.readUTF();
        String uid = in.readUTF();
        SubscriptionDestination destination = SubscriptionDestination.of(project, subscription);
        if (version == 1) {
            return new SubscriptionSplit(destination, uid);
        }
        byte kind = in.readByte();
        switch (kind) {
            case KIND_CONSUMING:
                return new SubscriptionSplit(destination, uid);
            case KIND_LEDGER_CHUNK:
                int length = in.readInt();
                if (length < 0 || length > in.available()) {
                    throw new IOException(
                            "Malformed ledger chunk: length "
                                    + length
                                    + " with "
                                    + in.available()
                                    + " bytes remaining.");
                }
                byte[] chunk = new byte[length];
                in.readFully(chunk);
                return SubscriptionSplit.ledgerChunk(destination, uid, chunk);
            default:
                throw new IOException("Unknown subscription split kind " + kind + ".");
        }
    }
}
