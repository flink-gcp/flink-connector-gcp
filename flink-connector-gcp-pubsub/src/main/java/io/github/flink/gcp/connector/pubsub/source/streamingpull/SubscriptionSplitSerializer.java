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
 */
@Internal
public final class SubscriptionSplitSerializer
        implements SimpleVersionedSerializer<SubscriptionSplit> {

    private static final int VERSION = 1;

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
        return out.getCopyOfBuffer();
    }

    @Override
    public SubscriptionSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
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
        return new SubscriptionSplit(SubscriptionDestination.of(project, subscription), uid);
    }
}
