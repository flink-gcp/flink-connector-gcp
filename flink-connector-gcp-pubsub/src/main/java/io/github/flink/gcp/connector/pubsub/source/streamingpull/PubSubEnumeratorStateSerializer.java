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
import java.util.ArrayList;
import java.util.List;

/**
 * Serializer for {@link PubSubEnumeratorState}.
 *
 * <p>Version 2 appends the start-position flag after the subscription list. Version 1 predates the
 * start position and is still read, mapping to "already applied": that state belongs to a job that
 * has been running, and upgrading the connector must not rewind its subscriptions.
 */
@Internal
public final class PubSubEnumeratorStateSerializer
        implements SimpleVersionedSerializer<PubSubEnumeratorState> {

    private static final int VERSION = 2;

    /** The version that predates {@code startPositionApplied}. */
    private static final int VERSION_WITHOUT_START_POSITION = 1;

    private static final int INITIAL_BUFFER_SIZE = 256;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(PubSubEnumeratorState state) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(INITIAL_BUFFER_SIZE);
        out.writeInt(state.getSubscriptions().size());
        for (SubscriptionDestination subscription : state.getSubscriptions()) {
            out.writeUTF(subscription.getProject());
            out.writeUTF(subscription.getSubscription());
        }
        out.writeBoolean(state.isStartPositionApplied());
        return out.getCopyOfBuffer();
    }

    @Override
    public PubSubEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION && version != VERSION_WITHOUT_START_POSITION) {
            throw new IOException(
                    "Unsupported Pub/Sub enumerator state serialization version "
                            + version
                            + "; this connector reads versions "
                            + VERSION_WITHOUT_START_POSITION
                            + " to "
                            + VERSION
                            + ".");
        }
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        int count = in.readInt();
        List<SubscriptionDestination> subscriptions = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String project = in.readUTF();
            String subscription = in.readUTF();
            subscriptions.add(SubscriptionDestination.of(project, subscription));
        }
        // Version 1 predates the start position, so treat it as applied rather than seeking the
        // subscriptions of a running job just because the connector was upgraded.
        boolean startPositionApplied =
                version == VERSION_WITHOUT_START_POSITION || in.readBoolean();
        return new PubSubEnumeratorState(subscriptions, startPositionApplied);
    }
}
