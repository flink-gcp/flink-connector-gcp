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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Serializer for {@link PubSubEnumeratorState}. */
@Internal
public final class PubSubEnumeratorStateSerializer
        implements SimpleVersionedSerializer<PubSubEnumeratorState> {

    private static final int VERSION = 1;

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
        return out.getCopyOfBuffer();
    }

    @Override
    public PubSubEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException(
                    "Unsupported Pub/Sub enumerator state serialization version "
                            + version
                            + "; this connector writes version "
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
        return new PubSubEnumeratorState(subscriptions);
    }
}
