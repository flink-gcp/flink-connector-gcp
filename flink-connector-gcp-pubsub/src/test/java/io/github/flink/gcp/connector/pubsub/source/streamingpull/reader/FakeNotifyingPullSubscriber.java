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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import com.google.pubsub.v1.PubsubMessage;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

/** Scriptable {@link NotifyingPullSubscriber} standing in for a real streaming-pull client. */
final class FakeNotifyingPullSubscriber implements NotifyingPullSubscriber {

    private final Runnable dataAvailableSignal;
    private final Deque<PubsubMessage> messages = new ArrayDeque<>();

    @Nullable private IOException failure;
    private boolean closed;
    private boolean closeThrows;

    FakeNotifyingPullSubscriber(Runnable dataAvailableSignal) {
        this.dataAvailableSignal = dataAvailableSignal;
    }

    /** Buffers a message as the client library would, and wakes a blocked fetch. */
    void deliver(PubsubMessage... delivered) {
        Collections.addAll(messages, delivered);
        dataAvailableSignal.run();
    }

    /** Makes the next pull report a permanent client failure. */
    void failWith(IOException failure) {
        this.failure = failure;
        dataAvailableSignal.run();
    }

    void failOnClose() {
        this.closeThrows = true;
    }

    boolean isClosed() {
        return closed;
    }

    @Override
    public List<PubsubMessage> pullMessages(int maxMessages) throws IOException {
        if (failure != null) {
            throw failure;
        }
        List<PubsubMessage> drained = new ArrayList<>();
        while (drained.size() < maxMessages && !messages.isEmpty()) {
            drained.add(messages.pollFirst());
        }
        return drained;
    }

    @Override
    public void close() throws Exception {
        closed = true;
        if (closeThrows) {
            throw new IOException("close failed");
        }
    }
}
