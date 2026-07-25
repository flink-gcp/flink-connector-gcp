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

import com.google.cloud.pubsub.v1.AckReplyConsumer;

/** {@link AckReplyConsumer} recording how it was settled, for acknowledgement-lifecycle tests. */
final class RecordingAckReplyConsumer implements AckReplyConsumer {

    private final String name;
    private int ackCount;
    private int nackCount;

    RecordingAckReplyConsumer(String name) {
        this.name = name;
    }

    @Override
    public void ack() {
        ackCount++;
    }

    @Override
    public void nack() {
        nackCount++;
    }

    boolean isAcked() {
        return ackCount > 0;
    }

    boolean isNacked() {
        return nackCount > 0;
    }

    boolean isUnsettled() {
        return ackCount == 0 && nackCount == 0;
    }

    @Override
    public String toString() {
        return "ack(" + name + "){acked=" + ackCount + ", nacked=" + nackCount + "}";
    }
}
