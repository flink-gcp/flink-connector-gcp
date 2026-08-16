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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import javax.annotation.Nullable;

import java.util.List;

/** Records the one call this class makes, and answers the rest with nothing. */
final class RecordingAckTracker implements AckTracker {

    private final List<String> calls;

    @Nullable private RuntimeException nackFailure;

    RecordingAckTracker(List<String> calls) {
        this.calls = calls;
    }

    /**
     * Makes {@link #nackSplit} throw, which is the step {@code shutdown()} runs before it asks the
     * client to stop.
     */
    void failNackWith(RuntimeException nackFailure) {
        this.nackFailure = nackFailure;
    }

    @Override
    public void addPendingAck(String splitId, String messageId, AckHandle ackHandle) {}

    @Override
    public void stagePendingAck(String splitId, String messageId) {}

    @Override
    public void ackPendingImmediately(String splitId, String messageId) {}

    @Override
    public void nackPendingImmediately(String splitId, String messageId) {}

    @Override
    public void addCheckpoint(long checkpointId) {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void nackSplit(String splitId) {
        calls.add("nackSplit");
        if (nackFailure != null) {
            // After the record, as the real tracker's own nacks happen before anything can
            // fail: this is a nack that ran and then threw, not one that never started.
            throw nackFailure;
        }
    }
}
