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

import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.pubsub.v1.AckResponse;

import javax.annotation.Nullable;

/**
 * {@link AckHandle} recording how it was settled, for acknowledgement-lifecycle tests.
 *
 * <p>By default it behaves like the fire-and-forget flavor and returns no confirmation future. Call
 * {@link #withConfirmation()} for the flavor {@code awaitAckConfirmation} builds, then settle the
 * returned future by hand.
 */
final class RecordingAckHandle implements AckHandle {

    private final String name;
    @Nullable private final SettableApiFuture<AckResponse> confirmation;
    private int ackCount;
    private int nackCount;

    RecordingAckHandle(String name) {
        this(name, null);
    }

    private RecordingAckHandle(String name, @Nullable SettableApiFuture<AckResponse> confirmation) {
        this.name = name;
        this.confirmation = confirmation;
    }

    /** Returns a handle whose acknowledgement reports a server response. */
    static RecordingAckHandle withConfirmation(String name) {
        return new RecordingAckHandle(name, SettableApiFuture.create());
    }

    @Override
    @Nullable
    public ApiFuture<AckResponse> ack() {
        ackCount++;
        return confirmation;
    }

    @Override
    public void nack() {
        nackCount++;
    }

    /** Completes the confirmation future with the given response. */
    void confirm(AckResponse response) {
        confirmation.set(response);
    }

    /** Fails the confirmation future, as an exactly-once subscription would. */
    void failConfirmation(Throwable failure) {
        confirmation.setException(failure);
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
