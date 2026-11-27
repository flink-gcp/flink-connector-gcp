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

import org.apache.flink.util.ExceptionUtils;

import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.pubsub.v1.AckResponse;

import javax.annotation.Nullable;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * {@link AckHandle} recording how it was settled, for acknowledgement-lifecycle tests.
 *
 * <p>By default it behaves like the fire-and-forget flavor and returns no confirmation future. Call
 * {@link #withConfirmation(String)} for the flavor {@code awaitAckConfirmation} builds, then settle
 * the returned future by hand; {@link #confirmationObservations()} counts how often anything
 * listened to or waited on that future. Call {@link #failAckWith(Throwable)} or {@link
 * #failNackWith(Throwable)} for a handle whose ack or nack throws after recording the attempt.
 */
final class RecordingAckHandle implements AckHandle {

    private final String name;
    @Nullable private final ObservedConfirmation confirmation;
    private int ackCount;
    private int nackCount;
    @Nullable private Throwable ackFailure;
    @Nullable private Throwable nackFailure;

    RecordingAckHandle(String name) {
        this(name, null);
    }

    private RecordingAckHandle(String name, @Nullable ObservedConfirmation confirmation) {
        this.name = name;
        this.confirmation = confirmation;
    }

    /** Returns a handle whose acknowledgement reports a server response. */
    static RecordingAckHandle withConfirmation(String name) {
        return new RecordingAckHandle(name, new ObservedConfirmation());
    }

    @Override
    @Nullable
    public ApiFuture<AckResponse> ack() {
        ackCount++;
        if (ackFailure != null) {
            ExceptionUtils.rethrow(ackFailure);
        }
        return confirmation;
    }

    @Override
    public void nack() {
        nackCount++;
        if (nackFailure != null) {
            ExceptionUtils.rethrow(nackFailure);
        }
    }

    /**
     * Makes every later {@link #ack()} throw the given failure, as itself for an unchecked one,
     * after counting the attempt.
     */
    RecordingAckHandle failAckWith(Throwable failure) {
        this.ackFailure = failure;
        return this;
    }

    /**
     * Makes every later {@link #nack()} throw the given failure, as itself for an unchecked one,
     * after counting the attempt.
     */
    RecordingAckHandle failNackWith(Throwable failure) {
        this.nackFailure = failure;
        return this;
    }

    /** Completes the confirmation future with the given response. */
    void confirm(AckResponse response) {
        confirmation.delegate.set(response);
    }

    /** Fails the confirmation future, as an exactly-once subscription would. */
    void failConfirmation(Throwable failure) {
        confirmation.delegate.setException(failure);
    }

    /**
     * Returns how many times a listener was added to the confirmation future or a caller waited on
     * it. Zero means nothing waited for this acknowledgement to be confirmed.
     */
    int confirmationObservations() {
        return confirmation.observations;
    }

    boolean isAcked() {
        return ackCount > 0;
    }

    boolean isNacked() {
        return nackCount > 0;
    }

    int ackCount() {
        return ackCount;
    }

    int nackCount() {
        return nackCount;
    }

    boolean isUnsettled() {
        return ackCount == 0 && nackCount == 0;
    }

    @Override
    public String toString() {
        return "ack(" + name + "){acked=" + ackCount + ", nacked=" + nackCount + "}";
    }

    /**
     * A confirmation future that counts every way of observing it. {@code ApiFutures.allAsList},
     * which the confirmation wait uses, adds a listener to a pending future and calls {@code get()}
     * on one already done, so both are counted.
     *
     * <p>It delegates rather than extending {@code AbstractApiFuture} on purpose: {@code
     * ApiFutures} unwraps that type to its internal Guava future and would never call the
     * overrides.
     */
    private static final class ObservedConfirmation implements ApiFuture<AckResponse> {

        private final SettableApiFuture<AckResponse> delegate = SettableApiFuture.create();
        private int observations;

        @Override
        public void addListener(Runnable listener, Executor executor) {
            observations++;
            delegate.addListener(listener, executor);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return delegate.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return delegate.isCancelled();
        }

        @Override
        public boolean isDone() {
            return delegate.isDone();
        }

        @Override
        public AckResponse get() throws InterruptedException, ExecutionException {
            observations++;
            return delegate.get();
        }

        @Override
        public AckResponse get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            observations++;
            return delegate.get(timeout, unit);
        }
    }
}
