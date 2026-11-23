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

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * A client whose lifecycle operations can be made to misbehave, standing in for the SDK {@code
 * Subscriber} — which cannot be subclassed, its only constructor being private. Its close, the
 * release of the transport, is recorded as {@code releaseTransport}.
 */
final class ScriptedClient implements SubscriberClient {

    private final List<String> calls;

    final List<Long> awaitedNanos = new ArrayList<>();
    final List<TimeUnit> awaitedUnits = new ArrayList<>();

    @Nullable private Consumer<Throwable> onPermanentFailure;
    @Nullable private Throwable startFailure;
    @Nullable private Throwable terminationFailure;
    @Nullable private Throwable stopFailure;
    private boolean stuck;

    ScriptedClient(List<String> calls) {
        this.calls = calls;
    }

    /** Takes a {@link Throwable} so a test can script an {@link Error} as well. */
    void failStartWith(Throwable failure) {
        this.startFailure = failure;
    }

    void failTerminationWith(Throwable failure) {
        this.terminationFailure = failure;
    }

    /**
     * Makes the wait behave as it does on a client stuck in shutdown: it sleeps for the whole
     * timeout it is given and then throws {@link TimeoutException}, so the time a caller hands it
     * is spent for real. Never less than that timeout, which a test's arithmetic relies on: the
     * sleep is repeated against {@link System#nanoTime()}, the clock the production deadline is on,
     * so the bound does not rest on how a JDK's {@code Thread.sleep} rounds.
     */
    void neverTerminate() {
        this.stuck = true;
    }

    /** Delivers a permanent failure the way the client's own service listener would. */
    void fail(Throwable failure) {
        requireStarted().accept(failure);
    }

    @Override
    public void start(Consumer<Throwable> onPermanentFailure) {
        this.onPermanentFailure = onPermanentFailure;
        if (startFailure != null) {
            ExceptionUtils.rethrow(startFailure);
        }
    }

    /**
     * Makes the stop deliver a permanent failure the way {@code Subscriber.doStop()}'s own thread
     * does, which is the only way the field can be written after the shutdown began.
     */
    void failWhenStopped(Throwable failure) {
        this.stopFailure = failure;
    }

    @Override
    public void stopAsync() {
        calls.add("stopAsync");
        if (stopFailure != null) {
            requireStarted().accept(stopFailure);
        }
    }

    @Override
    public void awaitTerminated(long timeout, TimeUnit unit) throws TimeoutException {
        calls.add("awaitTerminated");
        awaitedNanos.add(unit.toNanos(timeout));
        awaitedUnits.add(unit);
        if (stuck) {
            long deadline = System.nanoTime() + unit.toNanos(timeout);
            try {
                for (long left = deadline - System.nanoTime();
                        left > 0;
                        left = deadline - System.nanoTime()) {
                    TimeUnit.NANOSECONDS.sleep(left);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while playing a stuck client", e);
            }
            throw new TimeoutException("still shutting down after " + timeout + " " + unit);
        }
        if (terminationFailure instanceof TimeoutException) {
            throw (TimeoutException) terminationFailure;
        }
        if (terminationFailure instanceof RuntimeException) {
            throw (RuntimeException) terminationFailure;
        }
    }

    @Override
    public void close() {
        calls.add("releaseTransport");
    }

    private Consumer<Throwable> requireStarted() {
        if (onPermanentFailure == null) {
            throw new IllegalStateException("the subscriber was never started");
        }
        return onPermanentFailure;
    }
}
