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
 * A client whose three lifecycle operations can be made to misbehave, standing in for the SDK
 * {@code Subscriber} — which cannot be subclassed, its only constructor being private.
 */
final class ScriptedClient {

    private final List<String> calls;

    final List<Long> awaitedMillis = new ArrayList<>();
    final List<TimeUnit> awaitedUnits = new ArrayList<>();

    @Nullable private Consumer<Throwable> onPermanentFailure;
    @Nullable private Throwable startFailure;
    @Nullable private Throwable terminationFailure;
    @Nullable private Throwable stopFailure;

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

    /** Delivers a permanent failure the way the client's own service listener would. */
    void fail(Throwable failure) {
        requireStarted().accept(failure);
    }

    void start(Consumer<Throwable> onPermanentFailure) {
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

    void stopAsync() {
        calls.add("stopAsync");
        if (stopFailure != null) {
            requireStarted().accept(stopFailure);
        }
    }

    void awaitTerminated(long timeout, TimeUnit unit) throws TimeoutException {
        calls.add("awaitTerminated");
        awaitedMillis.add(unit.toMillis(timeout));
        awaitedUnits.add(unit);
        if (terminationFailure instanceof TimeoutException) {
            throw (TimeoutException) terminationFailure;
        }
        if (terminationFailure instanceof RuntimeException) {
            throw (RuntimeException) terminationFailure;
        }
    }

    private Consumer<Throwable> requireStarted() {
        if (onPermanentFailure == null) {
            throw new IllegalStateException("the subscriber was never started");
        }
        return onPermanentFailure;
    }
}
