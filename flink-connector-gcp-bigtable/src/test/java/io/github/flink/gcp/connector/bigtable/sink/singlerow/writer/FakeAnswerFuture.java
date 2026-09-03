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

package io.github.flink.gcp.connector.bigtable.sink.singlerow.writer;

import com.google.api.core.AbstractApiFuture;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The client's answer as the fake client hands it out: settable by the test, and recording whether
 * a cancel ran its own {@code cancel} override.
 *
 * <p>The client's answers do the same — {@code BigtableUnaryOperationCallable.UnaryFuture} cancels
 * the RPC on the wire from its {@code cancel} override and nowhere else — and a {@code
 * SettableApiFuture} could not tell that path from one that only marks the internal future
 * cancelled. Mirrors the override's shape: the flag is set only when this cancel was the one that
 * resolved the future, as {@code UnaryFuture} cancels upstream only then.
 */
final class FakeAnswerFuture<T> extends AbstractApiFuture<T> {

    private final AtomicBoolean upstreamCancelled = new AtomicBoolean();

    @Override
    public boolean set(T value) {
        return super.set(value);
    }

    @Override
    public boolean setException(Throwable throwable) {
        return super.setException(throwable);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (super.cancel(mayInterruptIfRunning)) {
            upstreamCancelled.set(true);
            return true;
        }
        return false;
    }

    /**
     * Whether a cancel reached this future's own override, the way the client's reaches the wire.
     */
    boolean upstreamCancelled() {
        return upstreamCancelled.get();
    }
}
