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

package io.github.flink.gcp.connector.bigtable.sink;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Publishes completion only after its measurement is recorded, preserving RPC cancellation. */
final class Stage2ObservedFuture<V> implements ApiFuture<V> {
    interface Completion<V> {
        void completed(V value, long completedAt) throws Exception;
    }

    private final ApiFuture<V> original;
    private final SettableApiFuture<V> observed = SettableApiFuture.create();
    private final LocalStagedHarness run;
    private final boolean measureCommitWait;

    Stage2ObservedFuture(ApiFuture<V> original, LocalStagedHarness run, Completion<V> completion) {
        this(original, run, completion, true);
    }

    Stage2ObservedFuture(
            ApiFuture<V> original,
            LocalStagedHarness run,
            Completion<V> completion,
            boolean measureCommitWait) {
        this.original = original;
        this.run = run;
        this.measureCommitWait = measureCommitWait;
        run.peakActive.accumulateAndGet(run.active.incrementAndGet(), Math::max);
        ApiFutures.addCallback(
                original,
                new ApiFutureCallback<V>() {
                    @Override
                    public void onSuccess(V value) {
                        Exception failure = null;
                        try {
                            completion.completed(value, System.nanoTime());
                        } catch (Exception problem) {
                            failure = problem;
                        } finally {
                            run.active.decrementAndGet();
                        }
                        if (failure == null) {
                            observed.set(value);
                        } else {
                            observed.setException(failure);
                        }
                    }

                    @Override
                    public void onFailure(Throwable failure) {
                        run.active.decrementAndGet();
                        if (original.isCancelled()) {
                            observed.cancel(false);
                        } else {
                            observed.setException(failure);
                        }
                    }
                },
                Runnable::run);
    }

    @Override
    public void addListener(Runnable listener, Executor executor) {
        observed.addListener(listener, executor);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return original.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return observed.isCancelled();
    }

    @Override
    public boolean isDone() {
        return observed.isDone();
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        long started = System.nanoTime();
        try {
            return observed.get();
        } finally {
            if (measureCommitWait) {
                run.committerWaited(System.nanoTime() - started);
            }
        }
    }

    @Override
    public V get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        long started = System.nanoTime();
        try {
            return observed.get(timeout, unit);
        } finally {
            if (measureCommitWait) {
                run.committerWaited(System.nanoTime() - started);
            }
        }
    }
}
