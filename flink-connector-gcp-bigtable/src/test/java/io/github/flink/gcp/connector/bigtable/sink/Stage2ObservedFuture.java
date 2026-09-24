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

import javax.annotation.Nullable;

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
    @Nullable private final Stage2CommitProgress.Invocation invocation;

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
        invocation = measureCommitWait ? run.commitSent() : null;
        ApiFutures.addCallback(
                original,
                new ApiFutureCallback<V>() {
                    @Override
                    public void onSuccess(V value) {
                        Exception failure = null;
                        long completedAt = System.nanoTime();
                        try {
                            completion.completed(value, completedAt);
                        } catch (Exception problem) {
                            failure = problem;
                        } finally {
                            run.completionHeld(System.nanoTime() - completedAt);
                            run.active.decrementAndGet();
                        }
                        if (failure == null) {
                            observed.set(value);
                        } else {
                            observed.setException(failure);
                        }
                        // Only after publishing, so a request is never counted complete behind
                        // itself while the committer is already waiting on it.
                        completedInInvocation();
                    }

                    @Override
                    public void onFailure(Throwable failure) {
                        run.active.decrementAndGet();
                        if (original.isCancelled()) {
                            observed.cancel(false);
                        } else {
                            observed.setException(failure);
                        }
                        completedInInvocation();
                    }
                },
                Runnable::run);
    }

    private void completedInInvocation() {
        if (invocation != null) {
            invocation.completed();
        }
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
        boolean blocked = !observed.isDone();
        int completedBehind = invocation == null ? 0 : invocation.completedBehindHead(!blocked);
        long started = System.nanoTime();
        try {
            return observed.get();
        } finally {
            if (measureCommitWait) {
                long waited = System.nanoTime() - started;
                run.committerWaited(waited);
                if (invocation != null) {
                    invocation.waited(waited, blocked, completedBehind);
                }
            }
        }
    }

    @Override
    public V get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        boolean blocked = !observed.isDone();
        int completedBehind = invocation == null ? 0 : invocation.completedBehindHead(!blocked);
        long started = System.nanoTime();
        try {
            return observed.get(timeout, unit);
        } finally {
            if (measureCommitWait) {
                long waited = System.nanoTime() - started;
                run.committerWaited(waited);
                if (invocation != null) {
                    invocation.waited(waited, blocked, completedBehind);
                }
            }
        }
    }
}
