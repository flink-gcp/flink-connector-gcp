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

package io.github.flink.gcp.connector.tier3.cloudtasks;

import org.apache.flink.annotation.Internal;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.tasks.v2.CreateTaskRequest;
import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.TaskCreator;
import io.grpc.Deadline;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/** Observes production RPC results without replacing retries or extending their deadlines. */
@Internal
final class ObservedTaskCreator implements TaskCreator {
    private final TaskCreator delegate;
    private final Consumer<Observation> observer;
    private final LongSupplier clock;
    private final long attemptLimit;
    private final Consumer<Terminal> terminal;
    private long attempts;
    private long completed;
    private long observations;
    private boolean evidenceFailed;
    private boolean limitReached;
    private boolean closing;

    ObservedTaskCreator(
            TaskCreator delegate,
            Consumer<Observation> observer,
            LongSupplier clock,
            long attemptLimit) {
        this(delegate, observer, clock, attemptLimit, ignored -> {});
    }

    ObservedTaskCreator(
            TaskCreator delegate,
            Consumer<Observation> observer,
            LongSupplier clock,
            long attemptLimit,
            Consumer<Terminal> terminal) {
        if (attemptLimit < 1) {
            throw new IllegalArgumentException("Attempt limit must be positive");
        }
        this.delegate = delegate;
        this.observer = observer;
        this.clock = clock;
        this.attemptLimit = attemptLimit;
        this.terminal = terminal;
    }

    @Override
    public ApiFuture<Task> createTask(CreateTaskRequest request) {
        return send(request, null);
    }

    @Override
    public ApiFuture<Task> createTask(CreateTaskRequest request, Deadline deadline) {
        if (deadline == null) {
            throw new IllegalArgumentException("Absolute deadline is required");
        }
        return send(request, deadline);
    }

    private ApiFuture<Task> send(CreateTaskRequest request, Deadline deadline) {
        MeasurementPayload.Origin origin =
                MeasurementPayload.read(request.getTask().getHttpRequest().getBody());
        long ordinal = reserve();
        long started = clock.getAsLong();
        ApiFuture<Task> call;
        try {
            call =
                    deadline == null
                            ? delegate.createTask(request)
                            : delegate.createTask(request, deadline);
        } catch (RuntimeException failure) {
            try {
                observe(request.getTask().getName(), origin, ordinal, started, null, failure);
            } catch (RuntimeException observationFailure) {
                observationFailure.addSuppressed(failure);
                throw observationFailure;
            }
            throw failure;
        }
        SettableApiFuture<Task> observed = SettableApiFuture.create();
        ApiFutures.addCallback(
                call,
                new ApiFutureCallback<Task>() {
                    @Override
                    public void onSuccess(Task task) {
                        try {
                            observe(
                                    request.getTask().getName(),
                                    origin,
                                    ordinal,
                                    started,
                                    task,
                                    null);
                            observed.set(task);
                        } catch (RuntimeException failure) {
                            observed.setException(failure);
                        }
                    }

                    @Override
                    public void onFailure(Throwable failure) {
                        try {
                            observe(
                                    request.getTask().getName(),
                                    origin,
                                    ordinal,
                                    started,
                                    null,
                                    failure);
                        } catch (RuntimeException observationFailure) {
                            observationFailure.addSuppressed(failure);
                            observed.setException(observationFailure);
                            return;
                        }
                        observed.setException(failure);
                    }
                },
                Runnable::run);
        return new ResultFuture(call, observed);
    }

    private synchronized long reserve() {
        if (closing) {
            throw new IllegalStateException("Measurement creator is closed");
        }
        if (attempts >= attemptLimit) {
            limitReached = true;
            throw new IllegalStateException("Measurement RPC attempt ceiling reached");
        }
        return ++attempts;
    }

    private void observe(
            String requestedName,
            MeasurementPayload.Origin origin,
            long ordinal,
            long started,
            Task result,
            Throwable failure) {
        long completion = clock.getAsLong();
        synchronized (this) {
            completed++;
        }
        try {
            observer.accept(
                    new Observation(
                            origin, ordinal, started, completion, requestedName, result, failure));
            synchronized (this) {
                observations++;
            }
        } catch (RuntimeException error) {
            synchronized (this) {
                evidenceFailed = true;
            }
            throw error;
        }
    }

    @Override
    public void close() throws Exception {
        synchronized (this) {
            if (closing) {
                return;
            }
            closing = true;
        }
        Exception closeFailure = null;
        try {
            delegate.close();
        } catch (Exception failure) {
            closeFailure = failure;
        }
        Terminal snapshot;
        synchronized (this) {
            snapshot =
                    new Terminal(
                            attempts,
                            completed,
                            observations,
                            evidenceFailed,
                            limitReached,
                            closeFailure != null);
        }
        try {
            terminal.accept(snapshot);
        } catch (RuntimeException receiptFailure) {
            if (closeFailure != null) {
                receiptFailure.addSuppressed(closeFailure);
            }
            throw receiptFailure;
        }
        if (closeFailure != null) {
            throw closeFailure;
        }
    }

    record Terminal(
            long attempts,
            long completed,
            long observations,
            boolean evidenceFailed,
            boolean limitReached,
            boolean clientCloseFailed) {
        boolean complete() {
            return attempts == completed
                    && completed == observations
                    && !evidenceFailed
                    && !limitReached
                    && !clientCloseFailed;
        }
    }

    record Observation(
            MeasurementPayload.Origin origin,
            long attempt,
            long startedNanos,
            long completedNanos,
            String requestedName,
            Task result,
            Throwable failure) {}

    private record ResultFuture(ApiFuture<Task> call, ApiFuture<Task> result)
            implements ApiFuture<Task> {
        @Override
        public boolean cancel(boolean interrupt) {
            boolean cancelled = result.cancel(interrupt);
            if (cancelled) {
                call.cancel(interrupt);
            }
            return cancelled;
        }

        @Override
        public boolean isCancelled() {
            return result.isCancelled();
        }

        @Override
        public boolean isDone() {
            return result.isDone();
        }

        @Override
        public Task get() throws java.util.concurrent.ExecutionException, InterruptedException {
            return result.get();
        }

        @Override
        public Task get(long timeout, TimeUnit unit)
                throws java.util.concurrent.ExecutionException,
                        InterruptedException,
                        java.util.concurrent.TimeoutException {
            return result.get(timeout, unit);
        }

        @Override
        public void addListener(Runnable listener, Executor executor) {
            result.addListener(listener, executor);
        }
    }
}
