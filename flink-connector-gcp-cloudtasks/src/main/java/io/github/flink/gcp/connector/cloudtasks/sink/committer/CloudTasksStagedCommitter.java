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

package io.github.flink.gcp.connector.cloudtasks.sink.committer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;
import org.apache.flink.util.Preconditions;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.tasks.v2.CreateTaskRequest;
import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.base.metrics.DestinationMetrics;
import io.github.flink.gcp.connector.base.metrics.ErrorClassCounters;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.cloudtasks.CloudTasksMetricNames;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksCommittable;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksReplaySnapshot;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksStagedOptions;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksStagingConfig;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksWriterOptions;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.CloudTasksErrorClassifier;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.TaskCreator;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.TimeSource;
import io.grpc.Deadline;
import io.grpc.Status;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Creates checkpoint-owned envelopes in a blocking, bounded-concurrency loop. Only the committing
 * thread changes requests or metrics. Callbacks enqueue attempt-local results; an old callback
 * cannot authorize a retry or change a replacement committer.
 */
@Internal
public final class CloudTasksStagedCommitter implements Committer<CloudTasksCommittable> {
    private final String queuePath;
    private final CloudTasksStagedOptions options;
    private final CloudTasksStagingConfig staging;
    private final int concurrency;
    private final RetrySchedule recovery;
    private final RetrySchedule notFoundRecovery;
    private final TaskCreator creator;
    private final TimeSource clock;
    private final Counter deduplicated;
    private final Counter expiredFailed;
    private volatile CloudTasksReplaySnapshot replaySnapshot = CloudTasksReplaySnapshot.empty();
    private final Counter expiredAssumedCommitted;
    private final Counter expiredDropped;
    private final Counter expiredCreatesAuthorized;
    private final DestinationMetrics.Counters destination;
    private final ErrorClassCounters errorClasses;
    private final Object sendLock = new Object();
    private volatile boolean closed;
    private volatile Thread committingThread;

    /** Creates a committer after its factory has checked the queue prerequisites. */
    public CloudTasksStagedCommitter(
            String queuePath,
            CloudTasksStagedOptions options,
            CloudTasksWriterOptions writerOptions,
            TaskCreator creator,
            TimeSource clock,
            SinkCommitterMetricGroup metricGroup) {
        this.queuePath = Preconditions.checkNotNull(queuePath, "queuePath");
        this.options = Preconditions.checkNotNull(options, "stagedOptions");
        this.staging = options.toStagingConfig();
        this.concurrency = writerOptions.getMaxInFlightTasks();
        Preconditions.checkArgument(concurrency > 0, "maxInFlightTasks must be positive");
        this.recovery = writerOptions.toRecoverySchedule();
        this.notFoundRecovery = writerOptions.toNotFoundRecoverySchedule();
        this.creator = Preconditions.checkNotNull(creator, "creator");
        this.clock = Preconditions.checkNotNull(clock, "clock");
        this.deduplicated = metricGroup.counter(CloudTasksMetricNames.TASKS_DEDUPLICATED);
        this.expiredFailed = metricGroup.counter(CloudTasksMetricNames.EXPIRED_ENVELOPES_FAILED);
        metricGroup.gauge(
                CloudTasksMetricNames.CURRENT_COMMIT_OLDEST_TASK_AGE_MILLIS,
                () -> replaySnapshot.ageMillis(clock.currentTimeMillis()));
        metricGroup.gauge(
                CloudTasksMetricNames.CURRENT_COMMIT_REPLAY_BUDGET_MILLIS,
                () -> replaySnapshot.remainingMillis(clock.currentTimeMillis()));
        this.expiredAssumedCommitted =
                metricGroup.counter(CloudTasksMetricNames.EXPIRED_ENVELOPES_ASSUMED_COMMITTED);
        this.expiredDropped = metricGroup.counter(CloudTasksMetricNames.EXPIRED_ENVELOPES_DROPPED);
        this.expiredCreatesAuthorized =
                metricGroup.counter(CloudTasksMetricNames.EXPIRED_ENVELOPE_CREATES_AUTHORIZED);
        this.destination =
                DestinationMetrics.of(metricGroup, writerOptions.isPerDestinationMetrics())
                        .forDestination(queuePath);
        this.errorClasses = new ErrorClassCounters(metricGroup);
    }

    @Override
    public void commit(Collection<CommitRequest<CloudTasksCommittable>> requests)
            throws IOException, InterruptedException {
        synchronized (sendLock) {
            checkRunning();
            Preconditions.checkState(
                    committingThread == null, "Concurrent Cloud Tasks commits are unsupported.");
            committingThread = Thread.currentThread();
        }
        List<Entry> entries = new ArrayList<>();
        BlockingQueue<Completion> completions = new LinkedBlockingQueue<>();
        try {
            // Validate every durable header before authorizing any request, including overrides.
            CloudTasksReplaySnapshot observed = CloudTasksReplaySnapshot.empty();
            for (var request : requests) {
                var envelope = request.getCommittable();
                validateEnvelope(envelope);
                observed =
                        observed.include(
                                envelope.getOriginEpochMillis(), effectiveDeadline(envelope));
            }
            // Keep the whole invocation's minima, including tasks completed before its last wave.
            replaySnapshot = observed;
            var pending = requests.iterator();
            while (pending.hasNext() || !entries.isEmpty()) {
                checkRunning();
                Completion result;
                while ((result = completions.poll()) != null) {
                    complete(result, entries);
                }
                for (Entry entry : entries) {
                    Attempt attempt = entry.attempt;
                    if (attempt != null && attempt.deadline.isExpired()) {
                        entry.attempt = null;
                        attempt.future.cancel(true);
                        retry(entry, Status.DEADLINE_EXCEEDED.asRuntimeException());
                    }
                }
                if (pending.hasNext() && entries.size() < concurrency) {
                    var request = pending.next();
                    // Parse even an expired envelope before consulting its override: poison state
                    // must never be turned into success by an operator's expiry decision.
                    var task = request.getCommittable().parseTask();
                    entries.add(
                            new Entry(
                                    request,
                                    CreateTaskRequest.newBuilder()
                                            .setParent(request.getCommittable().getQueuePath())
                                            .setTask(task)
                                            .build()));
                }
                boolean sent = false;
                for (var iterator = entries.iterator(); iterator.hasNext(); ) {
                    Entry entry = iterator.next();
                    if (entry.attempt == null && System.nanoTime() - entry.dueNanos >= 0) {
                        if (!send(entry, completions)) {
                            iterator.remove();
                        }
                        sent = true;
                        // Observe an immediate failure before authorizing more queued work.
                        break;
                    }
                }
                if (sent || pending.hasNext() && entries.size() < concurrency) {
                    continue;
                }
                if (!entries.isEmpty()) {
                    long waitNanos = Long.MAX_VALUE;
                    for (Entry entry : entries) {
                        long remaining =
                                entry.attempt == null
                                        ? entry.dueNanos - System.nanoTime()
                                        : entry.attempt.deadline.timeRemaining(
                                                TimeUnit.NANOSECONDS);
                        waitNanos = Math.min(waitNanos, Math.max(0, remaining));
                    }
                    result = completions.poll(waitNanos, TimeUnit.NANOSECONDS);
                    if (result != null) {
                        complete(result, entries);
                    }
                }
            }
        } finally {
            replaySnapshot = CloudTasksReplaySnapshot.empty();
            for (Entry entry : entries) {
                if (entry.attempt != null) {
                    entry.attempt.future.cancel(true);
                }
            }
            synchronized (sendLock) {
                committingThread = null;
            }
        }
    }

    private void validateEnvelope(CloudTasksCommittable envelope) throws IOException {
        if (envelope == null || !queuePath.equals(envelope.getQueuePath())) {
            throw new IOException(
                    "Cloud Tasks restored queue does not match queue(...); restore the original fixed destination.");
        }
        if (envelope.getAuthorizationDeadlineMillis() <= envelope.getOriginEpochMillis()) {
            throw new IOException(
                    "Cloud Tasks envelope has an unknown authorization deadline; recovery overrides do not apply.");
        }
        effectiveDeadline(envelope);
    }

    private long effectiveDeadline(CloudTasksCommittable envelope) throws IOException {
        try {
            return Math.min(
                    envelope.getAuthorizationDeadlineMillis(),
                    staging.authorizationDeadlineMillis(envelope.getOriginEpochMillis()));
        } catch (ArithmeticException e) {
            throw new IOException(
                    "Cloud Tasks envelope authorization deadline overflows; recovery overrides do not apply.");
        }
    }

    private boolean send(Entry entry, BlockingQueue<Completion> completions)
            throws IOException, InterruptedException {
        synchronized (sendLock) {
            checkRunning();
            // Create the absolute RPC deadline before reading the authorization clock, so a pause
            // after that check cannot restart the client budget at transport dispatch.
            Deadline rpcDeadline =
                    Deadline.after(options.getRequestTimeout().toNanos(), TimeUnit.NANOSECONDS);
            long now = clock.currentTimeMillis();
            var envelope = entry.request.getCommittable();
            long deadline = effectiveDeadline(envelope);
            if (now >= deadline) {
                switch (options.getExpiredEnvelopePolicy()) {
                    case FAIL:
                        expiredFailed.inc();
                        throw new IOException(
                                "Cloud Tasks envelope expired: queue="
                                        + queuePath
                                        + ", originEpochMillis="
                                        + envelope.getOriginEpochMillis()
                                        + ", authorizationDeadlineMillis="
                                        + deadline
                                        + ", now="
                                        + now
                                        + ". Restore the retained latest checkpoint; expiredEnvelopePolicy is an explicit"
                                        + " loss/duplicate-risk override. See docs/connectors/datastream/cloudtasks/#recovery-runbook.");
                    case ASSUME_COMMITTED:
                        expiredAssumedCommitted.inc();
                        entry.request.signalAlreadyCommitted();
                        return false;
                    case DROP:
                        expiredDropped.inc();
                        return false;
                    case CREATE_ANYWAY:
                        expiredCreatesAuthorized.inc();
                        break;
                    default:
                        throw new IOException("Unknown Cloud Tasks expiredEnvelopePolicy.");
                }
            }
            if (!entry.counted) {
                destination.recordSent();
                entry.counted = true;
            }
            Attempt attempt = new Attempt(entry, rpcDeadline);
            entry.attempt = attempt;
            try {
                attempt.future = creator.createTask(entry.create, rpcDeadline);
            } catch (RuntimeException e) {
                entry.attempt = null;
                retry(entry, e);
                return true;
            }
            ApiFutures.addCallback(
                    attempt.future,
                    new ApiFutureCallback<Task>() {
                        @Override
                        public void onSuccess(Task task) {
                            completions.add(new Completion(attempt, null));
                        }

                        @Override
                        public void onFailure(Throwable failure) {
                            completions.add(new Completion(attempt, failure));
                        }
                    },
                    Runnable::run);
            return true;
        }
    }

    private void complete(Completion completion, List<Entry> entries) throws IOException {
        Entry entry = completion.attempt.entry;
        if (entry.attempt != completion.attempt) {
            return;
        }
        entry.attempt = null;
        if (completion.failure == null) {
            entries.remove(entry);
        } else if (CloudTasksErrorClassifier.statusCode(completion.failure)
                == StatusCode.Code.ALREADY_EXISTS) {
            entry.request.signalAlreadyCommitted();
            deduplicated.inc();
            entries.remove(entry);
        } else {
            retry(entry, completion.failure);
        }
    }

    private void retry(Entry entry, Throwable failure) throws IOException {
        StatusCode.Code code = CloudTasksErrorClassifier.statusCode(failure);
        errorClasses.count(code);
        final long backoff;
        if (CloudTasksErrorClassifier.transientCode(failure) != null
                && ++entry.failures < recovery.maxAttempts()) {
            backoff = recovery.backoffMs(entry.failures);
        } else if (CloudTasksErrorClassifier.transientCode(failure) == null
                && code == StatusCode.Code.NOT_FOUND
                && ++entry.notFoundFailures < notFoundRecovery.maxAttempts()) {
            backoff = notFoundRecovery.backoffMs(entry.notFoundFailures);
        } else {
            destination.sendFailed();
            // Vendor exceptions can echo request contents. The status and queue locate the
            // failure without retaining a body, authentication header, or user exception cause.
            throw new IOException(
                    "Cloud Tasks checkpoint commit failed for queue="
                            + queuePath
                            + ", status="
                            + code
                            + ", transientFailures="
                            + entry.failures
                            + ", notFoundFailures="
                            + entry.notFoundFailures
                            + ". The latest retained checkpoint owns the tasks; repair the cause and use a bounded restart strategy.");
        }
        entry.dueNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(backoff);
    }

    private void checkRunning() throws IOException, InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Cloud Tasks checkpoint commit interrupted.");
        }
        if (closed) {
            throw new IOException("Cloud Tasks checkpoint committer is closed.");
        }
    }

    @Override
    public void close() throws Exception {
        synchronized (sendLock) {
            if (closed) {
                return;
            }
            closed = true;
            if (committingThread != null && committingThread != Thread.currentThread()) {
                committingThread.interrupt();
            }
        }
        creator.close();
    }

    private static final class Entry {
        private final CommitRequest<CloudTasksCommittable> request;
        private final CreateTaskRequest create;
        private long dueNanos = System.nanoTime();
        private boolean counted;
        private int failures;
        private int notFoundFailures;
        private Attempt attempt;

        private Entry(CommitRequest<CloudTasksCommittable> request, CreateTaskRequest create) {
            this.request = request;
            this.create = create;
        }
    }

    private static final class Attempt {
        private final Entry entry;
        private final Deadline deadline;
        private ApiFuture<Task> future;

        private Attempt(Entry entry, Deadline deadline) {
            this.entry = entry;
            this.deadline = deadline;
        }
    }

    private static final class Completion {
        private final Attempt attempt;
        private final Throwable failure;

        private Completion(Attempt attempt, Throwable failure) {
            this.attempt = attempt;
            this.failure = failure;
        }
    }
}
