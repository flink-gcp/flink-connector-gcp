/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.cloudtasks.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.function.ThrowingRunnable;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.tasks.v2.CreateTaskRequest;
import com.google.cloud.tasks.v2.Task;
import com.google.cloud.tasks.v2.TaskName;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.base.rpc.StatusCodes;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSinkConfig;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksWriterOptions;
import io.github.flink.gcp.connector.cloudtasks.sink.FailedTask;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;
import io.github.flink.gcp.connector.cloudtasks.sink.TaskIdExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * At-least-once writer creating one Cloud Tasks task per record.
 *
 * <h2>Threading model</h2>
 *
 * <p>All mutable state — the in-flight counter, the parked retries and the captured asynchronous
 * error — is touched only on the task thread. Creation completion callbacks do not mutate state
 * directly; they re-dispatch onto the {@link MailboxExecutor}, whose mails run on the task thread
 * inside {@link MailboxExecutor#yield()} calls. This is the model the Pub/Sub sink's writer uses.
 *
 * <h2>Delivery guarantees and state</h2>
 *
 * <p>The writer is stateless by design: it stores nothing in Flink state. {@link #flush(boolean)}
 * runs at every checkpoint barrier and waits for every outstanding creation — including those
 * waiting out a retry backoff — so a successful checkpoint means Cloud Tasks has durably accepted
 * every record up to the barrier (the service returns {@code OK} only once a task has been written
 * to its storage), and discarding operator state can never lose sink-buffered records.
 * Checkpointing must be enabled in streaming jobs; without it {@code flush()} never runs mid-stream
 * and outstanding creations are lost on failure. Batch execution is covered by the end-of-input
 * flush.
 *
 * <h2>Retries</h2>
 *
 * <p>Retrying is this sink's own responsibility: {@code CloudTasksStubSettings} gives {@code
 * CreateTask} an empty retryable-code set, as it does every mutating method. A failed creation is
 * therefore parked with a due time and re-dispatched from a later {@link #write} or {@link #flush}
 * — {@code UNAVAILABLE}, {@code DEADLINE_EXCEEDED} and {@code RESOURCE_EXHAUSTED} on the main
 * budget, {@code NOT_FOUND} on a shorter one of its own, everything else terminal. Under unnamed
 * tasks a retry after {@code DEADLINE_EXCEEDED} may duplicate the task, which is what at-least-once
 * prefers to losing it; naming removes the ambiguity.
 *
 * <p>The number of outstanding creations — in flight plus parked — is capped ({@code
 * CloudTasksWriterOptions.maxInFlightTasks}, default 1000); at the cap {@link #write} yields to the
 * mailbox until completions bring the count down, bounding sink memory between checkpoints.
 *
 * <h2>Per-task failures</h2>
 *
 * <p>Three data-shaped failures are handed to the configured {@link FailureHandler} instead of
 * failing the job outright: a record the serializer rejects, a {@link TaskIdExtractor} that throws,
 * and a creation the service rejects with {@code INVALID_ARGUMENT} (a malformed target URL, an
 * oversized body, a header the service refuses). Each concerns one record, and re-sending the same
 * bytes cannot succeed. The handler drops the task by returning and fails the job by throwing; the
 * default {@code failJob()} throws, which is why the failures it does <em>not</em> cover matter —
 * an exhausted retry budget stays a job failure, so no drop policy can quietly discard an outage's
 * backlog. A handler failing inside a completion callback is captured into {@link #asyncError} like
 * any other terminal failure, because a mailbox mail cannot throw a checked exception at its
 * caller.
 *
 * <h2>Task naming</h2>
 *
 * <p>Without a {@link TaskIdExtractor} the writer creates unnamed tasks and a replayed record calls
 * the endpoint twice. With one, the task name is composed from the resolved queue and the SHA-256
 * digest of the extracted key — Google documents sequential task ids as raising latency and error
 * rates — and an {@code ALREADY_EXISTS} response means Cloud Tasks still remembers the id, which is
 * exactly the deduplication that was asked for and therefore counts as success.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public class CloudTasksWriter<T> implements SinkWriter<T> {

    private static final Logger LOG = LoggerFactory.getLogger(CloudTasksWriter.class);

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private static final String COMPLETION_MAIL = "Complete a Cloud Tasks task creation";
    private static final String FAILURE_MAIL = "Fail a Cloud Tasks task creation";

    private final CloudTasksSinkConfig<T> config;
    private final TaskCreator creator;
    private final MailboxExecutor mailboxExecutor;
    private final TimeSource timeSource;
    private final int maxInFlightTasks;
    private final RetrySchedule retrySchedule;
    private final RetrySchedule notFoundSchedule;
    @Nullable private final TaskIdExtractor<? super T> taskIdExtractor;
    private final FailureHandler<? super FailedTask> failedTaskHandler;

    /** SHA-256 of the extracted keys; task-thread only, {@code null} when tasks are unnamed. */
    @Nullable private final MessageDigest digest;

    /** Number of creations not yet completed; touched only on the task thread. */
    private int inFlight;

    /**
     * Mail shared by every successful creation, so the success path allocates no mail per record.
     */
    private final ThrowingRunnable<Exception> completionMail = () -> inFlight--;

    /**
     * Creations waiting out a retry backoff, earliest due first; touched only on the task thread.
     */
    private final PriorityQueue<PendingCreate> parked =
            new PriorityQueue<>(Comparator.comparingLong(pending -> pending.dueAtMillis));

    /**
     * First terminal creation failure; set and read only on the task thread (failure callbacks
     * re-dispatch through the mailbox).
     */
    private IOException asyncError;

    /**
     * Creates the writer.
     *
     * @param config the sink configuration
     * @param creator the task creator; closed with the writer
     * @param mailboxExecutor the task mailbox, used to run creation completions on the task thread
     */
    public CloudTasksWriter(
            CloudTasksSinkConfig<T> config, TaskCreator creator, MailboxExecutor mailboxExecutor) {
        this(config, creator, mailboxExecutor, TimeSource.SYSTEM);
    }

    @VisibleForTesting
    CloudTasksWriter(
            CloudTasksSinkConfig<T> config,
            TaskCreator creator,
            MailboxExecutor mailboxExecutor,
            TimeSource timeSource) {
        this.config = config;
        this.creator = creator;
        this.mailboxExecutor = mailboxExecutor;
        this.timeSource = timeSource;
        CloudTasksWriterOptions options = config.getWriterOptions();
        this.maxInFlightTasks = options.getMaxInFlightTasks();
        this.retrySchedule = options.toRetrySchedule();
        this.notFoundSchedule = options.toNotFoundRetrySchedule();
        this.taskIdExtractor = config.getTaskIdExtractor();
        this.digest = taskIdExtractor == null ? null : sha256();
        this.failedTaskHandler = config.getFailedTaskHandler();
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            // Every Java platform is required to implement SHA-256.
            throw new IllegalStateException("SHA-256 is not available in this JVM.", e);
        }
    }

    @Override
    public void write(T element, Context context) throws IOException, InterruptedException {
        checkAsyncError();
        dispatchDueRetries();
        QueueDestination destination = config.getDestinationResolver().resolve(element, context);
        if (destination == null) {
            throw new IOException("The destination resolver returned null for a record.");
        }
        Task task;
        try {
            task = config.getSerializer().serialize(element);
        } catch (IOException | RuntimeException e) {
            // The record never became a task, so there is nothing to carry but the destination:
            // FailedTask.getPayloadBytes() is null, as the shared contract prescribes. The
            // description leaves the queue to describeDestination(), as routeFailedTask does.
            failedTaskHandler.handle(
                    FailedTask.of(destination, null, "The record could not be serialized.", e));
            return;
        }
        if (!task.getName().isEmpty()) {
            throw new IOException(
                    "The serializer returned a task already named '"
                            + task.getName()
                            + "' for Cloud Tasks queue "
                            + destination
                            + ". Task names are composed by the sink from taskIdExtractor(...), so"
                            + " that every id it writes is a hashed one; a serializer must leave"
                            + " the name unset.");
        }
        if (taskIdExtractor != null) {
            String key;
            try {
                key = taskIdExtractor.extractTaskId(element);
            } catch (RuntimeException e) {
                failedTaskHandler.handle(
                        FailedTask.of(
                                destination,
                                task,
                                "The task id extractor failed for the record.",
                                e));
                return;
            }
            task = task.toBuilder().setName(taskName(destination, key)).build();
        }
        awaitCapacity();
        dispatch(
                destination,
                CreateTaskRequest.newBuilder()
                        .setParent(destination.toQueuePath())
                        .setTask(task)
                        .build(),
                null);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        while (true) {
            checkAsyncError();
            dispatchDueRetries();
            if (inFlight > 0) {
                mailboxExecutor.yield();
                continue;
            }
            if (parked.isEmpty()) {
                break;
            }
            sleepUntilDue();
        }
        // After the loop, never inside it: the failures that reach the handler are discovered by
        // the drain — and by the re-dispatch of parked retries, which the loop also waits out — so
        // flushing earlier would checkpoint past dead letters still to come.
        failedTaskHandler.flush();
    }

    @Override
    public void close() throws Exception {
        // No explicit flush here: on success Flink calls flush(true) before close. On the failure
        // path the writer creates nothing further, and creations parked for retry are dropped with
        // it — they are not covered by a completed checkpoint, so the restart replays their
        // records.
        try {
            // Through closeAll, so the handler is closed even when the creator's shutdown throws:
            // the lifecycle contract promises close on the failure path too.
            IOUtils.closeAll(creator, failedTaskHandler::close);
        } finally {
            parked.clear();
        }
    }

    /**
     * Composes the task name from the resolved queue and the SHA-256 digest of the extracted key.
     * The digest is 64 characters from {@code [0-9a-f]}, well inside the 500-character limit for a
     * task id.
     *
     * <p>A missing key fails the job rather than reaching the failure handler: {@code
     * taskIdExtractor(...)} is set for the whole stream, so an extractor with no key to return
     * fails every record alike, and dropping those would leave an empty queue under a green job. An
     * extractor that <em>throws</em> is the opposite case — that is per-record, and it is routed.
     */
    private String taskName(QueueDestination destination, @Nullable String key) throws IOException {
        if (key == null || key.isEmpty()) {
            throw new IOException(
                    "The task id extractor returned "
                            + (key == null ? "null" : "an empty key")
                            + " for a record bound for Cloud Tasks queue "
                            + destination
                            + "; every record needs a deduplication key once taskIdExtractor(...)"
                            + " is set, since falling back to an unnamed task would silently drop"
                            + " deduplication for that record.");
        }
        return TaskName.format(
                destination.getProject(),
                destination.getLocation(),
                destination.getQueue(),
                hash(key));
    }

    /** Returns the lowercase hexadecimal SHA-256 digest of the key. */
    private String hash(String key) {
        // MessageDigest#digest resets the instance, so it is reusable across records.
        byte[] bytes = digest.digest(key.getBytes(StandardCharsets.UTF_8));
        char[] hex = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            hex[i * 2] = HEX[(bytes[i] >> 4) & 0xf];
            hex[i * 2 + 1] = HEX[bytes[i] & 0xf];
        }
        return new String(hex);
    }

    /**
     * Creates the task, counts it in flight and registers its completion callback. {@code pending}
     * carries the retry budgets already spent, and is {@code null} for a record's first attempt.
     */
    private void dispatch(
            QueueDestination destination,
            CreateTaskRequest request,
            @Nullable PendingCreate pending)
            throws IOException {
        ApiFuture<Task> future;
        try {
            future = creator.createTask(request);
        } catch (RuntimeException e) {
            throw new IOException(
                    "Failed to create a task in Cloud Tasks queue " + request.getParent() + ".", e);
        }
        inFlight++;
        ApiFutures.addCallback(
                future, new CreateCallback(destination, request, pending), Runnable::run);
    }

    /** Re-dispatches every parked creation whose backoff has elapsed. */
    private void dispatchDueRetries() throws IOException {
        if (parked.isEmpty()) {
            return;
        }
        long now = timeSource.currentTimeMillis();
        while (!parked.isEmpty() && parked.peek().dueAtMillis <= now) {
            PendingCreate pending = parked.poll();
            dispatch(pending.destination, pending.request, pending);
        }
    }

    /**
     * Blocks until the outstanding creations — in flight plus parked — are below the cap, running
     * mailbox mails (creation completions) while waiting and surfacing any captured failure before
     * the caller creates another task. With every creation parked rather than in flight there is no
     * mail to wait for, so the wait is the earliest backoff instead.
     */
    private void awaitCapacity() throws IOException, InterruptedException {
        while (inFlight + parked.size() >= maxInFlightTasks) {
            checkAsyncError();
            if (inFlight > 0) {
                mailboxExecutor.yield();
            } else {
                sleepUntilDue();
            }
            dispatchDueRetries();
        }
        checkAsyncError();
    }

    private void sleepUntilDue() throws InterruptedException {
        PendingCreate head = parked.peek();
        if (head == null) {
            return;
        }
        long waitMs = head.dueAtMillis - timeSource.currentTimeMillis();
        if (waitMs > 0) {
            timeSource.sleep(waitMs);
        }
    }

    private void checkAsyncError() throws IOException {
        if (asyncError != null) {
            throw asyncError;
        }
    }

    /** Task-thread handler for a failed creation, run as a mailbox mail. */
    private void onCreateFailed(
            QueueDestination destination,
            CreateTaskRequest request,
            @Nullable PendingCreate pending,
            Throwable throwable) {
        inFlight--;
        StatusCode.Code code = statusCode(throwable);
        boolean named = !request.getTask().getName().isEmpty();
        if (code == StatusCode.Code.ALREADY_EXISTS && named) {
            // The deduplication that naming asked for: Cloud Tasks still remembers this id, so the
            // task was created by an earlier attempt or an earlier run and must not be created
            // again.
            LOG.debug(
                    "Cloud Tasks already holds task {}; treating the duplicate create as success.",
                    request.getTask().getName());
            return;
        }
        if (code == StatusCode.Code.INVALID_ARGUMENT) {
            // Before the asyncError check below, deliberately: the writer is about to fail either
            // way, but this task really did fail terminally, and a dead-letter destination missing
            // it is worse than one holding a task a replay will produce again — the guarantee is
            // at-least-once.
            routeFailedTask(destination, request.getTask(), throwable);
            return;
        }
        if (asyncError != null) {
            // An earlier failure already fails the job; nothing is gained by retrying this one.
            return;
        }
        PendingCreate entry = pending != null ? pending : new PendingCreate(destination, request);
        if (code == StatusCode.Code.UNAVAILABLE
                || code == StatusCode.Code.DEADLINE_EXCEEDED
                || code == StatusCode.Code.RESOURCE_EXHAUSTED) {
            if (++entry.attempts < retrySchedule.maxAttempts()) {
                park(entry, retrySchedule.backoffMs(entry.attempts));
            } else {
                asyncError = exhausted(request, entry.attempts, code, throwable);
            }
            return;
        }
        if (code == StatusCode.Code.NOT_FOUND) {
            if (++entry.notFoundAttempts < notFoundSchedule.maxAttempts()) {
                park(entry, notFoundSchedule.backoffMs(entry.notFoundAttempts));
            } else {
                asyncError =
                        new IOException(
                                "Creating a task in Cloud Tasks queue "
                                        + request.getParent()
                                        + " kept failing with NOT_FOUND after "
                                        + entry.notFoundAttempts
                                        + " attempt(s). Either the queue does not exist — the sink"
                                        + " never creates one, because an auto-created queue would"
                                        + " carry default rate limits instead of the intended"
                                        + " pacing — or it is still re-activating after 30 days of"
                                        + " idleness, which takes longer than this budget covers"
                                        + " and is left to the job's restart strategy.",
                                throwable);
            }
            return;
        }
        asyncError =
                new IOException(
                        "Creating a task in Cloud Tasks queue "
                                + request.getParent()
                                + " failed"
                                + (code == StatusCode.Code.ALREADY_EXISTS
                                        ? " with ALREADY_EXISTS even though the task carries no"
                                                + " name, which should be unreachable"
                                        : "")
                                + ".",
                        throwable);
    }

    /**
     * Hands a task-level creation failure to the configured handler. Runs as a mailbox mail, so a
     * handler that fails the job cannot throw at a caller: its failure is captured into {@link
     * #asyncError} and rethrown from the next {@link #write} or {@link #flush}, exactly as a
     * terminal creation failure is. First failure wins, as everywhere else here.
     *
     * <p>The description does not name the queue: every reader of it reaches the element's {@code
     * describeDestination()} too — the built-in handlers compose the two — so naming it here would
     * put the queue in the sentence twice, in two spellings.
     */
    private void routeFailedTask(QueueDestination destination, Task task, Throwable throwable) {
        try {
            failedTaskHandler.handle(
                    FailedTask.of(
                            destination,
                            task,
                            "Cloud Tasks rejected the task with INVALID_ARGUMENT.",
                            throwable));
        } catch (IOException | RuntimeException e) {
            if (asyncError == null) {
                asyncError =
                        e instanceof IOException
                                ? (IOException) e
                                : new IOException(
                                        "The failed-task handler failed for Cloud Tasks queue "
                                                + destination
                                                + ".",
                                        e);
            }
        }
    }

    private void park(PendingCreate entry, long backoffMs) {
        entry.dueAtMillis = timeSource.currentTimeMillis() + backoffMs;
        parked.add(entry);
        LOG.debug(
                "Creating a task in Cloud Tasks queue {} failed; retrying in {} ms.",
                entry.request.getParent(),
                backoffMs);
    }

    private static IOException exhausted(
            CreateTaskRequest request, int attempts, StatusCode.Code code, Throwable throwable) {
        return new IOException(
                "Creating a task in Cloud Tasks queue "
                        + request.getParent()
                        + " kept failing with "
                        + code
                        + " after "
                        + attempts
                        + " attempt(s).",
                throwable);
    }

    /**
     * Returns the status code the failure carries — the first element of the cause chain {@link
     * StatusCodes#codeOf} can classify — or {@code null} when no element carries one, which is
     * treated as terminal.
     */
    @Nullable
    private static StatusCode.Code statusCode(Throwable throwable) {
        return ExceptionUtils.findThrowable(throwable, t -> StatusCodes.codeOf(t) != null)
                .map(StatusCodes::codeOf)
                .orElse(null);
    }

    @VisibleForTesting
    int getInFlightTasks() {
        return inFlight;
    }

    @VisibleForTesting
    int getParkedTasks() {
        return parked.size();
    }

    /** A creation waiting out a retry backoff, with the budgets it has already spent. */
    private static final class PendingCreate {

        private final QueueDestination destination;
        private final CreateTaskRequest request;

        private int attempts;
        private int notFoundAttempts;
        private long dueAtMillis;

        private PendingCreate(QueueDestination destination, CreateTaskRequest request) {
            this.destination = destination;
            this.request = request;
        }
    }

    /** Re-dispatches creation completions onto the mailbox so state stays task-thread-only. */
    private final class CreateCallback implements ApiFutureCallback<Task> {

        private final QueueDestination destination;
        private final CreateTaskRequest request;
        @Nullable private final PendingCreate pending;

        private CreateCallback(
                QueueDestination destination,
                CreateTaskRequest request,
                @Nullable PendingCreate pending) {
            this.destination = destination;
            this.request = request;
            this.pending = pending;
        }

        @Override
        public void onSuccess(Task task) {
            mailboxExecutor.execute(completionMail, COMPLETION_MAIL);
        }

        @Override
        public void onFailure(Throwable throwable) {
            mailboxExecutor.execute(
                    () -> onCreateFailed(destination, request, pending, throwable), FAILURE_MAIL);
        }
    }
}
