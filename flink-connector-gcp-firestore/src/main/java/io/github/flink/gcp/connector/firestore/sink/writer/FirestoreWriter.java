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

package io.github.flink.gcp.connector.firestore.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.function.ThrowingRunnable;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.firestore.WriteResult;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.sink.FailedWrite;
import io.github.flink.gcp.connector.firestore.sink.FirestoreSinkConfig;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWrite;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWriterOptions;
import io.github.flink.gcp.connector.firestore.sink.PreconditionFailurePolicy;
import io.github.flink.gcp.connector.firestore.sink.serializer.FirestoreWriteSerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;

/**
 * At-least-once writer applying one document write per record through the client library's {@code
 * BulkWriter}.
 *
 * <h2>Threading model</h2>
 *
 * <p>All logical state — the in-flight counters, the parked writes and the captured asynchronous
 * error — changes only on the task thread. Write completion callbacks do not mutate it; they
 * re-dispatch onto the {@link MailboxExecutor}, whose mails run on the task thread inside the
 * writer's own waits. This is the model the Bigtable and Pub/Sub sinks' writers use. One signal
 * crosses threads: {@code lastCompletionNanos}, stamped by the callbacks and by the retry listener
 * on the library's threads so a wait can tell a stalled client from a busy task thread. The waits
 * run {@link MailboxExecutor#tryYield()} and park rather than calling {@link
 * MailboxExecutor#yield()}, which is what lets them notice a stall at all, and why they read the
 * interrupt flag themselves ({@code docs/adr/0078}).
 *
 * <h2>Delivery guarantees and state</h2>
 *
 * <p>The writer is stateless by design: it stores nothing in Flink state. {@link #flush(boolean)}
 * runs at every checkpoint barrier, sends what the library holds and waits until every write is
 * answered, so a successful checkpoint means Firestore has applied every record up to the barrier —
 * other than those the serializer skipped by returning {@code null}. That guarantee assumes the
 * default {@code failJob()} policy; what a successful checkpoint means under a dropping policy is
 * stated once on {@link FailureHandler}.
 *
 * <h2>Retries</h2>
 *
 * <p>Retrying is the library's: {@code BulkWriter} re-sends a write refused with a transient
 * status, and {@link BulkWriterRetryPolicy} only decides how many times. A failure that reaches
 * this writer is one the library gave up on.
 *
 * <h2>Per-write failures</h2>
 *
 * <p>{@link FirestoreErrorClassifier} decides which failures are the record's and reach the failure
 * handler. An {@code INVALID_ARGUMENT} is <b>confirmed alone before it is routed</b> (ADR-0045):
 * the library reports a failure of a whole {@code BatchWrite} request against every write in it, so
 * the writer parks such a write and, before the next record, re-sends each parked write as the only
 * write of its request. Only a refusal that repeats alone is routed; a write that succeeds alone is
 * applied.
 *
 * <p>A create whose id the write drew ({@link FirestoreWrite#add}) and that Firestore refuses with
 * {@code ALREADY_EXISTS} is <b>sent again under a new id</b>, never routed: the id names an
 * existing document, which the create left in place, or the library retried a create that was
 * applied but whose answer was lost, which this writer cannot tell apart without a read. The second
 * case leaves the document twice. The writer parks such a write and sends it again before the next
 * record, as it does a write it confirms alone; {@value #MAX_ID_DRAWS} drawn ids refused in a row
 * for one record fail the job, since chance does not explain that.
 *
 * <h2>Two defects of the client library this writer works around</h2>
 *
 * <p>Both measured against google-cloud-firestore 3.46.0 on the emulator, 2026-09-27, one run, and
 * pinned by {@code BulkWriterDefectsITCase} so that a library release that fixes either fails that
 * test instead of leaving the workaround in place unexamined.
 *
 * <ul>
 *   <li><b>A failed write keeps its pending-operation slot forever.</b> {@code BulkWriter} frees a
 *       slot only through a handler for {@code ApiException}, and the {@code BulkWriterException} a
 *       failed write completes with is not one. Past 500 occupied slots the library queues every
 *       further write without sending it, so after 500 failures in one {@code BulkWriter}'s life a
 *       write never completes. The writer counts the failures of its current {@code BulkWriter} and
 *       replaces it, once nothing is in flight, before a submission could reach the ceiling. A
 *       fresh {@code BulkWriter} starts its throttle's ramp-up again, which {@code
 *       bulkWritersReplaced} makes visible.
 *   <li><b>A synchronous refusal corrupts the request it happened in.</b> A value the library
 *       cannot encode makes it throw after it has already queued the operation, and the rest of
 *       that request is then answered out of step — one write was applied while its future never
 *       completed. {@link FirestoreWrite} accepts only values the library encodes, and a
 *       synchronous refusal that still reaches this writer fails the job rather than being routed.
 * </ul>
 *
 * <h2>Request size</h2>
 *
 * <p>The library sends batches of up to 20 writes without looking at their size, and Firestore
 * limits a request to 10 MiB. So the writer asks the library to send what it holds before the
 * {@link DocumentSizeEstimator} bound on the writes since the last send would pass {@value
 * #REQUEST_BYTE_BUDGET} bytes: a batch never spans a send, so no first-attempt batch grows past
 * that. A write the library retries rejoins a later batch of at most ten, outside that count, so a
 * request carrying retries can still pass the limit; if the service then refuses it whole, its
 * writes are confirmed one at a time, which is slower but loses nothing.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public class FirestoreWriter<T> implements SinkWriter<T> {

    private static final Logger LOG = LoggerFactory.getLogger(FirestoreWriter.class);

    /**
     * The library's pending-operation ceiling, {@link
     * FirestoreWriterOptions#MAX_IN_FLIGHT_WRITES_LIMIT}: past it the library queues further writes
     * without sending them. A failed write keeps one of these slots for the rest of its {@code
     * BulkWriter}'s life; see the class documentation.
     */
    static final int PENDING_OPERATION_LIMIT = FirestoreWriterOptions.MAX_IN_FLIGHT_WRITES_LIMIT;

    /**
     * The bound on the bytes of writes after which the writer sends what the library holds: 9 MiB,
     * a mebibyte under Firestore's 10 MiB request limit, for the request's own envelope.
     */
    static final long REQUEST_BYTE_BUDGET = 9L * 1024 * 1024;

    /**
     * How long a wait may go without the library answering anything before it says so.
     *
     * <p>Derived from the library's own bounds rather than chosen: one {@code BatchWrite} call is
     * bounded by a 60-second total timeout by default ({@code FirestoreStubSettings},
     * google-cloud-firestore 3.46.0), and a retry waits at most a minute of backoff before it is
     * sent, so a write the library keeps retrying is answered at least every two minutes, and three
     * minutes of silence is past anything a healthy write does at those settings. A longer {@code
     * retryTotalTimeout} lengthens the gap, and the warning may then name a write that is still
     * being retried; it is a log line, not a verdict. The writer does not fail a stalled wait: a
     * streaming job's checkpoint timeout ends one, and the library can stall only through its own
     * defects (a rate below its batch size, which the options refuse, or a wall clock stepping
     * backwards inside its rate limiter).
     */
    private static final long STALL_WARN_AFTER_NANOS = Duration.ofMinutes(3).toNanos();

    /**
     * How long a wait parks when the mailbox has nothing to run: the throughput cost of not using
     * the blocking {@link MailboxExecutor#yield()}, paid only while nothing is arriving.
     */
    private static final long POLL_INTERVAL_NANOS = Duration.ofMillis(1).toNanos();

    /**
     * How many drawn ids one record may have refused with {@code ALREADY_EXISTS} before the job
     * fails. A collision of ids with about 119 bits of randomness, or a lost answer, ten times in a
     * row for one record is not chance; a service that refuses every fresh id would otherwise be
     * retried forever.
     */
    static final int MAX_ID_DRAWS = 10;

    /** {@link #awaitProgress} ran a mail rather than finding the mailbox empty. */
    private static final long RAN_A_MAIL = -1L;

    private static final String COMPLETION_MAIL = "Complete a Firestore write";
    private static final String FAILURE_MAIL = "Fail a Firestore write";

    private final DatabaseDestination database;
    private final FirestoreWriteSerializationSchema<? super T> serializer;
    private final FailureHandler<? super FailedWrite> failedWriteHandler;
    private final PreconditionFailurePolicy preconditionFailurePolicy;
    private final MailboxExecutor mailboxExecutor;
    private final FirestoreWriterMetrics metrics;
    private final DocumentSizeEstimator sizeEstimator;
    private final LongSupplier nanoClock;
    private final long stallWarnAfterNanos;
    private final int maxInFlightWrites;
    private final long maxInFlightBytes;
    private final int maxConsecutiveRejections;
    private final FirestoreDatabaseAccess access;

    /** Writes handed to the library and not yet answered; task thread only. */
    private int inFlightWrites;

    /** Their estimated size; task thread only. */
    private long inFlightBytes;

    /**
     * Writes the current {@code BulkWriter} reported failed, each holding one of its pending slots
     * for good; task thread only.
     */
    private int failuresOnBulkWriter;

    /** Estimated bytes handed to the library since it was last asked to send; task thread only. */
    private long bytesSinceSend;

    /** Writes awaiting a solo re-send to confirm an {@code INVALID_ARGUMENT}; task thread only. */
    private final Deque<ParkedWrite> pendingIsolation = new ArrayDeque<>();

    /**
     * Drawn-id creates refused with {@code ALREADY_EXISTS}, awaiting a re-send under a new id; task
     * thread only.
     */
    private final Deque<ParkedWrite> pendingRedraws = new ArrayDeque<>();

    /** Confirmed rejections since the last applied write; task thread only. */
    private int consecutiveRejections;

    /** First terminal failure; set and read only on the task thread. */
    private IOException asyncError;

    /** When the library last answered anything, a write or a failed attempt; any thread. */
    private volatile long lastCompletionNanos;

    /** When the last stall warning was logged; task thread only. */
    private long lastStallWarnNanos;

    /**
     * Creates the writer and opens its database access.
     *
     * @param config the sink configuration
     * @param accessFactory opens the database access the writer sends through
     * @param mailboxExecutor the task mailbox, used to run write completions on the task thread
     * @param metricGroup the writer's metric group
     * @throws IOException if the database access cannot be opened
     */
    public FirestoreWriter(
            FirestoreSinkConfig<T> config,
            FirestoreDatabaseAccessFactory accessFactory,
            MailboxExecutor mailboxExecutor,
            SinkWriterMetricGroup metricGroup)
            throws IOException {
        this(
                config,
                accessFactory,
                mailboxExecutor,
                metricGroup,
                System::nanoTime,
                STALL_WARN_AFTER_NANOS);
    }

    @VisibleForTesting
    FirestoreWriter(
            FirestoreSinkConfig<T> config,
            FirestoreDatabaseAccessFactory accessFactory,
            MailboxExecutor mailboxExecutor,
            SinkWriterMetricGroup metricGroup,
            LongSupplier nanoClock,
            long stallWarnAfterNanos)
            throws IOException {
        this.database = config.getDatabase();
        this.serializer = config.getSerializer();
        this.failedWriteHandler = config.getFailedWriteHandler();
        this.preconditionFailurePolicy = config.getPreconditionFailurePolicy();
        this.mailboxExecutor = mailboxExecutor;
        this.nanoClock = nanoClock;
        this.stallWarnAfterNanos = stallWarnAfterNanos;
        FirestoreWriterOptions options = config.getWriterOptions();
        // Re-checked, because a Java-deserialized options object never passed through its builder.
        this.maxInFlightWrites = options.getMaxInFlightWrites();
        if (maxInFlightWrites <= 0 || maxInFlightWrites > PENDING_OPERATION_LIMIT) {
            throw new IllegalArgumentException(
                    "maxInFlightWrites must be between 1 and "
                            + PENDING_OPERATION_LIMIT
                            + ", was "
                            + maxInFlightWrites);
        }
        this.maxInFlightBytes = options.getMaxInFlightBytes();
        if (maxInFlightBytes <= 0) {
            throw new IllegalArgumentException(
                    "maxInFlightBytes must be positive, was " + maxInFlightBytes);
        }
        this.maxConsecutiveRejections = options.getMaxConsecutiveRejections();
        this.sizeEstimator = new DocumentSizeEstimator(database);
        this.metrics = new FirestoreWriterMetrics(metricGroup);
        this.metrics.bindWriterState(
                (Gauge<Integer>) this::getInFlightWrites,
                (Gauge<Long>) this::getInFlightBytes,
                (Gauge<Integer>) this::getParkedWrites);
        this.lastCompletionNanos = nanoClock.getAsLong();
        this.lastStallWarnNanos = lastCompletionNanos - stallWarnAfterNanos;
        // Last, so that nothing after it can throw and strand the client it opens.
        this.access =
                accessFactory.create(
                        new BulkWriterRetryPolicy(
                                options.getWriteMaxAttempts(),
                                new BulkWriterRetryPolicy.Observer() {
                                    @Override
                                    public void attemptFailed() {
                                        lastCompletionNanos = nanoClock.getAsLong();
                                    }

                                    @Override
                                    public void retrying() {
                                        metrics.writeRetried();
                                    }
                                }));
    }

    @Override
    public void write(T element, Context context) throws IOException, InterruptedException {
        checkAsyncError();
        // Before this record is serialized, and this is what bounds the park: parking happens in
        // completion mails, which only answer writes already in flight, and every park releases
        // one write from the in-flight counters — so between two records at most
        // maxInFlightWrites can accumulate.
        if (!pendingRedraws.isEmpty()) {
            sendRedraws();
        }
        if (!pendingIsolation.isEmpty()) {
            runIsolationPass();
        }
        FirestoreWrite write;
        try {
            write = serializer.serialize(element, context);
        } catch (IOException | RuntimeException e) {
            // Handled on the task thread, so a handler that fails the job throws at the caller
            // directly. Not counted under errorClass: a serialization failure carries no status.
            metrics.writeFailed();
            failedWriteHandler.handle(
                    FailedWrite.of(database, null, "The record could not be serialized.", e));
            return;
        }
        if (write == null) {
            // Skip by contract, not a failure; counted, because nothing else reports it.
            metrics.recordSkipped();
            return;
        }
        int estimatedSize = sizeEstimator.estimate(write);
        awaitCapacity();
        submit(write, estimatedSize, true, false, 1);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        checkAsyncError();
        // A pass's solo re-sends confirm or route every parked write, so one iteration normally
        // ends with nothing parked; the loop is what makes "a completed checkpoint leaves nothing
        // parked" true regardless. A redraw parks again only when its new id is refused too, and a
        // solo re-send of a drawn-id create can park one, so both parks are looped on.
        do {
            if (!pendingRedraws.isEmpty()) {
                sendRedraws();
            }
            sendOutstanding();
            drainInFlight();
            if (!pendingIsolation.isEmpty()) {
                runIsolationPass();
            }
        } while (!pendingIsolation.isEmpty() || !pendingRedraws.isEmpty());
        // After the loop, never inside it: the drain is what discovers this checkpoint's failures,
        // and the pass is what turns the batched ones among them into dead letters.
        failedWriteHandler.flush();
    }

    @Override
    public void close() throws Exception {
        // No flush here: on success Flink calls flush(true) before close, and on the failure path
        // the writer sends nothing further. The access's close shuts the library's executor down,
        // which abandons whatever it still held; no completed checkpoint covers those writes, so
        // the restart replays their records. The counters back gauges a reporter may still sample,
        // and no mail will decrement them again, so they are zeroed first.
        inFlightWrites = 0;
        inFlightBytes = 0;
        pendingIsolation.clear();
        pendingRedraws.clear();
        Closers.closeAll(access, failedWriteHandler::close);
    }

    /**
     * Hands one write to the library and counts it in flight.
     *
     * @param firstAttempt whether this is the record's first submission, the only one {@code
     *     numRecordsSend} counts
     * @param solo whether this is a solo re-send, whose verdict is the write's own
     * @param idDraws how many ids the record has drawn, this write's included
     */
    private void submit(
            FirestoreWrite write,
            int estimatedSize,
            boolean firstAttempt,
            boolean solo,
            int idDraws)
            throws IOException, InterruptedException {
        ensureBulkWriterHasRoom();
        if (bytesSinceSend > 0 && bytesSinceSend + estimatedSize > REQUEST_BYTE_BUDGET) {
            sendOutstanding();
        }
        ApiFuture<WriteResult> future;
        try {
            future = access.submit(write);
        } catch (RuntimeException e) {
            throw new IOException(
                    "The Firestore client refused a write to "
                            + describe(write)
                            + " synchronously. Its BulkWriter may now hold a write it will never"
                            + " answer, so the job fails rather than routing the record.",
                    e);
        }
        // Counted only once the library accepted it: a synchronous throw registers no callback,
        // so nothing would ever release it.
        inFlightWrites++;
        inFlightBytes += estimatedSize;
        bytesSinceSend += estimatedSize;
        if (firstAttempt) {
            metrics.writeSent(estimatedSize);
        }
        ApiFutures.addCallback(
                future, new WriteCallback(write, estimatedSize, solo, idDraws), Runnable::run);
    }

    /**
     * Replaces the {@code BulkWriter} before a submission could reach its pending-operation
     * ceiling, counting the slots its failed writes still hold.
     *
     * <p>Only with nothing in flight: the replacement is dropped rather than closed, and a write
     * still in flight on it would complete into a count that no longer describes the current {@code
     * BulkWriter}. The drain runs failure mails, which may park writes and so add to the count this
     * reset clears — both belong to the {@code BulkWriter} being dropped.
     */
    private void ensureBulkWriterHasRoom() throws IOException, InterruptedException {
        if (failuresOnBulkWriter + inFlightWrites < PENDING_OPERATION_LIMIT) {
            return;
        }
        sendOutstanding();
        drainInFlight();
        access.replaceBulkWriter();
        LOG.info(
                "Replaced the Firestore BulkWriter for {} after {} failed write(s): the client"
                        + " library never releases the pending-operation slot of a failed write, and"
                        + " would stop sending at {}. The new BulkWriter starts its throttle's"
                        + " ramp-up again.",
                database,
                failuresOnBulkWriter,
                PENDING_OPERATION_LIMIT);
        failuresOnBulkWriter = 0;
        metrics.bulkWriterReplaced();
    }

    /**
     * Gives every parked write its own verdict, by re-sending each as the only write of its
     * request.
     *
     * <p>The opening send and drain are what make the re-sends solo: once everything in flight is
     * answered the library holds nothing, so a write submitted and sent at once travels alone.
     * Consumed with {@code poll()} rather than iterated: the opening drain runs mails that may park
     * further writes, and the loop picks those up. Nothing parks here during the solo drains,
     * because a solo verdict is routed, made fatal, applied or, for a drawn-id create refused with
     * {@code ALREADY_EXISTS}, handed to the redraw park, never parked here again — so the loop is
     * bounded by the park's size and raises, rather than spins, if that invariant is ever broken.
     *
     * <p>A failure raised here abandons the rest of the park, neither applied nor routed. That is
     * safe for the reason {@link #close()}'s discard is, and it is why the throw must not be
     * swallowed: the checkpoint does not complete, so the restart replays those records.
     */
    private void runIsolationPass() throws IOException, InterruptedException {
        sendOutstanding();
        drainInFlight();
        for (int budget = pendingIsolation.size(); budget > 0; budget--) {
            ParkedWrite parked = pendingIsolation.poll();
            metrics.writeConfirmedAlone();
            submit(parked.write, parked.estimatedSize, false, true, parked.idDraws);
            sendOutstanding();
            drainInFlight();
        }
        if (!pendingIsolation.isEmpty()) {
            throw new IllegalStateException(
                    "A solo re-send to Firestore database "
                            + database
                            + " was parked again instead of being routed, which cannot happen"
                            + " unless the isolation contract has been broken; "
                            + pendingIsolation.size()
                            + " write(s) would never get a verdict.");
        }
    }

    /**
     * Sends every parked drawn-id create again, each under a new id in the same collection. Not
     * solo: {@code ALREADY_EXISTS} answers only its own write, so batching cannot blur the next
     * verdict.
     *
     * <p>Each re-send waits for room under the in-flight caps like a record: a record admitted
     * while a redraw was parked may hold the slot the park released. The wait runs mails that may
     * park further redraws, which the loop picks up; it ends because a record draws at most {@value
     * #MAX_ID_DRAWS} ids.
     */
    private void sendRedraws() throws IOException, InterruptedException {
        ParkedWrite parked;
        while ((parked = pendingRedraws.poll()) != null) {
            awaitCapacity();
            FirestoreWrite write = parked.write;
            String documentPath = write.getDocumentPath();
            FirestoreWrite redrawn =
                    FirestoreWrite.add(
                            documentPath.substring(0, documentPath.lastIndexOf('/')),
                            write.getFields());
            metrics.idRedrawn();
            submit(redrawn, sizeEstimator.estimate(redrawn), false, false, parked.idDraws + 1);
        }
    }

    private void sendOutstanding() {
        access.sendOutstanding();
        bytesSinceSend = 0;
    }

    /**
     * Admission gate for {@link #write}: runs mailbox mails until both in-flight caps have room.
     */
    private void awaitCapacity() throws IOException, InterruptedException {
        long start = nanoClock.getAsLong();
        while (inFlightWrites >= maxInFlightWrites || inFlightBytes >= maxInFlightBytes) {
            checkAsyncError();
            warnIfStalled(awaitProgress(start, "admitting a record"), "admitting a record");
        }
        checkAsyncError();
    }

    /** Runs mailbox mails until no write is in flight, surfacing any captured failure. */
    private void drainInFlight() throws IOException, InterruptedException {
        long start = nanoClock.getAsLong();
        while (inFlightWrites > 0) {
            checkAsyncError();
            warnIfStalled(
                    awaitProgress(start, "draining the outstanding writes"),
                    "draining the outstanding writes");
        }
        checkAsyncError();
    }

    /**
     * Runs one mailbox mail, or asks the library to send what it holds and reports how long this
     * wait has gone without the library answering anything.
     *
     * <p>The send is on every pass that finds the mailbox empty, not once per wait. The library
     * sends a batch when it holds 20 writes (10 once it holds a retry) or when asked, and on no
     * timer, and a write it retries after a backoff joins whatever batch is open at that moment —
     * possibly after this wait's last request to send. A wait that asked only once could then wait
     * for a write nothing will ever send. Asking again costs one lock acquisition when the library
     * holds nothing.
     *
     * <p>The interrupt flag is read here, first and on every pass: {@code tryYield()} does not look
     * at it, and {@code LockSupport.parkNanos} returns on interrupt without clearing it, so without
     * this read a cancelling task would spin. The idle time is read only once {@code tryYield} has
     * come back empty: a completion mail queued behind other work is a write the library already
     * answered.
     *
     * @return the time this wait has gone without progress, or {@code RAN_A_MAIL} if it ran a mail
     */
    private long awaitProgress(long waitStartNanos, String what) throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException("Interrupted while " + what + " for Firestore.");
        }
        if (mailboxExecutor.tryYield()) {
            return RAN_A_MAIL;
        }
        sendOutstanding();
        long idleSinceNanos =
                lastCompletionNanos - waitStartNanos > 0 ? lastCompletionNanos : waitStartNanos;
        long idleNanos = nanoClock.getAsLong() - idleSinceNanos;
        LockSupport.parkNanos(POLL_INTERVAL_NANOS);
        return idleNanos;
    }

    /**
     * Says that a wait has stopped making progress. No counter can report this state, because the
     * state is that nothing resolves, and Flink's checkpoint timeout may fail the job first with a
     * message naming nothing about Firestore.
     */
    private void warnIfStalled(long idleNanos, String what) {
        if (idleNanos == RAN_A_MAIL || idleNanos < stallWarnAfterNanos) {
            return;
        }
        long now = nanoClock.getAsLong();
        if (now - lastStallWarnNanos < stallWarnAfterNanos) {
            return;
        }
        lastStallWarnNanos = now;
        LOG.warn(
                "No Firestore write to {} has been answered for {} while {} ({} write(s) in"
                        + " flight). The sink is waiting, not failing: each BatchWrite call is"
                        + " bounded by its total timeout (60 seconds unless retryTotalTimeout"
                        + " sets it), and Flink's"
                        + " execution.checkpointing.timeout may fail the job before the library"
                        + " gives up, with a message naming nothing about Firestore. Watch"
                        + " numRecordsSend, which stays flat for as long as this lasts.",
                database,
                Duration.ofNanos(idleNanos),
                what,
                inFlightWrites);
    }

    private void checkAsyncError() throws IOException {
        if (asyncError != null) {
            throw asyncError;
        }
    }

    private void releaseInFlight(int estimatedSize) {
        inFlightWrites--;
        inFlightBytes -= estimatedSize;
    }

    /** Task-thread handler for an applied write, run as a mailbox mail. */
    private void onWriteApplied(int estimatedSize) {
        releaseInFlight(estimatedSize);
        // An applied write is evidence the stream is not wholly broken, a solo re-send included.
        consecutiveRejections = 0;
    }

    /** Task-thread handler for a write the library gave up on, run as a mailbox mail. */
    private void onWriteFailed(
            FirestoreWrite write,
            int estimatedSize,
            boolean solo,
            int idDraws,
            Throwable throwable) {
        releaseInFlight(estimatedSize);
        failuresOnBulkWriter++;
        FirestoreErrorClassifier.Kind kind =
                FirestoreErrorClassifier.classify(throwable, write, preconditionFailurePolicy);
        if (kind == FirestoreErrorClassifier.Kind.INVALID && !solo) {
            // The status may answer the request rather than this write. Park it for the isolation
            // pass; routing here would drop a whole request for one bad write (ADR-0045). Not
            // counted yet, or one request-level status would be counted once per write it carried.
            pendingIsolation.add(new ParkedWrite(write, estimatedSize, idDraws));
            return;
        }
        if (kind == FirestoreErrorClassifier.Kind.ID_TAKEN) {
            // Not the record's failure: the create left the existing document in place. Neither
            // errorClass nor numRecordsSendErrors counts it; idsRedrawn does, when it is re-sent.
            if (idDraws < MAX_ID_DRAWS) {
                pendingRedraws.add(new ParkedWrite(write, estimatedSize, idDraws));
                return;
            }
            // The refusal that fails the job is counted like every other job-failing status.
            metrics.writeFailure(FirestoreErrorClassifier.statusCode(throwable));
            if (asyncError == null) {
                asyncError =
                        new IOException(
                                "Firestore refused "
                                        + idDraws
                                        + " drawn document id(s) in a row with ALREADY_EXISTS for"
                                        + " one record, the last at "
                                        + describe(write)
                                        + ". Ids drawn at random do not collide that often, so the"
                                        + " job fails rather than draw again.",
                                throwable);
            }
            return;
        }
        StatusCode.Code code = FirestoreErrorClassifier.statusCode(throwable);
        metrics.writeFailure(code);
        if (kind == FirestoreErrorClassifier.Kind.FATAL) {
            if (asyncError == null) {
                asyncError =
                        new IOException(
                                "Firestore refused a write to "
                                        + describe(write)
                                        + " with "
                                        + (code == null ? "no status" : code)
                                        + fatalReason(code, write)
                                        + ".",
                                throwable);
            }
            return;
        }
        route(write, code, throwable);
    }

    /** Explains, for the job-failure message, why a routable-looking status failed the job. */
    private String fatalReason(StatusCode.Code code, FirestoreWrite write) {
        if (code == StatusCode.Code.NOT_FOUND) {
            return write.getOperation() == FirestoreWrite.Operation.UPDATE
                    ? ", which is never routed: an update of a missing document cannot be told"
                            + " apart from a missing database, which would answer it for every"
                            + " write. Use setMerge for a document that may be missing"
                    : ", which a "
                            + write.getOperation()
                            + " can only receive when the database itself is missing: check that "
                            + database
                            + " exists and is a Firestore database in Native mode";
        }
        if (code == StatusCode.Code.FAILED_PRECONDITION) {
            return write.getLastUpdateTime() == null
                    ? ", which a write without a precondition is never routed for: it describes the"
                            + " database rather than the record (a database in Datastore mode is"
                            + " reported to refuse every Firestore API write with it)"
                    : " because its lastUpdateTime precondition no longer holds, and"
                            + " preconditionFailurePolicy is FAIL_JOB";
        }
        if (code == StatusCode.Code.ALREADY_EXISTS) {
            return ", which is routed only for a create";
        }
        return "";
    }

    /**
     * Hands a refused write to the configured handler. Runs as a mailbox mail, so a handler that
     * fails the job cannot throw at a caller: its failure is captured into {@code asyncError} and
     * rethrown from the next {@link #write} or {@link #flush}. First failure wins.
     */
    private void route(FirestoreWrite write, StatusCode.Code code, Throwable throwable) {
        metrics.writeFailed();
        // A replayed create answers ALREADY_EXISTS, and a replayed conditional write answers
        // FAILED_PRECONDITION (its first application changed the update time), for every such
        // write a restart repeats; counting either would turn the replay into a rejection run that
        // fails the job again on each restart (ADR-0076). Only a conditional write reaches here
        // with FAILED_PRECONDITION.
        boolean counts =
                code != StatusCode.Code.ALREADY_EXISTS
                        && code != StatusCode.Code.FAILED_PRECONDITION;
        if (counts) {
            consecutiveRejections++;
        }
        try {
            failedWriteHandler.handle(
                    FailedWrite.of(
                            database,
                            write,
                            "Firestore refused the write with " + code + ".",
                            throwable));
        } catch (IOException | RuntimeException e) {
            if (asyncError == null) {
                asyncError =
                        e instanceof IOException
                                ? (IOException) e
                                : new IOException(
                                        "The failed-write handler failed for Firestore database "
                                                + database
                                                + ".",
                                        e);
            }
        }
        // After the routing, not instead of it: the write that tripped the bound really was
        // refused, and a dead-letter destination missing it would be worse than one holding it.
        if (counts
                && maxConsecutiveRejections != FirestoreWriterOptions.UNBOUNDED
                && consecutiveRejections >= maxConsecutiveRejections
                && asyncError == null) {
            asyncError =
                    new IOException(
                            "Firestore refused "
                                    + consecutiveRejections
                                    + " write(s) in a row (the last to "
                                    + describe(write)
                                    + ", with status "
                                    + code
                                    + ") with none applied between them, reaching"
                                    + " maxConsecutiveRejections("
                                    + maxConsecutiveRejections
                                    + "): the stream's data looks broken rather than anomalous,"
                                    + " so the job fails instead of routing it record by record."
                                    + " Every refused write, this one included, was routed to the"
                                    + " configured handler first;"
                                    + " FirestoreWriterOptions.builder().maxConsecutiveRejections(-1)"
                                    + " removes this bound.",
                            throwable);
        }
    }

    private String describe(FirestoreWrite write) {
        return database + "/documents/" + write.getDocumentPath();
    }

    @VisibleForTesting
    int getInFlightWrites() {
        return inFlightWrites;
    }

    @VisibleForTesting
    long getInFlightBytes() {
        return inFlightBytes;
    }

    @VisibleForTesting
    int getParkedWrites() {
        return pendingIsolation.size();
    }

    /** A write awaiting a solo re-send, or a re-send under a new id. */
    private static final class ParkedWrite {

        private final FirestoreWrite write;
        private final int estimatedSize;
        private final int idDraws;

        private ParkedWrite(FirestoreWrite write, int estimatedSize, int idDraws) {
            this.write = write;
            this.estimatedSize = estimatedSize;
            this.idDraws = idDraws;
        }
    }

    /**
     * Re-dispatches a write's completion onto the mailbox so state stays task-thread-only. It is
     * also its own success mail.
     */
    private final class WriteCallback
            implements ApiFutureCallback<WriteResult>, ThrowingRunnable<Exception> {

        private final FirestoreWrite write;
        private final int estimatedSize;
        private final boolean solo;
        private final int idDraws;

        private WriteCallback(FirestoreWrite write, int estimatedSize, boolean solo, int idDraws) {
            this.write = write;
            this.estimatedSize = estimatedSize;
            this.solo = solo;
            this.idDraws = idDraws;
        }

        /** The success mail: runs on the task thread. */
        @Override
        public void run() {
            onWriteApplied(estimatedSize);
        }

        // Both stamp before dispatching: what a wait measures is whether the library is still
        // answering, and a failure is an answer.

        @Override
        public void onSuccess(WriteResult result) {
            lastCompletionNanos = nanoClock.getAsLong();
            dispatch(this, COMPLETION_MAIL);
        }

        @Override
        public void onFailure(Throwable throwable) {
            lastCompletionNanos = nanoClock.getAsLong();
            dispatch(
                    () -> onWriteFailed(write, estimatedSize, solo, idDraws, throwable),
                    FAILURE_MAIL);
        }

        private void dispatch(ThrowingRunnable<Exception> mail, String description) {
            try {
                mailboxExecutor.execute(mail, description);
            } catch (RejectedExecutionException e) {
                // The mailbox refuses mail once the task is torn down; the writer that would have
                // run this has been closed, and nothing waits for it any more.
                LOG.debug("Dropped a Firestore write completion after the writer closed.", e);
            }
        }
    }
}
