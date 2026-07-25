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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import java.time.Duration;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

/**
 * Fails a reader that holds unacknowledged messages but has never been asked for a checkpoint.
 *
 * <p>This source acknowledges only on checkpoint completion, so a streaming job running without
 * checkpointing acknowledges nothing: it consumes until the client library's flow control fills and
 * then stalls, silently and forever. That is worse than an exception, so the reader raises one.
 *
 * <p><b>The configuration cannot be asked about it.</b> A reader is handed the <em>TaskManager</em>
 * configuration — {@code SourceOperatorFactory} passes {@code
 * getTaskManagerInfo().getConfiguration()} into {@code SourceOperator}, which returns it verbatim
 * from {@code SourceReaderContext.getConfiguration()} — while {@code env.enableCheckpointing(...)}
 * writes {@code execution.checkpointing.interval} into the <em>job</em> configuration, and {@code
 * CheckpointConfig.disableCheckpointing()} removes the key rather than zeroing it. Absence
 * therefore proves nothing, and failing on it would break every job that enables checkpointing
 * programmatically. This class observes the outcome instead: no checkpoint taken, messages
 * outstanding, budget spent.
 *
 * <p>Checking is driven by {@link PubSubSplitReader#fetch()} rather than by the record path,
 * because the stalled state is precisely the state with no records: once flow control fills, the
 * client stops delivering, the fetch parks and nothing polls again. {@link #parkTimeoutMillis()}
 * bounds that park while the detector is armed, and returns zero once a checkpoint has been taken —
 * so a healthy reader parks indefinitely, exactly as it did before this class existed.
 *
 * <p>The budget starts at {@link #startBudget()} rather than at construction, because "waited too
 * long for a first checkpoint" is only meaningful over an interval in which there was something to
 * checkpoint. A reader is created before the enumerator assigns it anything, so a budget started in
 * the constructor is partly spent before the reader can do any work. An unstarted detector is
 * therefore never armed: a reader that is assigned no split at all never fires and parks
 * indefinitely.
 *
 * <p>{@link #checkpointTaken()} runs on the reader's task thread, which is what the one volatile
 * field is for. Everything else — including the {@link #startBudget()} call the split reader makes
 * from {@code handleSplitsChanges} — runs on the fetcher thread: {@code SplitFetcher} enqueues
 * split changes as tasks and runs them in the same loop as {@code fetch()}, and every {@code
 * SplitReader} method but {@code wakeUp()} is documented not to run in parallel with another. The
 * two fields backing the budget are therefore confined to that thread and need no synchronization.
 */
@Internal
public final class MissingCheckpointDetector {

    /**
     * Caps how long an armed fetch parks. The budget is the thing being measured; this only decides
     * how promptly the reader notices it has been spent, and a quarter of the budget gives four
     * chances to observe it.
     */
    private static final long MAX_PARK_TIMEOUT_MILLIS = Duration.ofSeconds(30).toMillis();

    private final long budgetNanos;
    private final long parkTimeoutMillis;
    private final IntSupplier outstandingAckCount;
    private final LongSupplier nanoTime;

    private boolean started;

    /** Meaningful only once {@link #started}. */
    private long startNanos;

    private volatile boolean sawCheckpoint;

    /**
     * Creates the detector. Its budget starts at the first {@link #startBudget()}.
     *
     * @param budget how long to wait for the first checkpoint; {@link Duration#ZERO} disables the
     *     detector
     * @param outstandingAckCount supplies the messages received or emitted but not yet
     *     acknowledged, which separates "nothing is being acknowledged" from "there is nothing to
     *     acknowledge"
     */
    public MissingCheckpointDetector(Duration budget, IntSupplier outstandingAckCount) {
        this(budget, outstandingAckCount, System::nanoTime);
    }

    @VisibleForTesting
    MissingCheckpointDetector(
            Duration budget, IntSupplier outstandingAckCount, LongSupplier nanoTime) {
        this.budgetNanos = budget.toNanos();
        this.parkTimeoutMillis =
                Math.max(1, Math.min(budget.toMillis() / 4, MAX_PARK_TIMEOUT_MILLIS));
        this.outstandingAckCount = outstandingAckCount;
        this.nanoTime = nanoTime;
    }

    /**
     * Starts the budget, which arms the detector. Called when the reader is given work; the first
     * call wins, so a later split assignment does not push the deadline out.
     */
    public void startBudget() {
        if (!started) {
            startNanos = nanoTime.getAsLong();
            started = true;
        }
    }

    /**
     * Records that a checkpoint was taken, which retires the detector for the reader's lifetime.
     *
     * <p>The reader reports this from {@code snapshotState}, which {@code SourceOperator} calls on
     * every barrier — one carrying no data, or reaching a reader that owns no split, counts. So the
     * detector retires at the first barrier to reach the reader, and what it is left measuring is
     * only the interval before a job's first checkpoint.
     */
    public void checkpointTaken() {
        sawCheckpoint = true;
    }

    /**
     * Returns how long a fetch may park before {@link #check()} should run again, or {@code 0} to
     * park indefinitely.
     *
     * @return the park timeout in milliseconds, or {@code 0} for no timeout
     */
    public long parkTimeoutMillis() {
        return isArmed() ? parkTimeoutMillis : 0;
    }

    /**
     * Fails the reader if the budget is spent while messages are outstanding.
     *
     * @throws IllegalStateException if no checkpoint has been taken in time
     */
    public void check() {
        if (!isArmed() || nanoTime.getAsLong() - startNanos < budgetNanos) {
            return;
        }
        int outstanding = outstandingAckCount.getAsInt();
        if (outstanding == 0) {
            return;
        }
        throw new IllegalStateException(
                "No checkpoint has been taken within "
                        + Duration.ofNanos(budgetNanos)
                        + " while "
                        + outstanding
                        + " messages wait to be acknowledged. The Pub/Sub source acknowledges"
                        + " messages only when the checkpoint covering them completes, so nothing"
                        + " will ever be acknowledged and the source stalls once the client"
                        + " library's flow control fills. Enable checkpointing"
                        + " (execution.checkpointing.interval), or raise"
                        + " PubSubSubscriberOptions.firstCheckpointTimeout(...) above the"
                        + " checkpoint interval if the job legitimately checkpoints less often.");
    }

    private boolean isArmed() {
        return budgetNanos > 0 && started && !sawCheckpoint;
    }
}
