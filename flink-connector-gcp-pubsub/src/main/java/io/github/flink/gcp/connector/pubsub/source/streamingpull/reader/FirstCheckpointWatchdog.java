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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.function.LongSupplier;

/**
 * Fails a reader that holds unacknowledged messages but has never been asked for a checkpoint.
 *
 * <p>This source acknowledges only on checkpoint completion, so a streaming job running without
 * checkpointing acknowledges nothing: it consumes until the client library's flow control fills and
 * then stalls, silently and forever. That is worse than an exception, so the reader raises one.
 *
 * <p>The configuration cannot be asked about it. A reader is handed the <em>TaskManager</em>
 * configuration — {@code SourceOperatorFactory} passes {@code
 * getTaskManagerInfo().getConfiguration()} into {@code SourceOperator} — while {@code
 * env.enableCheckpointing(...)} writes {@code execution.checkpointing.interval} into the
 * <em>job</em> configuration, and {@code CheckpointConfig.disableCheckpointing()} removes the key
 * rather than zeroing it. Absence therefore proves nothing, and failing on it would break every job
 * that enables checkpointing programmatically. The reader observes the outcome instead: no
 * checkpoint taken, messages outstanding, budget spent.
 *
 * <p>Checking is driven by {@code pollNext}, so an idle reader is not watched. That only delays the
 * diagnosis rather than losing it: the outstanding messages eventually exhaust their
 * acknowledgement-deadline extension budget and are redelivered, which resumes polling and trips
 * the watchdog on the next pass.
 *
 * <p>Not thread-safe; confined to the reader's task thread.
 */
@Internal
final class FirstCheckpointWatchdog {

    private static final Logger LOG = LoggerFactory.getLogger(FirstCheckpointWatchdog.class);

    /**
     * Polls between two clock reads. {@code pollNext} runs once per record, so consulting the clock
     * on every call would tax the hot path for the whole window before the first checkpoint; a
     * coarse sample is plenty for a budget measured in minutes.
     */
    private static final int POLLS_PER_CLOCK_READ = 1_024;

    private final long budgetNanos;
    private final long warnAfterNanos;
    private final LongSupplier nanoTime;
    private final long startNanos;

    private boolean sawCheckpoint;
    private boolean warned;
    private int pollsSinceClockRead;

    /**
     * Creates the watchdog and starts its budget.
     *
     * @param budget how long to wait for the first checkpoint; {@link Duration#ZERO} disables the
     *     watchdog
     */
    FirstCheckpointWatchdog(Duration budget) {
        this(budget, System::nanoTime);
    }

    @VisibleForTesting
    FirstCheckpointWatchdog(Duration budget, LongSupplier nanoTime) {
        this.budgetNanos = budget.toNanos();
        this.warnAfterNanos = budgetNanos / 2;
        this.nanoTime = nanoTime;
        this.startNanos = nanoTime.getAsLong();
    }

    /**
     * Records that a checkpoint was taken, which retires the watchdog for the reader's lifetime.
     */
    void checkpointTaken() {
        sawCheckpoint = true;
    }

    /**
     * Checks the budget, warning halfway through and failing at the end.
     *
     * @param ackTracker the tracker whose outstanding count separates "nothing is being
     *     acknowledged" from "there is nothing to acknowledge"
     * @throws IllegalStateException if the budget is spent while messages are outstanding
     */
    void check(AckTracker ackTracker) {
        if (sawCheckpoint || budgetNanos == 0) {
            return;
        }
        if (++pollsSinceClockRead < POLLS_PER_CLOCK_READ) {
            return;
        }
        pollsSinceClockRead = 0;
        long elapsedNanos = nanoTime.getAsLong() - startNanos;
        if (elapsedNanos < warnAfterNanos || ackTracker.outstandingAckCount() == 0) {
            return;
        }
        if (elapsedNanos < budgetNanos) {
            if (!warned) {
                warned = true;
                LOG.warn(
                        "No checkpoint has been taken in the first {} of this reader's life while"
                                + " {} messages wait to be acknowledged. The Pub/Sub source"
                                + " acknowledges only on checkpoint completion, so the job will"
                                + " fail after {} unless a checkpoint is taken.",
                        Duration.ofNanos(elapsedNanos),
                        ackTracker.outstandingAckCount(),
                        Duration.ofNanos(budgetNanos));
            }
            return;
        }
        throw new IllegalStateException(
                "No checkpoint has been taken within "
                        + Duration.ofNanos(budgetNanos)
                        + " while "
                        + ackTracker.outstandingAckCount()
                        + " messages wait to be acknowledged. The Pub/Sub source acknowledges"
                        + " messages only when the checkpoint covering them completes, so nothing"
                        + " will ever be acknowledged and the source stalls once the client"
                        + " library's flow control fills. Enable checkpointing"
                        + " (execution.checkpointing.interval), or raise"
                        + " PubSubSubscriberOptions.firstCheckpointTimeout(...) above the"
                        + " checkpoint interval if the job legitimately checkpoints less often.");
    }
}
