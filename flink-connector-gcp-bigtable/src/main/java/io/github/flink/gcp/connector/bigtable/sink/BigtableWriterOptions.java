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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.options.OptionChecks;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/**
 * Tuning options for the sink's writer: the batch thresholds handed to the client, and the writer's
 * own bounds on unacknowledged entries.
 *
 * <p>Set via {@link BigtableSinkBuilder#writerOptions(BigtableWriterOptions)}; optional — every
 * knob is defaulted, so {@link #defaults()} is equivalent to not setting options at all. An unset
 * batch threshold leaves the client's own default in place rather than restating it here, so a
 * client upgrade that retunes it is inherited.
 *
 * <h2>Entries, mutations, and which knob binds</h2>
 *
 * <p><b>Every count here is a count of entries, never of mutations.</b> An entry is one {@code
 * RowMutationEntry} — one record the serializer returned — and it carries as many mutations as the
 * serializer put {@code setCell} calls in it. Bigtable's documented limit is on <em>mutations</em>:
 * no more than 100,000 in a batch. The two numbers are not compared by this connector, and need not
 * be by a job either, because <b>the client enforces the mutation limit itself and
 * unconditionally</b>: its batch resource flushes as soon as one more entry would carry the
 * accumulated batch past 100,000 mutations, whatever {@link Builder#batchElementCount(long)} says,
 * and no single entry can carry more than that on its own. So no setting of these knobs produces an
 * over-limit request (read from google-cloud-bigtable 2.80.0 on 2026-08-10, and pinned by {@code
 * BigtableClientMutationLimitTest} so that a client upgrade moving either fact fails a test rather
 * than a job — {@code docs/adr/0082}).
 *
 * <p>A batch is therefore sent on whichever of five conditions arrives first: {@link
 * Builder#batchElementCount(long)}, {@link Builder#batchByteSize(long)}, the client's one-second
 * timer, the client's 100,000-mutation guard, and the writer's own {@link
 * Builder#maxInFlightEntries(int)} — which sends every batcher when the writer fills. Any claim of
 * the form "setting X large makes batches of X" has to name the condition that <em>binds</em>, or
 * it is false.
 *
 * <p>There are deliberately <em>no</em> retry knobs. Unlike Cloud Tasks, the Bigtable client
 * retries {@code MutateRows} itself — per entry, for the transient codes, on a schedule of its own
 * — so the sink owns no retry loop and has nothing to expose. The {@code recovery*} knobs are not
 * an exception: they budget the sink-owned table auto-creation repair (re-applying mutations after
 * creating a missing table), not the client's mutation retries.
 *
 * <h2>Why the in-flight bounds are the writer's own</h2>
 *
 * <p>The client also has a flow controller of its own, and it is the wrong instrument here: it
 * <em>blocks</em> the calling thread when its limits are reached, and the calling thread is Flink's
 * task thread, which must stay free to run mailbox mails. So the writer keeps its own bounds and
 * yields to the mailbox instead. That only works while the writer's bounds are reached first, and
 * the client's limits — 20,000 outstanding entries and 100 MiB, blocking — cannot be raised through
 * its public API. Raising {@link Builder#maxInFlightEntries(int)} far above the default therefore
 * moves the effective bound into the client, where it stalls the task thread rather than
 * backpressuring the stream.
 *
 * <p>Per-record destinations do not change that relationship, and the reason is measured rather
 * than assumed: the client's flow controller is one per <em>client</em> — {@code
 * EnhancedBigtableStub} builds a single {@code bulkMutationFlowController} and hands the same
 * instance to every batcher it creates — so the tables of one instance draw on one budget, and
 * these caps, being the writer's rather than each destination's, still bind first however many
 * tables the sink writes to.
 *
 * <p>Instances are immutable and serializable.
 */
@PublicEvolving
public final class BigtableWriterOptions implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(BigtableWriterOptions.class);

    private static final long serialVersionUID = 1L;

    /**
     * The default {@link Builder#maxConsecutiveRejections(int)}: enough confirmed rejections in a
     * row to say the stream's data is broken rather than anomalous, at an isolation cost of about a
     * hundred solo requests — one {@code MutateRows} round trip each — before the job fails.
     */
    public static final int DEFAULT_MAX_CONSECUTIVE_REJECTIONS = 100;

    /** {@link Builder#maxConsecutiveRejections(int)} value under which the bound never fires. */
    public static final int UNBOUNDED = -1;

    /**
     * Default for {@link Builder#destinationIdleTimeout(Duration)}: one hour. Coarse on purpose —
     * eviction is memory hygiene for long-lived jobs with per-record destinations (for example
     * date-suffixed tables), and an evicted table that receives a mutation again just rebuilds its
     * batcher once.
     */
    public static final Duration DEFAULT_DESTINATION_IDLE_TIMEOUT = Duration.ofHours(1);

    /**
     * The client's own flow-control budget for the bulk-mutation path: 20,000 accumulated entries
     * and 100 MiB, blocking, neither settable through its public API ({@code
     * ClientOperationSettings}, google-cloud-bigtable 2.80.0, read 2026-08-10).
     *
     * <p>These are what the two ceilings below are derived from, and the derivation is the client's
     * own rule rather than a judgement of ours: {@code
     * BigtableBatchingCallSettings.Builder.build()} requires each batch threshold to be
     * <em>strictly under</em> the matching budget, and throws when it is not. A threshold at or
     * above one of these therefore does not produce a large batch, or a blocked task thread, or
     * anything else — it produces a client that cannot be built, on the task manager, when the
     * writer opens.
     */
    private static final long CLIENT_MAX_OUTSTANDING_ENTRIES = 20_000;

    /** The byte half of the budget {@link #CLIENT_MAX_OUTSTANDING_ENTRIES} documents. */
    private static final long CLIENT_MAX_OUTSTANDING_BYTES = 100L * 1024 * 1024;

    /**
     * The largest {@link Builder#batchElementCount(long)} this connector accepts: one under {@link
     * #CLIENT_MAX_OUTSTANDING_ENTRIES}. Written as the subtraction rather than as 19,999 so that a
     * client release moving its budget moves this with it, instead of leaving a ceiling that admits
     * a value the client then refuses.
     *
     * <p>Package-private, and the setter's {@code @param} names the number rather than this symbol:
     * nothing outside this package has asked for it, and a public compile-time constant is inlined
     * into whatever refers to it, which would leave a caller pinned to a value a later release
     * changed. Widen it when something asks, not before.
     */
    static final long MAX_BATCH_ELEMENT_COUNT_LIMIT = CLIENT_MAX_OUTSTANDING_ENTRIES - 1;

    /**
     * The largest {@link Builder#batchByteSize(long)} this connector accepts: one byte under {@link
     * #CLIENT_MAX_OUTSTANDING_BYTES}, for the reason {@link #MAX_BATCH_ELEMENT_COUNT_LIMIT} gives.
     *
     * <p><b>No service figure stands behind this one</b>, and none exists to: Bigtable's quotas
     * page states no size limit for a {@code MutateRows} request at all — its size rows bound a
     * single mutation (200 MB), a cell value (100 MB), a row (256 MB) and a row key (4 KB). What is
     * bounded here is what the client will let a job configure, which is a stricter and
     * better-defined thing to bound at.
     */
    static final long MAX_BATCH_BYTE_SIZE_LIMIT = CLIENT_MAX_OUTSTANDING_BYTES - 1;

    private static final BigtableWriterOptions DEFAULTS = builder().build();

    @Nullable private final Long batchElementCount;
    @Nullable private final Long batchByteSize;
    private final int maxInFlightEntries;
    private final long maxInFlightBytes;
    private final int maxConsecutiveRejections;
    private final Duration recoveryInitialBackoff;
    private final Duration recoveryMaxBackoff;
    private final int recoveryMaxAttempts;
    private final Duration destinationIdleTimeout;
    private final boolean perDestinationMetrics;

    private BigtableWriterOptions(Builder builder) {
        this.batchElementCount = builder.batchElementCount;
        this.batchByteSize = builder.batchByteSize;
        this.maxInFlightEntries = builder.maxInFlightEntries;
        this.maxInFlightBytes = builder.maxInFlightBytes;
        this.maxConsecutiveRejections = builder.maxConsecutiveRejections;
        this.recoveryInitialBackoff = builder.recoveryInitialBackoff;
        this.recoveryMaxBackoff = builder.recoveryMaxBackoff;
        this.recoveryMaxAttempts = builder.recoveryMaxAttempts;
        this.destinationIdleTimeout = builder.destinationIdleTimeout;
        this.perDestinationMetrics = builder.perDestinationMetrics;
    }

    /**
     * Creates a new {@link Builder}.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the default options: the client's own batch thresholds, at most 1000 unacknowledged
     * entries, at most 64 MiB of them, a job failure after {@value
     * #DEFAULT_MAX_CONSECUTIVE_REJECTIONS} consecutive confirmed rejections under a dropping
     * policy, a table auto-creation recovery budget of 500 ms doubling to 10 s over at most 10
     * attempts, an idle table's batcher dropped after {@link #DEFAULT_DESTINATION_IDLE_TIMEOUT},
     * and no per-table counters.
     *
     * @return the default options
     */
    public static BigtableWriterOptions defaults() {
        return DEFAULTS;
    }

    /** Returns the batch element-count threshold, or {@code null} to use the client's default. */
    @Nullable
    public Long getBatchElementCount() {
        return batchElementCount;
    }

    /** Returns the batch byte threshold, or {@code null} to use the client's default. */
    @Nullable
    public Long getBatchByteSize() {
        return batchByteSize;
    }

    /** Returns the writer's cap on unacknowledged entries. */
    public int getMaxInFlightEntries() {
        return maxInFlightEntries;
    }

    /** Returns the writer's cap on the serialized size of unacknowledged entries. */
    public long getMaxInFlightBytes() {
        return maxInFlightBytes;
    }

    /**
     * Returns how many consecutive confirmed rejections fail the job, or {@link #UNBOUNDED} for
     * none.
     */
    public int getMaxConsecutiveRejections() {
        return maxConsecutiveRejections;
    }

    /** Returns the first backoff of the table auto-creation recovery. */
    public Duration getRecoveryInitialBackoff() {
        return recoveryInitialBackoff;
    }

    /** Returns the backoff cap of the table auto-creation recovery. */
    public Duration getRecoveryMaxBackoff() {
        return recoveryMaxBackoff;
    }

    /** Returns the maximum re-apply attempts of the table auto-creation recovery. */
    public int getRecoveryMaxAttempts() {
        return recoveryMaxAttempts;
    }

    /** Returns how long a table may go without mutations before the writer drops its batcher. */
    public Duration getDestinationIdleTimeout() {
        return destinationIdleTimeout;
    }

    /** Returns whether per-table counters are registered beside the writer's totals. */
    public boolean isPerDestinationMetrics() {
        return perDestinationMetrics;
    }

    /**
     * Returns the table auto-creation recovery schedule the {@code recovery*} knobs describe.
     * Jittered: every subtask that parked mutations for the same missing table resumes against the
     * same freshly created table, so unjittered they would re-apply in lockstep.
     */
    @Internal
    public RetrySchedule toRecoverySchedule() {
        return new RetrySchedule(
                recoveryInitialBackoff.toMillis(),
                recoveryMaxBackoff.toMillis(),
                recoveryMaxAttempts,
                RetrySchedule.DEFAULT_JITTER_RATIO);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BigtableWriterOptions that = (BigtableWriterOptions) o;
        return maxInFlightEntries == that.maxInFlightEntries
                && maxInFlightBytes == that.maxInFlightBytes
                && maxConsecutiveRejections == that.maxConsecutiveRejections
                && recoveryMaxAttempts == that.recoveryMaxAttempts
                && perDestinationMetrics == that.perDestinationMetrics
                && Objects.equals(batchElementCount, that.batchElementCount)
                && Objects.equals(batchByteSize, that.batchByteSize)
                && recoveryInitialBackoff.equals(that.recoveryInitialBackoff)
                && recoveryMaxBackoff.equals(that.recoveryMaxBackoff)
                && destinationIdleTimeout.equals(that.destinationIdleTimeout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                batchElementCount,
                batchByteSize,
                maxInFlightEntries,
                maxInFlightBytes,
                maxConsecutiveRejections,
                recoveryInitialBackoff,
                recoveryMaxBackoff,
                recoveryMaxAttempts,
                destinationIdleTimeout,
                perDestinationMetrics);
    }

    @Override
    public String toString() {
        return "BigtableWriterOptions{batchElementCount="
                + batchElementCount
                + ", batchByteSize="
                + batchByteSize
                + ", maxInFlightEntries="
                + maxInFlightEntries
                + ", maxInFlightBytes="
                + maxInFlightBytes
                + ", maxConsecutiveRejections="
                + maxConsecutiveRejections
                + ", recoveryInitialBackoff="
                + recoveryInitialBackoff
                + ", recoveryMaxBackoff="
                + recoveryMaxBackoff
                + ", recoveryMaxAttempts="
                + recoveryMaxAttempts
                + ", destinationIdleTimeout="
                + destinationIdleTimeout
                + ", perDestinationMetrics="
                + perDestinationMetrics
                + "}";
    }

    /** Builder for {@link BigtableWriterOptions}. */
    @PublicEvolving
    public static final class Builder {

        @Nullable private Long batchElementCount;
        @Nullable private Long batchByteSize;
        private int maxInFlightEntries = 1000;
        private long maxInFlightBytes = 64L * 1024 * 1024;
        private int maxConsecutiveRejections = DEFAULT_MAX_CONSECUTIVE_REJECTIONS;
        private Duration recoveryInitialBackoff = Duration.ofMillis(500);
        private Duration recoveryMaxBackoff = Duration.ofSeconds(10);
        private int recoveryMaxAttempts = 10;
        private Duration destinationIdleTimeout = DEFAULT_DESTINATION_IDLE_TIMEOUT;
        private boolean perDestinationMetrics;

        private Builder() {}

        /**
         * Sets how many <em>entries</em> the client accumulates before sending a batch — one per
         * record the serializer returned, however many mutations each carries. Defaults to the
         * client's own threshold (100 entries).
         *
         * <p>This is a threshold, not a cap on what a request may hold: the client sends a batch on
         * whichever condition arrives first, and its own 100,000-mutation guard is one of them. See
         * the class documentation for the other three.
         *
         * @param batchElementCount the element-count threshold, positive and at most 19,999 — one
         *     under the 20,000 entries the client's flow controller admits, which its own settings
         *     builder requires this threshold to stay strictly below
         * @return this builder
         */
        public Builder batchElementCount(long batchElementCount) {
            Preconditions.checkArgument(
                    batchElementCount > 0, "batchElementCount must be positive");
            Preconditions.checkArgument(
                    batchElementCount <= MAX_BATCH_ELEMENT_COUNT_LIMIT,
                    "batchElementCount must be at most %s: the client's settings builder requires"
                            + " it to stay strictly below the %s entries its flow controller"
                            + " admits, and refuses to build a client otherwise.",
                    MAX_BATCH_ELEMENT_COUNT_LIMIT,
                    CLIENT_MAX_OUTSTANDING_ENTRIES);
            this.batchElementCount = batchElementCount;
            return this;
        }

        /**
         * Sets how many bytes of mutations the client accumulates before sending a batch. Defaults
         * to the client's own threshold (20 MiB). Entry count alone bounds no memory: a single
         * entry may be megabytes, since Bigtable's own size limits are per mutation (200 MB), per
         * cell value (100 MB) and per row (256 MB).
         *
         * @param batchByteSize the byte threshold, positive and at most one byte under the 100 MiB
         *     the client's flow controller admits in flight, which its own settings builder
         *     requires this threshold to stay strictly below
         * @return this builder
         */
        public Builder batchByteSize(long batchByteSize) {
            Preconditions.checkArgument(batchByteSize > 0, "batchByteSize must be positive");
            Preconditions.checkArgument(
                    batchByteSize <= MAX_BATCH_BYTE_SIZE_LIMIT,
                    "batchByteSize must be at most %s bytes: the client's settings builder requires"
                            + " it to stay strictly below the %s bytes (100 MiB) its flow"
                            + " controller admits, and refuses to build a client otherwise.",
                    MAX_BATCH_BYTE_SIZE_LIMIT,
                    CLIENT_MAX_OUTSTANDING_BYTES);
            this.batchByteSize = batchByteSize;
            return this;
        }

        /**
         * Caps the entries the writer keeps unacknowledged — one per record written, whatever each
         * carries. A write at the cap yields to the task mailbox until completions bring the count
         * down, bounding sink memory between checkpoints. Defaults to 1000.
         *
         * <p>Raising this far above the default moves the effective bound into the client's own
         * flow controller, which blocks the task thread instead of yielding — see the class
         * documentation.
         *
         * @param maxInFlightEntries the in-flight cap, positive
         * @return this builder
         */
        public Builder maxInFlightEntries(int maxInFlightEntries) {
            Preconditions.checkArgument(
                    maxInFlightEntries > 0, "maxInFlightEntries must be positive");
            this.maxInFlightEntries = maxInFlightEntries;
            return this;
        }

        /**
         * Caps the serialized size of the entries the writer keeps unacknowledged. Defaults to 64
         * MiB. This is the bound that actually bounds memory — a single entry may be megabytes, so
         * a count alone does not.
         *
         * @param maxInFlightBytes the in-flight byte cap, positive
         * @return this builder
         */
        public Builder maxInFlightBytes(long maxInFlightBytes) {
            Preconditions.checkArgument(maxInFlightBytes > 0, "maxInFlightBytes must be positive");
            this.maxInFlightBytes = maxInFlightBytes;
            return this;
        }

        /**
         * Sets how many <em>consecutive</em> confirmed rejections fail the job. Defaults to {@value
         * #DEFAULT_MAX_CONSECUTIVE_REJECTIONS}; {@link #UNBOUNDED} (-1) never fails it.
         *
         * <p>This bound only matters beside a dropping {@code failedMutationHandler} — under the
         * default {@code failJob()} the first confirmed rejection fails the job anyway. A dropping
         * policy is a decision to keep running through <em>anomalous</em> records, and the sink
         * pays one solo request per rejection to isolate each from the good records batched with
         * it. When every record is being refused, that is no longer a stream with anomalies but a
         * broken pipeline degraded to unbatched writes under a green job — so once this many
         * confirmed rejections arrive in a row, with not one successfully applied mutation between
         * them, the job fails with a message naming this option. Any applied mutation resets the
         * count: an occasional bad record can never accumulate into a failure, however long the job
         * runs.
         *
         * <p>Only rejections the isolation pass has <em>confirmed</em> against a single mutation
         * count; records the serializer rejects do not, since they say nothing about the service's
         * view of the stream.
         *
         * @param maxConsecutiveRejections the bound, positive or {@link #UNBOUNDED}
         * @return this builder
         */
        public Builder maxConsecutiveRejections(int maxConsecutiveRejections) {
            Preconditions.checkArgument(
                    maxConsecutiveRejections > 0 || maxConsecutiveRejections == UNBOUNDED,
                    "maxConsecutiveRejections must be positive or -1 (unbounded)");
            this.maxConsecutiveRejections = maxConsecutiveRejections;
            return this;
        }

        /**
         * Sets the first backoff of the table auto-creation recovery (re-applying mutations after
         * creating a missing table). Defaults to 500 ms.
         *
         * @param recoveryInitialBackoff the first backoff, at least 1 ms
         * @return this builder
         */
        public Builder recoveryInitialBackoff(Duration recoveryInitialBackoff) {
            this.recoveryInitialBackoff =
                    OptionChecks.checkAtLeastOneMilli(
                            recoveryInitialBackoff, "recoveryInitialBackoff");
            return this;
        }

        /**
         * Caps the backoff of the table auto-creation recovery. Defaults to 10 s.
         *
         * @param recoveryMaxBackoff the backoff cap, at least 1 ms and at least the initial backoff
         * @return this builder
         */
        public Builder recoveryMaxBackoff(Duration recoveryMaxBackoff) {
            this.recoveryMaxBackoff =
                    OptionChecks.checkAtLeastOneMilli(recoveryMaxBackoff, "recoveryMaxBackoff");
            return this;
        }

        /**
         * Caps the re-apply attempts of the table auto-creation recovery. Defaults to 10.
         *
         * @param recoveryMaxAttempts the maximum attempts, positive
         * @return this builder
         */
        public Builder recoveryMaxAttempts(int recoveryMaxAttempts) {
            Preconditions.checkArgument(
                    recoveryMaxAttempts > 0, "recoveryMaxAttempts must be positive");
            this.recoveryMaxAttempts = recoveryMaxAttempts;
            return this;
        }

        /**
         * Sets how long a table may go without mutations before the writer closes and drops its
         * batcher. Eviction is memory hygiene for long-lived jobs with per-record destinations (for
         * example date-suffixed tables), whose per-table state otherwise grows without bound;
         * correctness is unaffected, and a table that receives a mutation again after eviction
         * rebuilds its batcher transparently. The sweep runs at the end of each successful flush,
         * when nothing is parked or in flight. Defaults to {@link
         * #DEFAULT_DESTINATION_IDLE_TIMEOUT}; to never evict, set a very large duration — up to
         * {@code Duration.ofNanos(Long.MAX_VALUE)}, about 292 years, which is as long as the
         * writer's nanosecond clock can express.
         *
         * <p>A client is not evicted with the table: the sink holds one per (project, instance),
         * shared by that instance's tables, and it is released when the sink closes.
         *
         * @param destinationIdleTimeout the idle timeout, positive and at most {@code
         *     Duration.ofNanos(Long.MAX_VALUE)}
         * @return this builder
         */
        public Builder destinationIdleTimeout(Duration destinationIdleTimeout) {
            OptionChecks.checkPositive(destinationIdleTimeout, "destinationIdleTimeout");
            // This knob's own documentation offers a very large duration as the way to say "never
            // evict", so the ceiling is what keeps that instruction from throwing
            // ArithmeticException from the writer's constructor on a TaskManager, failing the job
            // as it starts rather than here (ADR-0068).
            this.destinationIdleTimeout =
                    OptionChecks.checkExpressibleInNanos(
                            destinationIdleTimeout, "destinationIdleTimeout");
            return this;
        }

        /**
         * Registers per-table {@code recordsSend} and {@code sendErrors} counters beside the
         * writer's totals. Defaults to {@code false}.
         *
         * <p>Off by default because Flink cannot unregister a metric: with per-record destinations
         * the table set is unbounded, so every table the job ever writes to keeps a row in the
         * metric registry for the lifetime of the task — including one whose batcher {@link
         * #destinationIdleTimeout(Duration)} has since evicted. Counters survive eviction: a table
         * seen again resumes its own totals. Switch it on for a sink whose tables are few and
         * known.
         *
         * @param perDestinationMetrics whether to register per-table counters
         * @return this builder
         */
        public Builder perDestinationMetrics(boolean perDestinationMetrics) {
            this.perDestinationMetrics = perDestinationMetrics;
            return this;
        }

        /**
         * Builds the options.
         *
         * <p>Warns, rather than fails, when an in-flight bound is above the client's own
         * flow-control budget. Past that budget the writer's bound is no longer the one that binds:
         * the client blocks the task thread instead, which is what the writer's bounds exist to
         * avoid. It is <em>not</em> refused, because that budget is <b>per client</b> and this sink
         * holds one per (project, instance) — a resolver spreading records over several instances
         * draws on several budgets, so a writer-global bound above one of them can be exactly what
         * such a job means. Nothing here knows how many instances a resolver will name, and the
         * batch thresholds' ceilings are a different case: those the client refuses outright.
         *
         * @return the options
         */
        public BigtableWriterOptions build() {
            Preconditions.checkState(
                    recoveryMaxBackoff.compareTo(recoveryInitialBackoff) >= 0,
                    "recoveryMaxBackoff must be at least recoveryInitialBackoff.");
            // Logged where the options are built rather than in the writer, which would repeat it
            // once per subtask — the Spanner options' placement, for its reasons. What keeps this
            // off a task manager that merely initializes the class is that the defaults satisfy
            // neither condition, which BigtableWriterOptionsTest pins rather than leaves to chance.
            //
            // The comparison is > rather than >=: the client admits exactly its budget and blocks
            // on the request past it (gax's BlockingSemaphore waits while availablePermits <
            // permits), so a bound equal to the budget still binds first.
            if (maxInFlightEntries > CLIENT_MAX_OUTSTANDING_ENTRIES) {
                LOG.warn(
                        "maxInFlightEntries is {}, above the {} entries the Bigtable client's own"
                                + " flow controller admits per client. Past that the client is what"
                                + " bounds the sink, and it *blocks* the task thread rather than"
                                + " yielding to the mailbox, so checkpoint barriers wait behind it."
                                + " Deliberate only if this sink's resolver spreads records over"
                                + " several instances, since that budget is per client and this sink"
                                + " holds one per (project, instance).",
                        maxInFlightEntries,
                        CLIENT_MAX_OUTSTANDING_ENTRIES);
            }
            if (maxInFlightBytes > CLIENT_MAX_OUTSTANDING_BYTES) {
                LOG.warn(
                        "maxInFlightBytes is {}, above the {} bytes (100 MiB) the Bigtable client's"
                                + " own flow controller admits per client. Past that the client is what"
                                + " bounds the sink, and it *blocks* the task thread rather than"
                                + " yielding to the mailbox, so checkpoint barriers wait behind it."
                                + " Deliberate only if this sink's resolver spreads records over"
                                + " several instances, since that budget is per client and this sink"
                                + " holds one per (project, instance).",
                        maxInFlightBytes,
                        CLIENT_MAX_OUTSTANDING_BYTES);
            }
            return new BigtableWriterOptions(this);
        }
    }
}
