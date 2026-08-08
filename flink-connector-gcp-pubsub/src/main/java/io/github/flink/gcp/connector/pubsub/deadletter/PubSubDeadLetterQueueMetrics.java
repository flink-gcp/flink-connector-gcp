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

package io.github.flink.gcp.connector.pubsub.deadletter;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.pubsub.AbandonedShutdownsCounter;
import io.github.flink.gcp.connector.pubsub.PubSubMetricNames;
import io.github.flink.gcp.connector.pubsub.PubSubShutdownResidue;

import java.util.function.IntSupplier;

/**
 * The dead-letter queue's metrics, registered on the metric group {@code FailureHandlerContext}
 * hands the queue — <b>the host sink writer's</b>, whichever connector that sink belongs to. So a
 * BigQuery job dead-lettering to a topic reports these beside BigQuery's own names, which is why
 * each of them carries {@code deadLetter}.
 *
 * <p><b>What is deliberately not here is the offered count.</b> Every sink in this repository
 * increments {@code numRecordsSendErrors} immediately before calling its failure handler, so under
 * {@code sendToDeadLetterQueue(...)} that standard counter already reports exactly what this queue
 * was offered, on this very group. What nothing reported before is how much of it the service
 * confirmed, and how long the waits for those confirmations take — which is what these are.
 *
 * <p>Read as a triple: {@code numRecordsSendErrors} (offered) → {@code outstandingDeadLetters}
 * (handed over, unconfirmed) → {@code deadLettersPublished} (confirmed).
 *
 * <p><b>Task thread only for the writes</b>, like the sink's own metrics: every call here happens
 * inside {@code offer} or a flush. The reporter thread reads them, which is why the flush duration
 * is {@code volatile} — a non-volatile {@code long} may be read torn, where the {@code int} state
 * the sink writer's gauges expose cannot be.
 */
@Internal
final class PubSubDeadLetterQueueMetrics {

    private final Counter deadLettersPublished;

    /**
     * The most recent completed wait, in milliseconds. Volatile for the tearing reason in the class
     * javadoc, and plain rather than a {@code Counter} because it is a state read against {@code
     * flushTimeout}: a cumulative total could not tell one wait that spent the whole budget from
     * many short ones, which is the comparison the budget invites.
     */
    private volatile long flushMillis;

    /**
     * The longest wait this writer has seen, in milliseconds, which is the one {@link #flushMillis}
     * cannot keep: waits happen as often as the queue drains — once per <em>element</em> under
     * {@code WRITE_THROUGH} — so a slow one is overwritten long before a reporter reads it (#405).
     *
     * <p>Written on the task thread only, so the compare-and-assign in {@link #flushCompleted} is
     * safe without a CAS; {@code volatile} for the same tearing reason as its sibling.
     *
     * <p>It never falls, and its scope is the <b>task attempt</b> rather than the class loader —
     * this is writer state, unlike the shutdown residue — so a restart starts it over. That is the
     * right scope for "how bad did it get on this attempt", and it is why a value that stays high
     * is not a stuck metric.
     */
    private volatile long longestFlushMillis;

    /**
     * Registers the queue's metrics.
     *
     * @param metricGroup the host sink writer's metric group
     * @param outstanding reads the publishes handed over and not yet resolved
     */
    PubSubDeadLetterQueueMetrics(MetricGroup metricGroup, IntSupplier outstanding) {
        Preconditions.checkNotNull(metricGroup, "metricGroup must not be null");
        Preconditions.checkNotNull(outstanding, "outstanding must not be null");
        this.deadLettersPublished = metricGroup.counter(PubSubMetricNames.DEAD_LETTERS_PUBLISHED);
        metricGroup.gauge(
                PubSubMetricNames.OUTSTANDING_DEAD_LETTERS, (Gauge<Integer>) outstanding::getAsInt);
        metricGroup.gauge(
                PubSubMetricNames.DEAD_LETTER_FLUSH_MILLIS, (Gauge<Long>) () -> flushMillis);
        metricGroup.gauge(
                PubSubMetricNames.LONGEST_DEAD_LETTER_FLUSH_MILLIS,
                (Gauge<Long>) () -> longestFlushMillis);
        // The queue's own residue, under a name of its own: this group may already carry the
        // sink's `publisherShutdownsAbandoned`, and Flink resolves a collision by dropping the
        // later registration with a warning rather than by failing.
        metricGroup.counter(
                PubSubMetricNames.DEAD_LETTER_PUBLISHER_SHUTDOWNS_ABANDONED,
                new AbandonedShutdownsCounter(
                        PubSubShutdownResidue.DEAD_LETTER_PUBLISHER_SHUTDOWNS_ABANDONED));
    }

    /** Counts one dead letter the service confirmed. */
    void deadLetterPublished() {
        deadLettersPublished.inc();
    }

    /**
     * Records how long a wait for the buffered publishes took, whether it resolved them all or ran
     * out of its budget. An expiry fails the job, so that value is rarely scraped before the metric
     * group is torn down — it is recorded anyway rather than only on success, because a value that
     * silently skipped the interesting case would be worse than one that is merely hard to catch.
     *
     * <p>The caller does not call this for a flush that had nothing buffered: {@code flush()} runs
     * at every checkpoint barrier, so on a job that dead-letters occasionally those calls would
     * otherwise overwrite the slow wait this exists to show with a zero.
     *
     * <p>It feeds both duration gauges: the last wait, and the longest one this writer has seen —
     * which is what survives the next fast wait overwriting the first.
     *
     * @param millis the elapsed time of the wait
     */
    void flushCompleted(long millis) {
        flushMillis = millis;
        if (millis > longestFlushMillis) {
            longestFlushMillis = millis;
        }
    }
}
