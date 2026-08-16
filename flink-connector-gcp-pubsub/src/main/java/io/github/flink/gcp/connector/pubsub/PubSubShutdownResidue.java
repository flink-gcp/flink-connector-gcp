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

package io.github.flink.gcp.connector.pubsub;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import java.util.concurrent.atomic.LongAdder;

/**
 * What this connector's bounded teardowns leave behind, counted for the lifetime of the class
 * loader rather than of a task.
 *
 * <p><b>Why it outlives the task.</b> A publisher whose shutdown overruns its budget is left to a
 * background thread, and the interesting quantity is how often that happens <em>across</em> restart
 * attempts. A per-attempt counter cannot report it: measured with a MiniCluster probe whose
 * reporter ran at 10 ms — Flink's default is 10 s — a metric the sink writer incremented only in
 * {@code close()} was never once seen above zero across four runs, because the writer's metric
 * group is unregistered as its task is cleaned up, in the same instant. Only the next attempt's
 * writer can report what the previous ones left, so the count has to survive between them.
 *
 * <p><b>Not every count here is a stranded resource.</b> The source's unreported-failure count is a
 * teardown <em>outcome</em> nothing consumed rather than a thread left running, and it is here for
 * the reason above: most of its increments happen in a reader's {@code close()}, where a
 * per-attempt counter is unregistered before a reporter reads it.
 *
 * <p><b>Not every increment needs that, and it does not change the answer.</b> Parking a paused
 * split tears one subscriber down on a job that keeps running, so that increment would be scraped
 * from a per-attempt counter too. One metric name has one storage, and the increments that would
 * otherwise be lost are the ones that decide which.
 *
 * <p><b>Why it lives here and not in {@code BoundedShutdown}.</b> That class is shared main code
 * and client-agnostic; a count held there would be one number for every client it ever serves, and
 * a metric named for one of them would silently include the rest. The nearest such client is not
 * another connector but this connector's own <em>source</em>, whose subscriber teardown has the
 * same shape. Each owner holds its own, which keeps the names true by construction: the subscriber
 * counts below are fields of their own rather than second meanings for the publisher's.
 *
 * <p><b>Read the value with the deployment in mind.</b> It is scoped to whichever class loader
 * loaded this class: a job's own jar gets Flink's per-job loader, so the count is that job's; the
 * SQL uber-jar is documented to go in {@code lib/}, where the system loader owns it and the count
 * is TaskManager-wide across every job and never resets. A resubmitted job gets a fresh loader and
 * a zero while any stranded threads remain, so zero does not mean clean.
 */
@Internal
public final class PubSubShutdownResidue {

    /**
     * Publisher closes that overran their shutdown budget, reported as {@code
     * PubSubMetricNames#PUBLISHER_SHUTDOWNS_ABANDONED}. Counts closes, not threads still running:
     * once a close gives up, the background thread exits as soon as the client's own shutdown
     * returns.
     */
    public static final LongAdder PUBLISHER_SHUTDOWNS_ABANDONED = new LongAdder();

    /**
     * The same for the dead-letter queue's publisher, reported as {@code
     * PubSubMetricNames#DEAD_LETTER_PUBLISHER_SHUTDOWNS_ABANDONED}. A second field rather than a
     * second meaning for the one above, which is the rule this class already states for a new
     * owner: the queue's metrics are registered on the <em>host</em> sink writer's group, and a
     * host that is itself a Pub/Sub sink has already registered the name above there — Flink keeps
     * the metric registered first and drops the other with a "Metric will not be reported" warning,
     * so sharing a name would make a healthy configuration log one. Reading them apart also answers
     * which publisher is stalling, which one total cannot.
     */
    public static final LongAdder DEAD_LETTER_PUBLISHER_SHUTDOWNS_ABANDONED = new LongAdder();

    /**
     * Subscriber teardowns whose wait for termination expired, reported as {@code
     * PubSubMetricNames#SUBSCRIBER_SHUTDOWNS_ABANDONED} — the source's counterpart to the sink's
     * first field, and named the same way because it means the same thing. Counts subscriber
     * teardowns rather than reader closes: a reader owns one subscriber per split, and parking a
     * paused split closes one on its own.
     */
    public static final LongAdder SUBSCRIBER_SHUTDOWNS_ABANDONED = new LongAdder();

    /**
     * Failures a subscriber's teardown surfaced that nothing else reports, reported as {@code
     * PubSubMetricNames#SUBSCRIBER_FAILURES_UNREPORTED}. Deliberately not the same field as the one
     * above: an expired wait is a tuning signal, while this is an incident nothing but a log line
     * would otherwise record, and one number would bury it under the other.
     */
    public static final LongAdder SUBSCRIBER_FAILURES_UNREPORTED = new LongAdder();

    private PubSubShutdownResidue() {}

    /**
     * Clears every count, so a test can assert an absolute value instead of a delta. Safe because a
     * fork runs its test classes sequentially and every increment is on the thread calling {@code
     * close()} — never on the teardown's own thread.
     */
    @VisibleForTesting
    public static void resetForTests() {
        PUBLISHER_SHUTDOWNS_ABANDONED.reset();
        DEAD_LETTER_PUBLISHER_SHUTDOWNS_ABANDONED.reset();
        SUBSCRIBER_SHUTDOWNS_ABANDONED.reset();
        SUBSCRIBER_FAILURES_UNREPORTED.reset();
    }
}
