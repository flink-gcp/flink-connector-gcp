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

package io.github.flink.gcp.connector.bigtable.sink.singlerow.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import io.github.flink.gcp.connector.base.metrics.DestinationMetrics;
import io.github.flink.gcp.connector.base.metrics.ErrorClassCounters;
import io.github.flink.gcp.connector.bigtable.BigtableMetricNames;
import io.github.flink.gcp.connector.bigtable.TableDestination;

import javax.annotation.Nullable;

import java.util.function.Supplier;

/**
 * The single-row runtime's counters and gauges, registered on a sink writer's group or an async
 * function's operator group.
 *
 * <p>Counts are of requests, one per record: a single-row RPC carries exactly one row. A request is
 * {@code accepted} once the client took it, {@code completed} when the service answered it, {@code
 * failed} when it did not — including the ambiguous failures, since each is a distinct give-up
 * rather than an attempt — and {@code timedOut} on top of {@code failed} when the failure was the
 * request's own deadline.
 *
 * <p>A successful conditional response also counts either {@code predicatesMatched} or {@code
 * predicatesNotMatched}, and {@code emptyBranchesSelected} when its selected list is empty. These
 * counters and {@code requestsCompleted} advance before the empty-branch policy runs; a policy
 * failure does not count as a failed RPC.
 *
 * <p>The sink surface also moves Flink's standard {@code numRecordsSend} and {@code
 * numRecordsSendErrors}, with the meaning every sink in this repository gives them ({@code
 * docs/adr/0037}): a record is sent when it is first handed to the client — accepted, here — and a
 * send error is a record routed to the failure handler, whatever the handler then did with it. A
 * failure that fails the job is under {@code requestsFailed} and {@code errorClass} but not there,
 * exactly as the batching sink's is not, so a dashboard built on the two standard names reads the
 * two Bigtable sink families alike. The per-table counters follow the standard pair. An operator
 * group has no standard counters, and the async surface routes nothing.
 *
 * <p>The counter type is the caller's: the sink writer counts from the task thread only and takes
 * the default, while the async function counts from gax threads and passes a thread-safe one.
 */
@Internal
public final class SingleRowRequestMetrics {

    private final MetricGroup metricGroup;
    @Nullable private final Counter numRecordsSend;
    @Nullable private final Counter numRecordsSendErrors;
    private final Counter requestsAccepted;
    private final Counter requestsCompleted;
    private final Counter predicatesMatched;
    private final Counter predicatesNotMatched;
    private final Counter emptyBranchesSelected;
    private final Counter requestsFailed;
    private final Counter requestsTimedOut;
    private final Counter recordsSkipped;
    private final Counter capacityEvictions;
    private final Counter idleEvictions;
    private final ErrorClassCounters errorClasses;
    private final DestinationMetrics destinations;

    private SingleRowRequestMetrics(
            MetricGroup metricGroup,
            @Nullable Counter numRecordsSend,
            @Nullable Counter numRecordsSendErrors,
            boolean perDestinationMetrics,
            Supplier<? extends Counter> counters) {
        this.metricGroup = metricGroup;
        this.numRecordsSend = numRecordsSend;
        this.numRecordsSendErrors = numRecordsSendErrors;
        // Each name is spelled at its registration: the metric-docs checker reads the
        // registrations, and a helper taking the name as a parameter is invisible to it.
        this.requestsAccepted =
                metricGroup.counter(BigtableMetricNames.REQUESTS_ACCEPTED, counters.get());
        this.requestsCompleted =
                metricGroup.counter(BigtableMetricNames.REQUESTS_COMPLETED, counters.get());
        this.requestsFailed =
                metricGroup.counter(BigtableMetricNames.REQUESTS_FAILED, counters.get());
        this.requestsTimedOut =
                metricGroup.counter(BigtableMetricNames.REQUESTS_TIMED_OUT, counters.get());
        this.recordsSkipped =
                metricGroup.counter(BigtableMetricNames.RECORDS_SKIPPED, counters.get());
        this.capacityEvictions =
                metricGroup.counter(BigtableMetricNames.CAPACITY_EVICTIONS, counters.get());
        this.idleEvictions =
                metricGroup.counter(BigtableMetricNames.IDLE_EVICTIONS, counters.get());
        this.predicatesMatched =
                metricGroup.counter(BigtableMetricNames.PREDICATES_MATCHED, counters.get());
        this.predicatesNotMatched =
                metricGroup.counter(BigtableMetricNames.PREDICATES_NOT_MATCHED, counters.get());
        this.emptyBranchesSelected =
                metricGroup.counter(BigtableMetricNames.EMPTY_BRANCHES_SELECTED, counters.get());
        this.errorClasses = new ErrorClassCounters(metricGroup, counters);
        this.destinations = DestinationMetrics.of(metricGroup, perDestinationMetrics, counters);
    }

    /**
     * Creates the metrics of a sink writer, which counts from the task thread only.
     *
     * @param metricGroup the writer's group
     * @param perDestinationMetrics whether per-table counters are registered
     * @return the metrics
     */
    public static SingleRowRequestMetrics forSink(
            SinkWriterMetricGroup metricGroup, boolean perDestinationMetrics) {
        return new SingleRowRequestMetrics(
                metricGroup,
                metricGroup.getNumRecordsSendCounter(),
                metricGroup.getNumRecordsSendErrorsCounter(),
                perDestinationMetrics,
                SimpleCounter::new);
    }

    /**
     * Creates the metrics of an operator, with the counter type its threads need.
     *
     * @param metricGroup the operator's group
     * @param perDestinationMetrics whether per-table counters are registered
     * @param counters creates each counter before it is registered under its name; thread-safe when
     *     increments arrive from more than one thread
     * @return the metrics
     */
    public static SingleRowRequestMetrics forOperator(
            MetricGroup metricGroup,
            boolean perDestinationMetrics,
            Supplier<? extends Counter> counters) {
        return new SingleRowRequestMetrics(
                metricGroup, null, null, perDestinationMetrics, counters);
    }

    /**
     * Registers the gauges over the runtime's live state.
     *
     * @param inFlightRequests requests accepted and not yet answered
     * @param activeClients instance clients held
     */
    public void bindState(Gauge<Integer> inFlightRequests, Gauge<Integer> activeClients) {
        metricGroup.gauge(BigtableMetricNames.IN_FLIGHT_REQUESTS, inFlightRequests);
        metricGroup.gauge(BigtableMetricNames.ACTIVE_CLIENTS, activeClients);
    }

    /**
     * Returns a table's counters, registering them on first use; a no-op when per-table counters
     * are off.
     *
     * @param destination the table
     * @return the table's counters
     */
    public DestinationMetrics.Counters forTable(TableDestination destination) {
        // toString(), which is project.instance.table — how the docs page spells a destination.
        return destinations.forDestination(destination.toString());
    }

    /**
     * Counts a request the client accepted: the record's first hand-off, which is where {@code
     * numRecordsSend} counts it.
     *
     * @param table the table's counters
     */
    public void requestAccepted(DestinationMetrics.Counters table) {
        requestsAccepted.inc();
        if (numRecordsSend != null) {
            numRecordsSend.inc();
        }
        table.recordSent();
    }

    /** Counts a request the service answered. */
    public void requestCompleted() {
        requestsCompleted.inc();
    }

    /**
     * Counts a request that failed, under its status when it carries one, and under {@code
     * requestsTimedOut} too when the failure was the request's deadline. A record that never became
     * a request — its serializer threw — is counted here with no failure, so under no status.
     *
     * @param throwable the failure, or {@code null} when the record never became a request
     */
    public void requestFailed(@Nullable Throwable throwable) {
        requestsFailed.inc();
        if (throwable == null) {
            return;
        }
        errorClasses.count(RequestFailures.statusCode(throwable));
        if (RequestFailures.isTimeout(throwable)) {
            requestsTimedOut.inc();
        }
    }

    /**
     * Counts a record routed to the failure handler — a record the serializer rejected, a request
     * the client's validation refused, or one the service rejected — which is what {@code
     * numRecordsSendErrors} counts, whether the handler then dropped it or failed the job.
     *
     * @param table the table's counters
     */
    public void requestRouted(DestinationMetrics.Counters table) {
        if (numRecordsSendErrors != null) {
            numRecordsSendErrors.inc();
        }
        table.sendFailed();
    }

    /**
     * Counts a request the runtime itself gave up on at Flink's operator timeout, which carries no
     * status of its own.
     */
    public void requestTimedOut() {
        requestsFailed.inc();
        requestsTimedOut.inc();
    }

    /** Counts a record the serializer skipped. */
    public void recordSkipped() {
        recordsSkipped.inc();
    }

    /** Counts an instance evicted to make room under {@code maxActiveInstances}. */
    public void capacityEviction() {
        capacityEvictions.inc();
    }

    /** Counts an instance whose last table went idle past the idle timeout. */
    public void idleEviction() {
        idleEvictions.inc();
    }

    /**
     * Counts a conditional response before any empty-branch policy is applied.
     *
     * @param matched whether the predicate selected any cells
     * @param hasMutations whether the selected mutation list was nonempty
     */
    public void conditionalOutcome(boolean matched, boolean hasMutations) {
        if (matched) {
            predicatesMatched.inc();
        } else {
            predicatesNotMatched.inc();
        }
        if (!hasMutations) {
            emptyBranchesSelected.inc();
        }
    }
}
