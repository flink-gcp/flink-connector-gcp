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

package io.github.flink.gcp.connector.base.metrics;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Optional per-destination send counters, as {@code destination.NAME.recordsSend} and {@code
 * destination.NAME.sendErrors} on the writer's metric group, where {@code NAME} is the destination
 * as the connector's docs page spells it.
 *
 * <p><b>Opt-in, and off by default, because Flink cannot unregister a metric.</b> A sink writing to
 * per-record destinations has an unbounded destination set — a topic per tenant, a table per day —
 * so registering a subgroup per destination would grow the metric registry for the lifetime of the
 * task and undo the eviction hygiene the writers themselves practise. Each connector exposes the
 * switch as {@code perDestinationMetrics} on its own options object.
 *
 * <p>Entries are never removed, which is the deliberate consequence: a destination whose writer
 * state was evicted and later rebuilt reuses the counters it had, so its totals stay continuous
 * instead of restarting at zero. Nothing else could be true — an unregistered metric cannot be
 * re-registered under the same name.
 *
 * <p>Call sites hold a {@link Counters} handle rather than passing a destination name per record:
 * the name is then composed once per destination instead of once per record, and a disabled
 * instance costs the writer nothing beyond two null checks. {@link Counters} is safe to cache
 * alongside the writer's own per-destination state.
 *
 * <p><b>Task thread only</b>, for the reason {@link ErrorClassCounters} records.
 */
@Internal
public final class DestinationMetrics {

    /** Group name carrying the destination, so the destination is a metric variable, not a name. */
    public static final String DESTINATION_GROUP = "destination";

    /** Counter name for records handed to the client for a destination. */
    public static final String RECORDS_SEND = "recordsSend";

    /** Counter name for records routed to the failure handler for a destination. */
    public static final String SEND_ERRORS = "sendErrors";

    private static final Counters DISABLED = new Counters(null, null);

    /** {@code null} when per-destination metrics are switched off. */
    @Nullable private final MetricGroup metricGroup;

    private final Map<String, Counters> byDestination = new HashMap<>();

    private DestinationMetrics(@Nullable MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
    }

    /**
     * Creates the per-destination counters.
     *
     * @param metricGroup the sink writer's metric group
     * @param enabled whether the connector's {@code perDestinationMetrics} option is set; when
     *     false, nothing is ever registered and {@link #forDestination} always returns a no-op
     * @return the counters
     */
    public static DestinationMetrics of(MetricGroup metricGroup, boolean enabled) {
        return new DestinationMetrics(enabled ? metricGroup : null);
    }

    /**
     * Returns the counters for a destination, registering them on first use. The result is stable
     * per destination name and is meant to be cached by the caller.
     *
     * @param destination the destination name, as the connector's docs page spells it
     * @return the destination's counters, or a no-op when the option is off
     */
    public Counters forDestination(String destination) {
        if (metricGroup == null) {
            return DISABLED;
        }
        return byDestination.computeIfAbsent(
                destination,
                name -> {
                    MetricGroup group = metricGroup.addGroup(DESTINATION_GROUP, name);
                    return new Counters(group.counter(RECORDS_SEND), group.counter(SEND_ERRORS));
                });
    }

    /** One destination's counters; a no-op instance when per-destination metrics are off. */
    public static final class Counters {

        @Nullable private final Counter recordsSend;
        @Nullable private final Counter sendErrors;

        private Counters(@Nullable Counter recordsSend, @Nullable Counter sendErrors) {
            this.recordsSend = recordsSend;
            this.sendErrors = sendErrors;
        }

        /** Counts one record handed to the client library for this destination. */
        public void recordSent() {
            recordsSent(1);
        }

        /**
         * Counts the records of one request handed to the client library for this destination — the
         * batching connectors' form of {@link #recordSent()}, which is the same counter.
         *
         * @param count the number of records the request carries
         */
        public void recordsSent(long count) {
            if (recordsSend != null) {
                recordsSend.inc(count);
            }
        }

        /** Counts one record routed to the failure handler for this destination. */
        public void sendFailed() {
            if (sendErrors != null) {
                sendErrors.inc();
            }
        }
    }
}
