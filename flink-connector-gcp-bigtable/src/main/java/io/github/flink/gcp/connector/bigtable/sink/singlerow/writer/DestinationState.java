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

import io.github.flink.gcp.connector.base.metrics.DestinationMetrics;
import io.github.flink.gcp.connector.bigtable.TableDestination;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * One table's client lease and the bookkeeping that belongs to it alone.
 *
 * <p>Shared by both runtime surfaces, which is why the two mutable fields are written for
 * cross-thread reads: the sink writer touches them from the task thread only, but the async
 * function releases a request from the client thread that answered it. {@code lastAccessNanos} is a
 * timestamp read only as a difference, and {@code inFlight} is what an eviction consults to tell an
 * idle table from a busy one.
 */
@Internal
final class DestinationState {

    final TableDestination destination;
    final String instanceKey;
    final SingleRowClient client;
    final DestinationMetrics.Counters counters;
    final String completionDescription;
    final String failureDescription;

    /**
     * When this table last received a request ({@code nanoClock} time), for idle eviction.
     * Initialized to creation time so a freshly opened table is not instantly idle.
     */
    volatile long lastAccessNanos;

    /** Requests accepted for this table and not yet answered. */
    final AtomicInteger inFlight = new AtomicInteger();

    DestinationState(
            TableDestination destination,
            String instanceKey,
            SingleRowClient client,
            DestinationMetrics.Counters counters,
            long createdNanos) {
        this.destination = destination;
        this.instanceKey = instanceKey;
        this.client = client;
        // Resolved once per destination, not per record: the handle is stable, and composing the
        // table's name per record is what DestinationMetrics exists to avoid.
        this.counters = counters;
        this.completionDescription = "Complete a Bigtable single-row request to " + destination;
        this.failureDescription = "Fail a Bigtable single-row request to " + destination;
        this.lastAccessNanos = createdNanos;
    }
}
