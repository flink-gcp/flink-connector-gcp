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

package io.github.flink.gcp.connector.spanner;

import org.apache.flink.annotation.Internal;

/**
 * Every metric name this connector registers, in one place.
 *
 * <p>Counters name the event that happened ({@code recordsSkipped}), gauges name the state they
 * read ({@code bufferedCells}), and no name takes Flink's {@code num} prefix — a name meaning the
 * same thing in another connector of this project is spelled the same way there.
 *
 * <p>Deliberately absent: the names Flink itself provides through {@code SinkWriterMetricGroup}
 * ({@code numRecordsSend}, {@code numBytesSend}, {@code numRecordsSendErrors}), and the templated
 * leaves of the shared {@code base.metrics} subgroups ({@code errorClass.CODE.errors}) — neither is
 * this connector's to name.
 */
@Internal
public final class SpannerMetricNames {

    // Registered by the sink writer (SpannerWriterMetrics).

    /** Mutations held in the writer's batch, waiting for the next flush. */
    public static final String BUFFERED_MUTATIONS = "bufferedMutations";

    /**
     * Mutation cells held in the writer's batch, counted the way Spanner counts them against the
     * per-request limit — index entries included.
     */
    public static final String BUFFERED_CELLS = "bufferedCells";

    /** Estimated bytes of the mutations held in the writer's batch. */
    public static final String BUFFERED_BYTES = "bufferedBytes";

    /** Records the serializer returned {@code null} for, which the sink wrote nowhere. */
    public static final String RECORDS_SKIPPED = "recordsSkipped";

    /**
     * Mutations re-sent after a transient failure. Counted per re-send, so one mutation retried
     * three times contributes three — the question it answers is how much work the retry loop is
     * doing, not how many mutations were unlucky.
     */
    public static final String MUTATIONS_RETRIED = "mutationsRetried";

    /** Batch write requests the writer sent, first attempts and re-sends alike. */
    public static final String BATCHES_SENT = "batchesSent";

    private SpannerMetricNames() {}
}
