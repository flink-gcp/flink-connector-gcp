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

package io.github.flink.gcp.connector.spanner.sink;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Entry point for building a Spanner sink.
 *
 * <p>The sink applies one mutation per record through {@code batchWriteAtLeastOnce}, at-least-once,
 * and flushes everything it holds at each checkpoint barrier. A mutation names its own table, so
 * one sink writes to as many tables of the configured database as its serializer produces; no table
 * is ever created, and a missing one fails the job.
 *
 * <p><b>Replay is the serializer's problem to make harmless.</b> Spanner's batch write offers no
 * replay protection, so a mutation may be applied more than once — after a job restart, and also
 * within one attempt when a request whose outcome never arrived is retried. An {@code
 * insertOrUpdate}, {@code replace} or {@code delete} mutation is idempotent when that same mutation
 * is replayed; a replayed {@code insert} is rejected with {@code ALREADY_EXISTS}, which is routed
 * to the failure handler as a per-mutation failure. This per-mutation property is not a same-key
 * ordering guarantee: separate {@code BatchWrite} mutation groups may be applied in an unspecified
 * order.
 *
 * <p>That at-least-once statement assumes the default {@code FailureHandler.failJob()} policy.
 * Under a dropping policy configured through {@link SpannerSinkBuilder#failedMutationHandler}, a
 * completed checkpoint means every record up to the barrier was either applied, skipped by the
 * serializer, or handed to that handler.
 *
 * <p>Example:
 * <!-- javadoc-example file="JavadocSpannerExamples.java" tag="sink" -->
 *
 * <pre>{@code
 * Sink<OrderEvent> sink =
 *         SpannerSink.<OrderEvent>builder()
 *                 .database(SpannerDatabase.of("my-project", "my-instance", "orders-db"))
 *                 .serializer(
 *                         (event, context) ->
 *                                 Mutation.newInsertOrUpdateBuilder("Orders")
 *                                         .set("OrderId")
 *                                         .to(event.getId())
 *                                         .set("Total")
 *                                         .to(event.getTotal())
 *                                         .build())
 *                 .build();
 * }</pre>
 */
@PublicEvolving
public final class SpannerSink {

    private SpannerSink() {}

    /**
     * Creates a new {@link SpannerSinkBuilder}.
     *
     * @param <T> type of the records written by the sink
     * @return a new builder
     */
    public static <T> SpannerSinkBuilder<T> builder() {
        return new SpannerSinkBuilder<>();
    }
}
