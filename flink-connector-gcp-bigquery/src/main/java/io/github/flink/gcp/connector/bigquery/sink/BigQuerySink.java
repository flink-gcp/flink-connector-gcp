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

package io.github.flink.gcp.connector.bigquery.sink;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Entry point for building a BigQuery sink.
 *
 * <p>The sink exposes a single builder-based API and dispatches, at job-graph construction time, to
 * one of several write-method implementations, in the spirit of Apache Beam's {@code BigQueryIO}:
 *
 * <ul>
 *   <li>{@link WriteMethod#STORAGE_API_AT_LEAST_ONCE} — BigQuery Storage Write API default stream,
 *       at-least-once semantics, supporting dynamic per-record table destinations
 *   <li>{@link WriteMethod#STORAGE_API_EXACTLY_ONCE} — BigQuery Storage Write API buffered streams
 *       with two-phase commit, exactly-once semantics
 *   <li>{@link WriteMethod#FILE_LOADS} — files staged on Cloud Storage followed by BigQuery load
 *       jobs, exactly-once in batch and checkpointed streaming execution
 * </ul>
 *
 * <p>Those semantics assume the default {@code FailureHandler.failJob()} policy. Under a dropping
 * policy configured through {@link BigQuerySinkBuilder#failureHandler}, they cover every record
 * except those handed to that handler, which are never written at all. A record the serializer
 * skips by returning {@code null} is written nowhere either, under any policy.
 *
 * <p>Write methods that are not implemented yet are rejected by {@link BigQuerySinkBuilder#build()}
 * with an {@link UnsupportedOperationException}.
 *
 * <p>Example:
 * <!-- javadoc-example file="JavadocBigQueryExamples.java" tag="sink" -->
 *
 * <pre>{@code
 * Sink<MyEvent> sink =
 *         BigQuerySink.<MyEvent>builder()
 *                 .writeMethod(WriteMethod.STORAGE_API_AT_LEAST_ONCE)
 *                 .destinationResolver(
 *                         (e, ctx) ->
 *                                 TableDestination.of(
 *                                         "my-project", "my_dataset", e.tableName()))
 *                 .serializer(new MyEventProtoSerializer())
 *                 .build();
 * }</pre>
 */
@PublicEvolving
public final class BigQuerySink {

    private BigQuerySink() {}

    /**
     * Creates a new {@link BigQuerySinkBuilder}.
     *
     * @param <T> type of the records written by the sink
     * @return a new builder
     */
    public static <T> BigQuerySinkBuilder<T> builder() {
        return new BigQuerySinkBuilder<>();
    }
}
