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

package io.github.flink.gcp.connector.spanner.source;

import org.apache.flink.annotation.Public;

/**
 * Reads rows from a Cloud Spanner database.
 * <!-- javadoc-example file="JavadocSpannerExamples.java" tag="source" -->
 *
 * <pre>{@code
 * Source<Singer, ?, ?> source =
 *         SpannerSource.<Singer>builder()
 *                 .database(DatabaseDestination.of("my-project", "my-instance", "my-db"))
 *                 .readOperation(
 *                         SpannerReadOperation.query(
 *                                 Statement.of("SELECT id, name FROM singers")))
 *                 .deserializer(mySingerDeserializer)
 *                 .build();
 * }</pre>
 *
 * <p>The read is bounded: the source reads the rows the operation names, at one snapshot, and
 * finishes. That is not the same as batch-only — a bounded source runs inside a streaming pipeline
 * and simply ends, which is what makes reading a Spanner table and joining it against an unbounded
 * stream work.
 *
 * <p><b>Every subtask reads the same snapshot.</b> The source asks Spanner to divide the read into
 * partitions at one timestamp, and each subtask rejoins that transaction to read the partitions it
 * was given. There is no column to split on and no bounds to supply: where the divisions fall is
 * the service's decision, made from how the data is actually stored.
 */
@Public
public final class SpannerSource {

    private SpannerSource() {}

    /**
     * Returns a builder for a Spanner batch source.
     *
     * @param <T> the record type produced
     * @return the builder
     */
    public static <T> SpannerSourceBuilder<T> builder() {
        return new SpannerSourceBuilder<>();
    }
}
