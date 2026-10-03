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

package io.github.flink.gcp.connector.datastore.source;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Entry point for building a bounded Datastore source.
 *
 * <p>The source reads a Firestore database in Datastore mode at one snapshot time and finishes. It
 * reads a kind, a query or a GQL query; a kind, and a query that filters only by equality and
 * ancestry, are cut into key ranges read in parallel, and any other query is read as one split.
 * Every split reads at the same read time, so the job sees the database as it stood at one instant,
 * and a restore resumes each split after the last entity it passed.
 *
 * <p>Example:
 * <!-- javadoc-example file="JavadocDatastoreExamples.java" tag="source" -->
 *
 * <pre>{@code
 * Source<Order, ?, ?> source =
 *         DatastoreSource.<Order>builder()
 *                 .database(DatabaseDestination.of("my-project"))
 *                 .kind("Order")
 *                 .deserializer(new OrderEntityDeserializer())
 *                 .build();
 * }</pre>
 *
 * <p>The builder-returned source implements {@code LineageVertexProvider}. A kind, and a query
 * naming exactly one kind, report the kind with namespace {@code datastore://{project}/{database}}
 * (followed by {@code /{namespace}} outside the default namespace), name {@code {kind}} and a
 * {@code gcp} physical-resource facet; a query naming no kind or several, and a GQL query, which is
 * parsed only when the read is planned, report no dataset. Extraction opens no client. The
 * supported Flink 2.x versions extract the metadata automatically; Flink 1.20 supports direct
 * inspection only.
 */
@PublicEvolving
public final class DatastoreSource {

    private DatastoreSource() {}

    /**
     * Returns a builder for a bounded Datastore source.
     *
     * @param <T> the record type produced
     * @return the builder
     */
    public static <T> DatastoreSourceBuilder<T> builder() {
        return new DatastoreSourceBuilder<>();
    }
}
