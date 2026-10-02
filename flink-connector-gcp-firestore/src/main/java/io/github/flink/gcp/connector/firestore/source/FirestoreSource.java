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

package io.github.flink.gcp.connector.firestore.source;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Entry point for building a bounded Firestore source.
 *
 * <p>The source reads a Firestore database in Native mode at one snapshot time and finishes. It
 * reads either a whole collection group, cut into partitions the service plans, or one query of
 * your own, as one split. Every split reads at the same read time, so the job sees the database as
 * it stood at one instant, and a restore resumes each split after the last document it passed.
 *
 * <p>Example:
 * <!-- javadoc-example file="JavadocFirestoreExamples.java" tag="source" -->
 *
 * <pre>{@code
 * Source<Order, ?, ?> source =
 *         FirestoreSource.<Order>builder()
 *                 .database(DatabaseDestination.of("my-project"))
 *                 .collectionGroup("orders")
 *                 .deserializer(new OrderDeserializer())
 *                 .build();
 * }</pre>
 *
 * <p>The builder-returned source implements {@code LineageVertexProvider}. A collection-group scan
 * reports the group with namespace {@code firestore://{project}/{database}}, name {@code
 * {collectionGroup}} and a {@code gcp} physical-resource facet; a query reports no dataset, because
 * its collections are the factory's and extraction never calls it. Extraction opens no client. The
 * supported Flink 2.x versions extract the metadata automatically; Flink 1.20 supports direct
 * inspection only.
 */
@PublicEvolving
public final class FirestoreSource {

    private FirestoreSource() {}

    /**
     * Returns a builder for a bounded Firestore source.
     *
     * @param <T> the record type produced
     * @return the builder
     */
    public static <T> FirestoreSourceBuilder<T> builder() {
        return new FirestoreSourceBuilder<>();
    }
}
