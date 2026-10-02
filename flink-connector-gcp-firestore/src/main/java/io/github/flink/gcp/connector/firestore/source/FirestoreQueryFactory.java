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

import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;

import java.io.Serializable;

/**
 * Builds the query a {@link FirestoreSource} reads, from the client the source opened.
 *
 * <p>A factory rather than a {@link Query}, because a query belongs to a client instance and cannot
 * travel in a job graph. The source calls it on the JobManager when it plans the read — once,
 * unless the job restarts before a checkpoint has recorded the plan; the query's serialized form is
 * what reaches the readers. Build the query from the given client: a query addressing another
 * database is refused.
 *
 * <p>Filters, ordering, a projection ({@code select}), cursors, {@code limit} and {@code offset}
 * are all read as the query states them. An {@code offset} is resolved once, when the read is
 * planned, into a position after the documents it skips; the service reads and bills those
 * documents then, as it would for the offset itself. A {@code limitToLast} query reads the same
 * documents in the reverse of its stated order, because the client library carries that reversal
 * outside the query's serialized form.
 *
 * <p>Example:
 * <!-- javadoc-example file="JavadocFirestoreExamples.java" tag="query-factory" -->
 *
 * <pre>{@code
 * FirestoreQueryFactory openOrders =
 *         firestore -> firestore.collection("orders").whereEqualTo("status", "open");
 * }</pre>
 */
@PublicEvolving
@FunctionalInterface
public interface FirestoreQueryFactory extends Serializable {

    /**
     * Builds the query to read.
     *
     * @param firestore the client the source planned the read with; build the query from it
     * @return the query, never {@code null}
     */
    Query create(Firestore firestore);
}
