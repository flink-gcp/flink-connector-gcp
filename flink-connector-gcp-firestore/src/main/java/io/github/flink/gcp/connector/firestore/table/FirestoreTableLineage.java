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

package io.github.flink.gcp.connector.firestore.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;

import io.github.flink.gcp.connector.base.lineage.ResourceIdentifier;
import io.github.flink.gcp.connector.base.lineage.internal.Lineage;
import io.github.flink.gcp.connector.base.lineage.internal.LineageIdentifiers;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.sink.CrossVersionSink;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * The physical identity of a {@code firestore} table, carried from the factory to the runtime
 * providers it wraps (ADR-0160): the collection the table names, in its database, reported under
 * the table's catalog name.
 */
@Internal
public final class FirestoreTableLineage implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String logicalName;
    private final String namespace;
    private final List<ResourceIdentifier> resources;

    private FirestoreTableLineage(
            String logicalName, String namespace, List<ResourceIdentifier> resources) {
        this.logicalName = logicalName;
        this.namespace = namespace;
        this.resources = List.copyOf(resources);
    }

    /**
     * Captures the configured collection, before any runtime client exists.
     *
     * @param logicalName the table's catalog name
     * @param database the database the table is in
     * @param collection the collection path the table names
     * @return the lineage
     */
    public static FirestoreTableLineage of(
            String logicalName, DatabaseDestination database, String collection) {
        return new FirestoreTableLineage(
                logicalName,
                "firestore://" + database.getProject() + "/" + database.getDatabaseId(),
                List.of(
                        LineageIdentifiers.firestoreCollection(
                                database.getProject(), database.getDatabaseId(), collection)));
    }

    /**
     * Wraps a sink so that it reports this table's lineage, keeping its writer creation path.
     *
     * @param sink the sink
     * @param <T> the sink's input type
     * @return the wrapped sink
     */
    public <T> Sink<T> sink(Sink<T> sink) {
        return new TableSink<>(sink, this);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof FirestoreTableLineage)) {
            return false;
        }
        FirestoreTableLineage that = (FirestoreTableLineage) other;
        return logicalName.equals(that.logicalName)
                && namespace.equals(that.namespace)
                && resources.equals(that.resources);
    }

    @Override
    public int hashCode() {
        return Objects.hash(logicalName, namespace, resources);
    }

    static final class TableSink<T> implements CrossVersionSink<T>, LineageVertexProvider {
        private static final long serialVersionUID = 1L;
        @VisibleForTesting final Sink<T> delegate;
        private final FirestoreTableLineage metadata;

        private TableSink(Sink<T> delegate, FirestoreTableLineage metadata) {
            this.delegate = delegate;
            this.metadata = metadata;
        }

        @Override
        public LineageVertex getLineageVertex() {
            return Lineage.tableSink(metadata.logicalName, metadata.namespace, metadata.resources);
        }

        @Override
        public SinkWriter<T> createWriter(WriterInitContext context) throws IOException {
            return delegate.createWriter(context);
        }
    }
}
