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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;

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
 * providers it wraps (ADR-0160): the collection the table names, in its database, or for a
 * collection-group scan the group, reported under the table's catalog name.
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
        return of(
                logicalName,
                LineageIdentifiers.firestoreCollection(
                        database.getProject(), database.getDatabaseId(), collection));
    }

    private static FirestoreTableLineage of(String logicalName, ResourceIdentifier resource) {
        return new FirestoreTableLineage(logicalName, resource.namespace(), List.of(resource));
    }

    /**
     * Captures a collection-group scan's group, before any runtime client exists.
     *
     * @param logicalName the table's catalog name
     * @param database the database the table is in
     * @param collectionGroup the collection id every collection of the group has
     * @return the lineage
     */
    public static FirestoreTableLineage ofCollectionGroup(
            String logicalName, DatabaseDestination database, String collectionGroup) {
        return of(
                logicalName,
                LineageIdentifiers.firestoreCollectionGroup(
                        database.getProject(), database.getDatabaseId(), collectionGroup));
    }

    /**
     * Wraps a source so that it reports this table's lineage, keeping its runtime operations and
     * produced type.
     *
     * @param source the source
     * @param type the produced type
     * @param <T> the produced type
     * @param <S> the split type
     * @param <E> the enumerator checkpoint type
     * @return the wrapped source
     */
    public <T, S extends SourceSplit, E> Source<T, S, E> source(
            Source<T, S, E> source, TypeInformation<T> type) {
        return new TableSource<>(source, type, this);
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

    static final class TableSource<T, S extends SourceSplit, E>
            implements Source<T, S, E>, ResultTypeQueryable<T>, LineageVertexProvider {
        private static final long serialVersionUID = 1L;
        @VisibleForTesting final Source<T, S, E> delegate;
        private final TypeInformation<T> type;
        private final FirestoreTableLineage metadata;

        private TableSource(
                Source<T, S, E> delegate, TypeInformation<T> type, FirestoreTableLineage metadata) {
            this.delegate = delegate;
            this.type = type;
            this.metadata = metadata;
        }

        @Override
        public SourceLineageVertex getLineageVertex() {
            return Lineage.tableSource(
                    metadata.logicalName, metadata.namespace, getBoundedness(), metadata.resources);
        }

        @Override
        public Boundedness getBoundedness() {
            return delegate.getBoundedness();
        }

        @Override
        public TypeInformation<T> getProducedType() {
            return type;
        }

        @Override
        public SourceReader<T, S> createReader(SourceReaderContext context) throws Exception {
            return delegate.createReader(context);
        }

        @Override
        public SplitEnumerator<S, E> createEnumerator(SplitEnumeratorContext<S> context)
                throws Exception {
            return delegate.createEnumerator(context);
        }

        @Override
        public SplitEnumerator<S, E> restoreEnumerator(
                SplitEnumeratorContext<S> context, E checkpoint) throws Exception {
            return delegate.restoreEnumerator(context, checkpoint);
        }

        @Override
        public SimpleVersionedSerializer<S> getSplitSerializer() {
            return delegate.getSplitSerializer();
        }

        @Override
        public SimpleVersionedSerializer<E> getEnumeratorCheckpointSerializer() {
            return delegate.getEnumeratorCheckpointSerializer();
        }
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
