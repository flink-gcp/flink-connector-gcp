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

package io.github.flink.gcp.connector.spanner.table;

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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;

import io.github.flink.gcp.connector.base.lineage.ResourceIdentifier;
import io.github.flink.gcp.connector.base.lineage.internal.Lineage;
import io.github.flink.gcp.connector.base.lineage.internal.LineageIdentifiers;
import io.github.flink.gcp.connector.spanner.SpannerTableName;
import io.github.flink.gcp.connector.spanner.sink.CrossVersionSink;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Immutable physical identities carried from the Table factory to its runtime providers. */
@Internal
public final class SpannerTableLineage implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String logicalName;
    private final String namespace;
    private final List<ResourceIdentifier> resources;

    private SpannerTableLineage(
            String logicalName, String namespace, List<ResourceIdentifier> resources) {
        this.logicalName = logicalName;
        this.namespace = namespace;
        this.resources = List.copyOf(resources);
    }

    /** Captures only resource options, before pushdown or runtime client creation. */
    public static SpannerTableLineage from(String logicalName, ReadableConfig config) {
        String project = config.get(SpannerConnectorOptions.PROJECT);
        String instance = config.get(SpannerConnectorOptions.INSTANCE);
        String database = config.get(SpannerConnectorOptions.DATABASE);
        String configuredSchema = config.getOptional(SpannerConnectorOptions.SCHEMA).orElse(null);
        String configuredTable = config.get(SpannerConnectorOptions.TABLE);
        SpannerTableName table =
                SpannerTableName.of(
                        configuredSchema,
                        configuredTable,
                        config.get(SpannerConnectorOptions.DIALECT));
        List<ResourceIdentifier> resources = new ArrayList<>();
        resources.add(
                LineageIdentifiers.spannerTable(
                        project,
                        instance,
                        database,
                        table.lineageSchema(),
                        table.table(),
                        configuredSchema,
                        configuredTable));
        if (config.get(SpannerConnectorOptions.SCAN_MODE) == ScanMode.CHANGE_STREAM) {
            resources.add(
                    LineageIdentifiers.spannerChangeStream(
                            project,
                            instance,
                            database,
                            config.get(SpannerConnectorOptions.SCAN_CHANGE_STREAM_NAME)));
        }
        return new SpannerTableLineage(
                logicalName, "spanner://" + project + ":" + instance, resources);
    }

    /** Adapts metadata while retaining the source's runtime operations and produced type. */
    public <T, S extends SourceSplit, E> Source<T, S, E> source(
            Source<T, S, E> source, TypeInformation<T> type) {
        return new TableSource<>(source, type, this);
    }

    /** Adapts metadata while retaining the sink's writer creation path. */
    public <T> Sink<T> sink(Sink<T> sink) {
        return new TableSink<>(sink, this);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof SpannerTableLineage)) {
            return false;
        }
        SpannerTableLineage that = (SpannerTableLineage) other;
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
        private final SpannerTableLineage metadata;

        private TableSource(
                Source<T, S, E> delegate, TypeInformation<T> type, SpannerTableLineage metadata) {
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
        private final SpannerTableLineage metadata;

        private TableSink(Sink<T> delegate, SpannerTableLineage metadata) {
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
