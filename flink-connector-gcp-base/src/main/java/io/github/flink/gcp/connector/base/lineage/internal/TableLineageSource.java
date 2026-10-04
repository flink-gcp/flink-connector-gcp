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

package io.github.flink.gcp.connector.base.lineage.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;

import io.github.flink.gcp.connector.base.lineage.ResourceIdentifier;

import java.util.List;

/**
 * A Table source's runtime {@link Source}, reporting {@link Lineage#tableSource(String, String,
 * Boundedness, List)} under the table's catalog name (ADR-0160). Reader creation, enumerator
 * creation and restoration, serializers and boundedness go to the wrapped source unchanged; the
 * produced type is the one the table layer supplies.
 *
 * <p>The Flink 2.x compatibility seam also forwards generalized watermark declarations, which Flink
 * 1.20's {@code Source} does not expose.
 *
 * @param <T> the produced type
 * @param <S> the split type
 * @param <E> the enumerator checkpoint type
 */
@Internal
public final class TableLineageSource<T, S extends SourceSplit, E>
        implements CrossVersionSource<T, S, E>, ResultTypeQueryable<T>, LineageVertexProvider {
    private static final long serialVersionUID = 1L;

    private final Source<T, S, E> delegate;
    private final TypeInformation<T> type;
    private final String logicalName;
    private final String namespace;
    private final List<ResourceIdentifier> resources;

    private TableLineageSource(
            Source<T, S, E> delegate,
            TypeInformation<T> type,
            String logicalName,
            String namespace,
            List<ResourceIdentifier> resources) {
        this.delegate = delegate;
        this.type = type;
        this.logicalName = logicalName;
        this.namespace = namespace;
        this.resources = List.copyOf(resources);
    }

    /**
     * Wraps a source so that it reports the table's lineage.
     *
     * @param delegate the source whose readers, enumerators, serializers and boundedness are used
     * @param type the produced type
     * @param logicalName the table's catalog identifier
     * @param namespace the connector's canonical namespace
     * @param resources the physical resources the table reads
     * @param <T> the produced type
     * @param <S> the split type
     * @param <E> the enumerator checkpoint type
     * @return the wrapped source
     */
    public static <T, S extends SourceSplit, E> TableLineageSource<T, S, E> of(
            Source<T, S, E> delegate,
            TypeInformation<T> type,
            String logicalName,
            String namespace,
            List<ResourceIdentifier> resources) {
        return new TableLineageSource<>(delegate, type, logicalName, namespace, resources);
    }

    /**
     * Returns the wrapped source for version-specific delegation and connector factory tests.
     *
     * @return the wrapped source
     */
    @Override
    public Source<T, S, E> delegate() {
        return delegate;
    }

    @Override
    public SourceLineageVertex getLineageVertex() {
        return Lineage.tableSource(logicalName, namespace, getBoundedness(), resources);
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
    public SplitEnumerator<S, E> restoreEnumerator(SplitEnumeratorContext<S> context, E checkpoint)
            throws Exception {
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
