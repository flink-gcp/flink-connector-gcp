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

package io.github.flink.gcp.connector.datastore.source.batch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.datastore.v1.Query;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.lineage.internal.Lineage;
import io.github.flink.gcp.connector.base.lineage.internal.LineageIdentifiers;
import io.github.flink.gcp.connector.base.source.ReaderInitializationContext;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.DatastoreCredentials;
import io.github.flink.gcp.connector.datastore.source.DatastoreSourceConfig;
import io.github.flink.gcp.connector.datastore.source.batch.enumerator.DatastoreBatchSplitEnumerator;
import io.github.flink.gcp.connector.datastore.source.batch.enumerator.QueryPlanner;
import io.github.flink.gcp.connector.datastore.source.batch.reader.DatastoreRecordEmitter;
import io.github.flink.gcp.connector.datastore.source.batch.reader.DatastoreSourceReader;
import io.github.flink.gcp.connector.datastore.source.batch.reader.DatastoreSourceReaderMetrics;
import io.github.flink.gcp.connector.datastore.source.batch.reader.DatastoreSplitReader;
import io.github.flink.gcp.connector.datastore.source.batch.reader.QueryPageReader;
import io.github.flink.gcp.connector.datastore.source.serializer.DatastoreEntityDeserializationSchema;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * The bounded source over Datastore's {@code RunQuery}, at one snapshot time.
 *
 * @param <T> the record type produced
 */
@Internal
public final class DatastoreBatchSource<T>
        implements Source<T, QuerySplit, DatastoreBatchEnumeratorState>,
                ResultTypeQueryable<T>,
                LineageVertexProvider {

    private static final long serialVersionUID = 1L;

    private final DatastoreSourceConfig<T> config;

    /**
     * Creates the source.
     *
     * @param config the configuration the builder assembled
     */
    public DatastoreBatchSource(DatastoreSourceConfig<T> config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    /**
     * Reports the kind a kind scan or a query reads, or no dataset for a query naming no kind or
     * several and for a GQL query, which is parsed only when the read is planned.
     */
    @Override
    public SourceLineageVertex getLineageVertex() {
        Query query = config.getQuery();
        String kind = query != null ? SplittableQueries.kindOf(query) : null;
        if (kind == null) {
            return Lineage.source(getBoundedness(), List.of());
        }
        DatabaseDestination database = config.getDatabase();
        return Lineage.source(
                getBoundedness(),
                List.of(
                        LineageIdentifiers.datastoreKind(
                                database.getProject(),
                                database.getDatabaseId().isEmpty()
                                        ? DatabaseDestination.DEFAULT_DATABASE_NAME
                                        : database.getDatabaseId(),
                                config.getNamespace(),
                                kind)));
    }

    /** Returns the configuration, for the source's own tests. */
    @VisibleForTesting
    public DatastoreSourceConfig<T> getConfig() {
        return config;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<T, QuerySplit> createReader(SourceReaderContext context) throws Exception {
        QueryPageReader pageReader = config.getPageReader();
        pageReader.useCredentials(credentials());
        DatastoreEntityDeserializationSchema<T> deserializer = config.getDeserializer();
        deserializer.open(new ReaderInitializationContext(context));

        DatastoreSourceReaderMetrics metrics =
                new DatastoreSourceReaderMetrics(context.metricGroup());
        Supplier<SplitReader<FetchedEntity, QuerySplit>> splitReaderSupplier =
                () ->
                        new DatastoreSplitReader(
                                config.getDatabase(), pageReader, config.getPageSize(), metrics);
        return new DatastoreSourceReader<>(
                splitReaderSupplier,
                new DatastoreRecordEmitter<>(deserializer, metrics),
                context.getConfiguration(),
                context,
                pageReader);
    }

    @Override
    public SplitEnumerator<QuerySplit, DatastoreBatchEnumeratorState> createEnumerator(
            SplitEnumeratorContext<QuerySplit> context) throws Exception {
        return enumerator(context, null);
    }

    @Override
    public SplitEnumerator<QuerySplit, DatastoreBatchEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<QuerySplit> context, DatastoreBatchEnumeratorState checkpoint)
            throws Exception {
        return enumerator(context, checkpoint);
    }

    /**
     * Builds one enumerator and the one planner it owns — minted here rather than carried on the
     * configuration, because a coordinator reset builds the next enumerator from this same object
     * and a shared planner would already be closed ({@code docs/adr/0128}). The credentials are
     * loaded before the planner is minted, so a key file that cannot be read fails with nothing
     * built to release.
     */
    private DatastoreBatchSplitEnumerator enumerator(
            SplitEnumeratorContext<QuerySplit> context,
            @Nullable DatastoreBatchEnumeratorState checkpoint)
            throws Exception {
        GoogleCredentials credentials = credentials();
        QueryPlanner planner = config.getPlannerFactory().create();
        try {
            planner.useCredentials(credentials);
            return new DatastoreBatchSplitEnumerator(context, config, planner, checkpoint);
        } catch (Throwable e) {
            // The enumerator never took ownership, so nothing else will close what was just minted.
            Closers.closeAllSuppressing(e, planner);
            throw e;
        }
    }

    /** Loads the credentials once per runtime component: reader and enumerator run apart. */
    @Nullable
    private GoogleCredentials credentials() throws IOException {
        return DatastoreCredentials.load(config.getServiceAccountKeyFile());
    }

    @Override
    public SimpleVersionedSerializer<QuerySplit> getSplitSerializer() {
        return new QuerySplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<DatastoreBatchEnumeratorState>
            getEnumeratorCheckpointSerializer() {
        return new DatastoreBatchEnumeratorStateSerializer();
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return config.getDeserializer().getProducedType();
    }
}
