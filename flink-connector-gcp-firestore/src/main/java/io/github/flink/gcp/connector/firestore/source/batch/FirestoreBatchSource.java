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

package io.github.flink.gcp.connector.firestore.source.batch;

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
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.lineage.internal.Lineage;
import io.github.flink.gcp.connector.base.lineage.internal.LineageIdentifiers;
import io.github.flink.gcp.connector.base.source.ReaderInitializationContext;
import io.github.flink.gcp.connector.firestore.FirestoreCredentials;
import io.github.flink.gcp.connector.firestore.source.FirestoreSourceConfig;
import io.github.flink.gcp.connector.firestore.source.batch.enumerator.FirestoreBatchSplitEnumerator;
import io.github.flink.gcp.connector.firestore.source.batch.enumerator.QueryPlanner;
import io.github.flink.gcp.connector.firestore.source.batch.reader.FirestoreRecordEmitter;
import io.github.flink.gcp.connector.firestore.source.batch.reader.FirestoreSourceReader;
import io.github.flink.gcp.connector.firestore.source.batch.reader.FirestoreSourceReaderMetrics;
import io.github.flink.gcp.connector.firestore.source.batch.reader.FirestoreSplitReader;
import io.github.flink.gcp.connector.firestore.source.batch.reader.QueryPageReader;
import io.github.flink.gcp.connector.firestore.source.serializer.FirestoreDocumentDeserializationSchema;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * The bounded source over Firestore's {@code RunQuery}, at one snapshot time.
 *
 * @param <T> the record type produced
 */
@Internal
public final class FirestoreBatchSource<T>
        implements Source<T, QuerySplit, FirestoreBatchEnumeratorState>,
                ResultTypeQueryable<T>,
                LineageVertexProvider {

    private static final long serialVersionUID = 1L;

    private final FirestoreSourceConfig<T> config;

    /**
     * Creates the source.
     *
     * @param config the configuration the builder assembled
     */
    public FirestoreBatchSource(FirestoreSourceConfig<T> config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    /**
     * Reports the scanned collection group, or no dataset for a query, whose collections are the
     * factory's and which extraction never calls.
     */
    @Override
    public SourceLineageVertex getLineageVertex() {
        String collectionGroup = config.getCollectionGroup();
        return Lineage.source(
                getBoundedness(),
                collectionGroup == null
                        ? List.of()
                        : List.of(
                                LineageIdentifiers.firestoreCollectionGroup(
                                        config.getDatabase().getProject(),
                                        config.getDatabase().getDatabaseId(),
                                        collectionGroup)));
    }

    /** Returns the configuration, for the source's own tests. */
    @VisibleForTesting
    public FirestoreSourceConfig<T> getConfig() {
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
        FirestoreDocumentDeserializationSchema<T> deserializer = config.getDeserializer();
        deserializer.open(new ReaderInitializationContext(context));

        FirestoreSourceReaderMetrics metrics =
                new FirestoreSourceReaderMetrics(context.metricGroup());
        Supplier<SplitReader<FetchedDocument, QuerySplit>> splitReaderSupplier =
                () ->
                        new FirestoreSplitReader(
                                config.getDatabase(), pageReader, config.getPageSize(), metrics);
        return new FirestoreSourceReader<>(
                splitReaderSupplier,
                new FirestoreRecordEmitter<>(deserializer, metrics),
                context.getConfiguration(),
                context,
                pageReader);
    }

    @Override
    public SplitEnumerator<QuerySplit, FirestoreBatchEnumeratorState> createEnumerator(
            SplitEnumeratorContext<QuerySplit> context) throws Exception {
        return enumerator(context, null);
    }

    @Override
    public SplitEnumerator<QuerySplit, FirestoreBatchEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<QuerySplit> context, FirestoreBatchEnumeratorState checkpoint)
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
    private FirestoreBatchSplitEnumerator enumerator(
            SplitEnumeratorContext<QuerySplit> context,
            @Nullable FirestoreBatchEnumeratorState checkpoint)
            throws Exception {
        GoogleCredentials credentials = credentials();
        QueryPlanner planner = config.getPlannerFactory().create();
        try {
            planner.useCredentials(credentials);
            return new FirestoreBatchSplitEnumerator(context, config, planner, checkpoint);
        } catch (Throwable e) {
            // The enumerator never took ownership, so nothing else will close what was just minted.
            Closers.closeAllSuppressing(e, planner);
            throw e;
        }
    }

    /** Loads the credentials once per runtime component: reader and enumerator run apart. */
    @Nullable
    private GoogleCredentials credentials() throws IOException {
        return FirestoreCredentials.load(config.getServiceAccountKeyFile());
    }

    @Override
    public SimpleVersionedSerializer<QuerySplit> getSplitSerializer() {
        return new QuerySplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<FirestoreBatchEnumeratorState>
            getEnumeratorCheckpointSerializer() {
        return new FirestoreBatchEnumeratorStateSerializer();
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return config.getDeserializer().getProducedType();
    }
}
