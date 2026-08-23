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

package io.github.flink.gcp.connector.bigquery.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
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
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.bigquery.source.enumerator.BigQueryReadEnumeratorState;
import io.github.flink.gcp.connector.bigquery.source.enumerator.BigQueryReadEnumeratorStateSerializer;
import io.github.flink.gcp.connector.bigquery.source.enumerator.BigQueryReadSplitEnumerator;
import io.github.flink.gcp.connector.bigquery.source.enumerator.ReadSessionCreator;
import io.github.flink.gcp.connector.bigquery.source.reader.BigQueryRecordEmitter;
import io.github.flink.gcp.connector.bigquery.source.reader.BigQuerySourceReader;
import io.github.flink.gcp.connector.bigquery.source.reader.BigQuerySourceReaderMetrics;
import io.github.flink.gcp.connector.bigquery.source.reader.BigQuerySplitReader;
import io.github.flink.gcp.connector.bigquery.source.reader.RowStreamOpener;
import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializer;
import io.github.flink.gcp.connector.bigquery.source.split.ReadStreamSplit;
import io.github.flink.gcp.connector.bigquery.source.split.ReadStreamSplitSerializer;
import org.apache.avro.generic.GenericRecord;

import javax.annotation.Nullable;

import java.util.function.Supplier;

/**
 * Reads a BigQuery table through the Storage Read API.
 *
 * <p>Bounded, and only bounded: BigQuery has no changelog read primitive, so a stream of changes
 * would have to be emulated by polling. A bounded source is not a batch-only one — it runs inside a
 * STREAMING pipeline and simply finishes, which is what makes reading a dimension table and joining
 * it against an unbounded stream work.
 *
 * @param <T> type of the records produced by the source
 */
@Internal
public class BigQueryStorageReadSource<T>
        implements Source<T, ReadStreamSplit, BigQueryReadEnumeratorState>, ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;

    private final BigQuerySourceConfig<T> config;

    /**
     * Creates the source.
     *
     * @param config the source configuration
     */
    public BigQueryStorageReadSource(BigQuerySourceConfig<T> config) {
        this.config = config;
    }

    @VisibleForTesting
    BigQuerySourceConfig<T> getConfig() {
        return config;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<T, ReadStreamSplit> createReader(SourceReaderContext context)
            throws Exception {
        BigQueryRowDeserializer<T> deserializer = config.getDeserializer();
        deserializer.open(new ReaderInitializationContext(context));

        BigQuerySourceReaderMetrics metrics =
                new BigQuerySourceReaderMetrics(context.metricGroup());
        RowStreamOpener opener = config.getRowStreamOpener();
        // Before any fetcher starts, which is what the SPI's contract asks for: an implementation
        // whose client captures the listener when it is built would ignore a later registration.
        opener.setRetryListener(metrics::readRetried);
        Supplier<SplitReader<GenericRecord, ReadStreamSplit>> splitReaderSupplier =
                () ->
                        new BigQuerySplitReader(
                                opener,
                                config.getMaxRecordsPerFetch(),
                                deserializer.getReaderSchema(),
                                metrics);
        return new BigQuerySourceReader<>(
                splitReaderSupplier,
                new BigQueryRecordEmitter<>(deserializer, metrics),
                context.getConfiguration(),
                context,
                opener);
    }

    @Override
    public SplitEnumerator<ReadStreamSplit, BigQueryReadEnumeratorState> createEnumerator(
            SplitEnumeratorContext<ReadStreamSplit> context) throws Exception {
        return enumerator(context, null);
    }

    @Override
    public SplitEnumerator<ReadStreamSplit, BigQueryReadEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<ReadStreamSplit> context, BigQueryReadEnumeratorState checkpoint)
            throws Exception {
        return enumerator(context, checkpoint);
    }

    /**
     * Builds one enumerator and the one session creator it owns.
     *
     * <p>A creator per enumerator, minted here rather than carried on the configuration: this
     * object is what the JobManager keeps for a job's whole life, and a coordinator reset builds
     * the next enumerator from it. A shared creator would already be closed by then, and would
     * refuse this enumerator and every retry after it ({@code docs/adr/0128}).
     *
     * <p>The query runner is not minted alongside it. It is not {@code AutoCloseable} — the REST
     * client it wraps has nothing to release — so no teardown makes reusing it unsafe, and it stays
     * on the configuration where its own first-use guard already expects a second enumerator.
     *
     * @param context the enumerator context
     * @param checkpoint the state to restore, or {@code null} for a fresh enumerator
     * @return the enumerator
     * @throws Exception if the session creator cannot be created
     */
    private BigQueryReadSplitEnumerator enumerator(
            SplitEnumeratorContext<ReadStreamSplit> context,
            @Nullable BigQueryReadEnumeratorState checkpoint)
            throws Exception {
        ReadSessionCreator sessionCreator = config.getSessionCreatorFactory().create();
        try {
            return new BigQueryReadSplitEnumerator(
                    context, config, sessionCreator, config.getQueryRunner(), checkpoint);
        } catch (Throwable e) {
            // The enumerator never took ownership, so nothing else will close what was just minted.
            Closers.closeAllSuppressing(e, sessionCreator);
            throw e;
        }
    }

    @Override
    public SimpleVersionedSerializer<ReadStreamSplit> getSplitSerializer() {
        return new ReadStreamSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<BigQueryReadEnumeratorState>
            getEnumeratorCheckpointSerializer() {
        return new BigQueryReadEnumeratorStateSerializer();
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return config.getDeserializer().getProducedType();
    }

    /**
     * Adapts the reader context to the deserializer's initialization context, as the Pub/Sub source
     * does: the two carry the same two members under different interfaces.
     */
    private static final class ReaderInitializationContext
            implements DeserializationSchema.InitializationContext {

        private final SourceReaderContext context;

        private ReaderInitializationContext(SourceReaderContext context) {
            this.context = context;
        }

        @Override
        public MetricGroup getMetricGroup() {
            return context.metricGroup();
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return context.getUserCodeClassLoader();
        }
    }
}
