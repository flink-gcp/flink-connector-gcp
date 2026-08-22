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

package io.github.flink.gcp.connector.spanner.source.batch;

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
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.UserCodeClassLoader;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.Struct;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.spanner.SpannerCredentials;
import io.github.flink.gcp.connector.spanner.source.SpannerSourceConfig;
import io.github.flink.gcp.connector.spanner.source.batch.enumerator.PartitionPlanner;
import io.github.flink.gcp.connector.spanner.source.batch.enumerator.SpannerPartitionSplitEnumerator;
import io.github.flink.gcp.connector.spanner.source.batch.reader.SpannerRecordEmitter;
import io.github.flink.gcp.connector.spanner.source.batch.reader.SpannerSourceReader;
import io.github.flink.gcp.connector.spanner.source.batch.reader.SpannerSourceReaderMetrics;
import io.github.flink.gcp.connector.spanner.source.batch.reader.SpannerSplitReader;
import io.github.flink.gcp.connector.spanner.source.batch.reader.StructStreamOpener;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerStructDeserializationSchema;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * The bounded batch source over Spanner's partitioned reads.
 *
 * <p>Bounded, and only bounded: the read is planned once at one snapshot, its partitions are read
 * once, and the source finishes. A change stream is a different API with different semantics and is
 * a source of its own.
 *
 * @param <T> the record type produced
 */
@Internal
public class SpannerBatchReadSource<T>
        implements Source<T, PartitionSplit, SpannerBatchEnumeratorState>, ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;

    private final SpannerSourceConfig<T> config;

    /**
     * Creates the source.
     *
     * @param config the configuration the builder assembled
     */
    public SpannerBatchReadSource(SpannerSourceConfig<T> config) {
        this.config = Preconditions.checkNotNull(config, "config must not be null");
    }

    /**
     * Returns the configuration, for the source's own tests.
     *
     * @return the configuration
     */
    @VisibleForTesting
    public SpannerSourceConfig<T> getConfig() {
        return config;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<T, PartitionSplit> createReader(SourceReaderContext context)
            throws Exception {
        StructStreamOpener opener = config.getOpener();
        opener.useCredentials(credentials());
        SpannerStructDeserializationSchema<T> deserializer = config.getDeserializer();
        deserializer.open(new ReaderInitializationContext(context));

        SpannerSourceReaderMetrics metrics = new SpannerSourceReaderMetrics(context.metricGroup());
        Supplier<SplitReader<Struct, PartitionSplit>> splitReaderSupplier =
                () ->
                        new SpannerSplitReader(
                                config.getDatabase(),
                                opener,
                                config.getMaxRecordsPerFetch(),
                                metrics);
        return new SpannerSourceReader<>(
                splitReaderSupplier,
                new SpannerRecordEmitter<>(deserializer, metrics),
                context.getConfiguration(),
                context,
                opener);
    }

    @Override
    public SplitEnumerator<PartitionSplit, SpannerBatchEnumeratorState> createEnumerator(
            SplitEnumeratorContext<PartitionSplit> context) throws Exception {
        return enumerator(context, null);
    }

    @Override
    public SplitEnumerator<PartitionSplit, SpannerBatchEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<PartitionSplit> context, SpannerBatchEnumeratorState checkpoint)
            throws Exception {
        return enumerator(context, checkpoint);
    }

    /**
     * Builds one enumerator and the one planner it owns.
     *
     * <p>A planner per enumerator, minted here rather than carried on the configuration: this
     * object is what the JobManager keeps for a job's whole life, and a coordinator reset builds
     * the next enumerator from it. A shared planner would already be closed by then, and would
     * refuse this enumerator and every retry after it ({@code docs/adr/0128}).
     *
     * <p>The credentials are loaded <em>before</em> the planner is minted, so a key file that
     * cannot be read still fails here with nothing built to release.
     *
     * @param context the enumerator context
     * @param checkpoint the state to restore, or {@code null} for a fresh enumerator
     * @return the enumerator
     * @throws Exception if the credentials cannot be loaded or the planner cannot be created
     */
    private SpannerPartitionSplitEnumerator enumerator(
            SplitEnumeratorContext<PartitionSplit> context,
            @Nullable SpannerBatchEnumeratorState checkpoint)
            throws Exception {
        GoogleCredentials credentials = credentials();
        PartitionPlanner planner = config.getPlannerFactory().create();
        try {
            planner.useCredentials(credentials);
            return new SpannerPartitionSplitEnumerator(context, config, planner, checkpoint);
        } catch (Throwable e) {
            // The enumerator never took ownership, so nothing else will close what was just minted.
            Closers.closeAllSuppressing(e, planner);
            throw e;
        }
    }

    /**
     * Loads the key this component was configured with.
     *
     * <p>Called once per runtime component rather than once per source: the reader runs on a
     * TaskManager and the enumerator on the JobManager, so each loads the mounted path itself. The
     * seams are handed the result rather than the path, so none of them loads a second one and no
     * caller reaches one by downcast.
     *
     * @return the loaded credentials, or {@code null} when no key file is configured
     * @throws IOException if the configured key file cannot be read
     */
    @Nullable
    private GoogleCredentials credentials() throws IOException {
        return SpannerCredentials.load(config.getServiceAccountKeyFile());
    }

    @Override
    public SimpleVersionedSerializer<PartitionSplit> getSplitSerializer() {
        return new PartitionSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<SpannerBatchEnumeratorState>
            getEnumeratorCheckpointSerializer() {
        return new SpannerBatchEnumeratorStateSerializer();
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return config.getDeserializer().getProducedType();
    }

    /**
     * Adapts a {@link SourceReaderContext} to the context a {@link DeserializationSchema} expects.
     *
     * <p>Flink offers no adapter of its own, and every FLIP-27 source that opens a deserialization
     * schema writes this one.
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
