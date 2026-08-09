/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.bigtable.source.readrows;

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

import com.google.cloud.bigtable.data.v2.models.Row;
import io.github.flink.gcp.connector.bigtable.source.BigtableSourceConfig;
import io.github.flink.gcp.connector.bigtable.source.readrows.enumerator.BigtableScanSplitEnumerator;
import io.github.flink.gcp.connector.bigtable.source.readrows.reader.BigtableRecordEmitter;
import io.github.flink.gcp.connector.bigtable.source.readrows.reader.BigtableSourceReader;
import io.github.flink.gcp.connector.bigtable.source.readrows.reader.BigtableSourceReaderMetrics;
import io.github.flink.gcp.connector.bigtable.source.readrows.reader.BigtableSplitReader;
import io.github.flink.gcp.connector.bigtable.source.readrows.reader.RowStreamOpener;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableRowDeserializationSchema;

import java.util.function.Supplier;

/**
 * The bounded scan source over Bigtable's {@code ReadRows}.
 *
 * <p>Bounded, and only bounded: the configured ranges are read once and the source finishes. A
 * changelog read is a different RPC with different semantics and is a source of its own.
 *
 * @param <T> the record type produced
 */
@Internal
public class BigtableReadRowsSource<T>
        implements Source<T, RowRangeSplit, BigtableScanEnumeratorState>, ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;

    private final BigtableSourceConfig<T> config;

    /**
     * Creates the source.
     *
     * @param config the configuration the builder assembled
     */
    public BigtableReadRowsSource(BigtableSourceConfig<T> config) {
        this.config = Preconditions.checkNotNull(config, "config must not be null");
    }

    /** Returns the configuration, for the source's own tests. */
    @VisibleForTesting
    public BigtableSourceConfig<T> getConfig() {
        return config;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<T, RowRangeSplit> createReader(SourceReaderContext context)
            throws Exception {
        BigtableRowDeserializationSchema<T> deserializer = config.getDeserializer();
        deserializer.open(new ReaderInitializationContext(context));

        BigtableSourceReaderMetrics metrics =
                new BigtableSourceReaderMetrics(context.metricGroup());
        RowStreamOpener opener = config.getOpener();
        Supplier<SplitReader<Row, RowRangeSplit>> splitReaderSupplier =
                () ->
                        new BigtableSplitReader(
                                config.getTable(),
                                opener,
                                config.getFilter(),
                                config.getMaxRowsPerFetch(),
                                metrics);
        return new BigtableSourceReader<>(
                splitReaderSupplier,
                new BigtableRecordEmitter<>(deserializer, metrics),
                context.getConfiguration(),
                context,
                opener);
    }

    @Override
    public SplitEnumerator<RowRangeSplit, BigtableScanEnumeratorState> createEnumerator(
            SplitEnumeratorContext<RowRangeSplit> context) {
        return new BigtableScanSplitEnumerator(context, config, null);
    }

    @Override
    public SplitEnumerator<RowRangeSplit, BigtableScanEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<RowRangeSplit> context, BigtableScanEnumeratorState checkpoint) {
        return new BigtableScanSplitEnumerator(context, config, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<RowRangeSplit> getSplitSerializer() {
        return new RowRangeSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<BigtableScanEnumeratorState>
            getEnumeratorCheckpointSerializer() {
        return new BigtableScanEnumeratorStateSerializer();
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
