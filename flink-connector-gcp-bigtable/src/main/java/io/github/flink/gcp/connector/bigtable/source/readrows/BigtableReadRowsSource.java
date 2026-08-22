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

package io.github.flink.gcp.connector.bigtable.source.readrows;

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
import org.apache.flink.util.Preconditions;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.bigtable.data.v2.models.Row;
import io.github.flink.gcp.connector.base.source.ReaderInitializationContext;
import io.github.flink.gcp.connector.bigtable.BigtableCredentials;
import io.github.flink.gcp.connector.bigtable.source.BigtableSourceConfig;
import io.github.flink.gcp.connector.bigtable.source.readrows.enumerator.BigtableScanSplitEnumerator;
import io.github.flink.gcp.connector.bigtable.source.readrows.reader.BigtableRecordEmitter;
import io.github.flink.gcp.connector.bigtable.source.readrows.reader.BigtableSourceReader;
import io.github.flink.gcp.connector.bigtable.source.readrows.reader.BigtableSourceReaderMetrics;
import io.github.flink.gcp.connector.bigtable.source.readrows.reader.BigtableSplitReader;
import io.github.flink.gcp.connector.bigtable.source.readrows.reader.RowStreamOpener;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableRowDeserializationSchema;

import javax.annotation.Nullable;

import java.io.IOException;
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
        RowStreamOpener opener = config.getOpener();
        opener.useCredentials(dataCredentials());
        BigtableRowDeserializationSchema<T> deserializer = config.getDeserializer();
        deserializer.open(new ReaderInitializationContext(context));

        BigtableSourceReaderMetrics metrics =
                new BigtableSourceReaderMetrics(context.metricGroup());
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
            SplitEnumeratorContext<RowRangeSplit> context) throws Exception {
        useEnumeratorCredentials();
        return new BigtableScanSplitEnumerator(context, config, null);
    }

    @Override
    public SplitEnumerator<RowRangeSplit, BigtableScanEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<RowRangeSplit> context, BigtableScanEnumeratorState checkpoint)
            throws Exception {
        useEnumeratorCredentials();
        return new BigtableScanSplitEnumerator(context, config, checkpoint);
    }

    /** Hands the sampler the provider before the enumerator makes its one planning call. */
    private void useEnumeratorCredentials() throws IOException {
        config.getSampler().useCredentials(dataCredentials());
    }

    /**
     * Loads the provider for this component's one client family.
     *
     * <p>Called once per runtime component rather than once per source: the reader and the
     * enumerator run in different processes, so each loads the key it was configured with.
     */
    @Nullable
    private CredentialsProvider dataCredentials() throws IOException {
        return BigtableCredentials.loadData(config.getServiceAccountKeyFile());
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
}
