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

package io.github.flink.gcp.connector.bigtable.source;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.runtime.metrics.groups.InternalSplitEnumeratorMetricGroup;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.ResponseObserver;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.bigtable.ClientMetricsSettings;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutation;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.ChangeStreamCoordinatorClient;
import io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.ChangeStreamCoordinatorClientFactory;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.ChangeStreamOpener;
import io.github.flink.gcp.connector.bigtable.source.readrows.BigtableScanSource;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRangeSplit;
import io.github.flink.gcp.connector.bigtable.source.readrows.enumerator.RowKeySample;
import io.github.flink.gcp.connector.bigtable.source.readrows.enumerator.RowKeySampler;
import io.github.flink.gcp.connector.bigtable.source.readrows.enumerator.RowKeySamplerFactory;
import io.github.flink.gcp.connector.bigtable.source.readrows.reader.RowStream;
import io.github.flink.gcp.connector.bigtable.source.readrows.reader.RowStreamOpener;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableChangeStreamMutationDeserializationSchema;
import io.github.flink.gcp.connector.testutils.FakeSourceReaderContext;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins where each source component registers its client's metrics and what it hands its seam: the
 * readers on their own group, the enumerators on the enumerator group, shared by every enumerator
 * built on that group after the first.
 */
class BigtableSourceClientMetricsTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "t");

    @Test
    void theScanReaderRegistersReadRowsAndHandsItsOpenerTheClientMetrics() throws Exception {
        CapturingRowStreamOpener opener = new CapturingRowStreamOpener();
        MetricListener listener = new MetricListener();
        BigtableScanSource<String> source =
                (BigtableScanSource<String>)
                        TestSources.withSamplerFactory(
                                        BigtableSource.<String>builder()
                                                .table(TestSources.TABLE)
                                                .deserializer(new TestSources.RowKeyDeserializer())
                                                .opener(opener)
                                                .exportClientMetricsToFlink(true),
                                        new CapturingRowKeySamplerFactory())
                                .build();

        source.createReader(readerContext(listener)).close();

        assertThat(opener.clientMetrics).isNotNull();
        assertThat(listener.getCounter("rpcMethod", "ReadRows", "operations")).isPresent();
        assertThat(listener.getCounter("rpcMethod", "SampleRowKeys", "operations")).isEmpty();
    }

    @Test
    void scanEnumeratorsOnOneGroupShareOneSetOfClientMetrics() throws Exception {
        // A coordinator reset builds the next enumerator on the group the first one registered on,
        // where a second registration of a name would be dropped. The adaptive scheduler builds it
        // from a new source object too, so the sharing must not hang on the source (#1908).
        FakeSplitEnumeratorContext<RowRangeSplit> context = new FakeSplitEnumeratorContext<>(1);
        CapturingRowKeySamplerFactory samplers = new CapturingRowKeySamplerFactory();

        scanSource(samplers).createEnumerator(context).close();
        scanSource(samplers).createEnumerator(context).close();
        scanSource(samplers)
                .createEnumerator(new FakeSplitEnumeratorContext<RowRangeSplit>(1))
                .close();

        assertThat(samplers.minted).hasSize(3);
        assertThat(samplers.minted.get(0).clientMetrics).isNotNull();
        assertThat(samplers.minted.get(1).clientMetrics)
                .isSameAs(samplers.minted.get(0).clientMetrics);
        assertThat(samplers.minted.get(2).clientMetrics)
                .isNotNull()
                .isNotSameAs(samplers.minted.get(0).clientMetrics);
    }

    private static BigtableScanSource<String> scanSource(CapturingRowKeySamplerFactory samplers) {
        return (BigtableScanSource<String>)
                TestSources.withSamplerFactory(
                                BigtableSource.<String>builder()
                                        .table(TestSources.TABLE)
                                        .deserializer(new TestSources.RowKeyDeserializer())
                                        .opener(new CapturingRowStreamOpener())
                                        .exportClientMetricsToFlink(true),
                                samplers)
                        .build();
    }

    @Test
    void theScanSourceRegistersNothingWhenTheExportIsOff() throws Exception {
        CapturingRowStreamOpener opener = new CapturingRowStreamOpener();
        CapturingRowKeySamplerFactory samplers = new CapturingRowKeySamplerFactory();
        MetricListener listener = new MetricListener();
        BigtableScanSource<String> source =
                (BigtableScanSource<String>)
                        TestSources.withSamplerFactory(
                                        BigtableSource.<String>builder()
                                                .table(TestSources.TABLE)
                                                .deserializer(new TestSources.RowKeyDeserializer())
                                                .opener(opener),
                                        samplers)
                                .build();

        source.createReader(readerContext(listener)).close();
        source.createEnumerator(new FakeSplitEnumeratorContext<RowRangeSplit>(1)).close();

        assertThat(opener.clientMetrics).isNull();
        assertThat(samplers.minted.get(0).clientMetrics).isNull();
        assertThat(listener.getCounter("rpcMethod", "ReadRows", "operations")).isEmpty();
    }

    @Test
    void theChangeStreamReaderRegistersReadChangeStream() throws Exception {
        CapturingChangeStreamOpener opener = new CapturingChangeStreamOpener();
        MetricListener listener = new MetricListener();

        changeStreamSource(opener, new CapturingCoordinatorClientFactory(), true)
                .createReader(readerContext(listener))
                .close();

        // The set handed to the opener is the one registered on the reader's group.
        ClientMetricsSettings.recordOneOperation(
                opener.clientMetrics.openTelemetry(), "ReadChangeStream");
        assertThat(
                        listener.getCounter("rpcMethod", "ReadChangeStream", "operations")
                                .orElseThrow()
                                .getCount())
                .isOne();
    }

    @Test
    void enumeratorsRegisterTheirMethodOnTheEnumeratorGroupAndRecordIntoIt() throws Exception {
        CapturingCoordinatorClientFactory clients = new CapturingCoordinatorClientFactory();
        MetricListener changeStreamGroup = new MetricListener();
        changeStreamSource(new CapturingChangeStreamOpener(), clients, true)
                .createEnumerator(
                        withGroup(
                                new FakeSplitEnumeratorContext<ChangeStreamPartitionSplit>(1),
                                changeStreamGroup))
                .close();
        ClientMetricsSettings.recordOneOperation(
                clients.minted.get(0).clientMetrics.openTelemetry(),
                "GenerateInitialChangeStreamPartitions");
        assertThat(
                        changeStreamGroup
                                .getCounter(
                                        "enumerator",
                                        "rpcMethod",
                                        "GenerateInitialChangeStreamPartitions",
                                        "operations")
                                .orElseThrow()
                                .getCount())
                .isOne();

        CapturingRowKeySamplerFactory samplers = new CapturingRowKeySamplerFactory();
        MetricListener scanGroup = new MetricListener();
        ((BigtableScanSource<String>)
                        TestSources.withSamplerFactory(
                                        BigtableSource.<String>builder()
                                                .table(TestSources.TABLE)
                                                .deserializer(new TestSources.RowKeyDeserializer())
                                                .opener(new CapturingRowStreamOpener())
                                                .exportClientMetricsToFlink(true),
                                        samplers)
                                .build())
                .createEnumerator(
                        withGroup(new FakeSplitEnumeratorContext<RowRangeSplit>(1), scanGroup))
                .close();
        ClientMetricsSettings.recordOneOperation(
                samplers.minted.get(0).clientMetrics.openTelemetry(), "SampleRowKeys");
        assertThat(
                        scanGroup
                                .getCounter(
                                        "enumerator", "rpcMethod", "SampleRowKeys", "operations")
                                .orElseThrow()
                                .getCount())
                .isOne();
    }

    /** The context with its metric group replaced by one the test can read back. */
    @SuppressWarnings("unchecked")
    private static <SplitT extends SourceSplit> SplitEnumeratorContext<SplitT> withGroup(
            SplitEnumeratorContext<SplitT> context, MetricListener listener) {
        SplitEnumeratorMetricGroup group =
                new InternalSplitEnumeratorMetricGroup(listener.getMetricGroup());
        return (SplitEnumeratorContext<SplitT>)
                Proxy.newProxyInstance(
                        SplitEnumeratorContext.class.getClassLoader(),
                        new Class<?>[] {SplitEnumeratorContext.class},
                        (proxy, method, arguments) -> {
                            if (method.getName().equals("metricGroup")) {
                                return group;
                            }
                            try {
                                return method.invoke(context, arguments);
                            } catch (InvocationTargetException e) {
                                throw e.getCause();
                            }
                        });
    }

    @Test
    void changeStreamEnumeratorsOnOneGroupShareOneSetOfClientMetrics() throws Exception {
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context =
                new FakeSplitEnumeratorContext<>(1);
        CapturingCoordinatorClientFactory clients = new CapturingCoordinatorClientFactory();

        changeStreamSource(new CapturingChangeStreamOpener(), clients, true)
                .createEnumerator(context)
                .close();
        changeStreamSource(new CapturingChangeStreamOpener(), clients, true)
                .createEnumerator(context)
                .close();

        assertThat(clients.minted).hasSize(2);
        assertThat(clients.minted.get(0).clientMetrics).isNotNull();
        assertThat(clients.minted.get(1).clientMetrics)
                .isSameAs(clients.minted.get(0).clientMetrics);
    }

    @Test
    void theChangeStreamSourceRegistersNothingWhenTheExportIsOff() throws Exception {
        CapturingChangeStreamOpener opener = new CapturingChangeStreamOpener();
        CapturingCoordinatorClientFactory clients = new CapturingCoordinatorClientFactory();
        MetricListener listener = new MetricListener();
        BigtableChangeStreamSource<BigtableChangeStreamMutation> source =
                changeStreamSource(opener, clients, false);

        source.createReader(readerContext(listener)).close();
        source.createEnumerator(new FakeSplitEnumeratorContext<ChangeStreamPartitionSplit>(1))
                .close();

        assertThat(opener.clientMetrics).isNull();
        assertThat(clients.minted.get(0).clientMetrics).isNull();
        assertThat(listener.getCounter("rpcMethod", "ReadChangeStream", "operations")).isEmpty();
    }

    private static BigtableChangeStreamSource<BigtableChangeStreamMutation> changeStreamSource(
            ChangeStreamOpener opener,
            ChangeStreamCoordinatorClientFactory clients,
            boolean exportToFlink) {
        return BigtableChangeStreamSource.<BigtableChangeStreamMutation>builder()
                .table(TABLE)
                .appProfileId("single-cluster")
                .deserializer(new BigtableChangeStreamMutationDeserializationSchema())
                .opener(opener)
                .coordinatorClientFactory(clients)
                .exportClientMetricsToFlink(exportToFlink)
                .build();
    }

    private static FakeSourceReaderContext readerContext(MetricListener listener) {
        return new FakeSourceReaderContext(
                InternalSourceReaderMetricGroup.mock(listener.getMetricGroup()));
    }

    private static final class CapturingRowStreamOpener implements RowStreamOpener {
        private static final long serialVersionUID = 1L;

        @Nullable private transient ClientMetrics clientMetrics;

        @Override
        public RowStream open(
                TableDestination table, ByteStringRange range, @Nullable Filters.Filter filter) {
            throw new UnsupportedOperationException("This opener records client metrics only.");
        }

        @Override
        public void useCredentials(@Nullable CredentialsProvider credentials) {}

        @Override
        public void useClientMetrics(@Nullable ClientMetrics clientMetrics) {
            this.clientMetrics = clientMetrics;
        }

        @Override
        public void close() {}
    }

    private static final class CapturingRowKeySamplerFactory implements RowKeySamplerFactory {
        private static final long serialVersionUID = 1L;

        private final transient List<CapturingRowKeySampler> minted = new ArrayList<>();

        @Override
        public RowKeySampler create() {
            CapturingRowKeySampler sampler = new CapturingRowKeySampler();
            minted.add(sampler);
            return sampler;
        }
    }

    private static final class CapturingRowKeySampler implements RowKeySampler {

        @Nullable private ClientMetrics clientMetrics;

        @Override
        public List<RowKeySample> sample(TableDestination table) {
            throw new UnsupportedOperationException("This sampler records client metrics only.");
        }

        @Override
        public void useCredentials(@Nullable CredentialsProvider credentials) {}

        @Override
        public void useClientMetrics(@Nullable ClientMetrics clientMetrics) {
            this.clientMetrics = clientMetrics;
        }

        @Override
        public void close() {}
    }

    private static final class CapturingChangeStreamOpener implements ChangeStreamOpener {
        private static final long serialVersionUID = 1L;

        @Nullable private transient ClientMetrics clientMetrics;

        @Override
        public void open(
                TableDestination table,
                ChangeStreamPartitionSplit split,
                @Nullable Instant boundedTimestamp,
                ResponseObserver<ChangeStreamRecord> observer) {}

        @Override
        public void useCredentials(@Nullable CredentialsProvider credentials) {}

        @Override
        public void useClientMetrics(@Nullable ClientMetrics clientMetrics) {
            this.clientMetrics = clientMetrics;
        }

        @Override
        public void close() {}
    }

    private static final class CapturingCoordinatorClientFactory
            implements ChangeStreamCoordinatorClientFactory {
        private static final long serialVersionUID = 1L;

        private final transient List<CapturingCoordinatorClient> minted = new ArrayList<>();

        @Override
        public ChangeStreamCoordinatorClient create() {
            CapturingCoordinatorClient client = new CapturingCoordinatorClient();
            minted.add(client);
            return client;
        }
    }

    private static final class CapturingCoordinatorClient implements ChangeStreamCoordinatorClient {

        @Nullable private ClientMetrics clientMetrics;

        @Override
        public void validateSingleClusterAppProfile() {}

        @Override
        public Duration retention() {
            return Duration.ofDays(1);
        }

        @Override
        public List<ByteStringRange> generateInitialPartitions() {
            throw new UnsupportedOperationException("This client records client metrics only.");
        }

        @Override
        public void useClientMetrics(@Nullable ClientMetrics clientMetrics) {
            this.clientMetrics = clientMetrics;
        }

        @Override
        public void close() {}
    }
}
