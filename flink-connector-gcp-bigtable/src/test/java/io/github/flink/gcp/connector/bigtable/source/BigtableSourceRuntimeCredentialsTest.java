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

import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.ResponseObserver;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamEnumeratorState;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutation;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.ChangeStreamOpener;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.ChangeStreamRestoreResolver;
import io.github.flink.gcp.connector.bigtable.source.readrows.BigtableReadRowsSource;
import io.github.flink.gcp.connector.bigtable.source.readrows.BigtableScanEnumeratorState;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRangeSplit;
import io.github.flink.gcp.connector.bigtable.source.readrows.enumerator.RowKeySample;
import io.github.flink.gcp.connector.bigtable.source.readrows.enumerator.RowKeySampler;
import io.github.flink.gcp.connector.bigtable.source.readrows.reader.RowStream;
import io.github.flink.gcp.connector.bigtable.source.readrows.reader.RowStreamOpener;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableChangeStreamMutationDeserializationSchema;
import io.github.flink.gcp.connector.testutils.FakeSourceReaderContext;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import io.github.flink.gcp.connector.testutils.ServiceAccountKeyFiles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that source runtime boundaries load, rather than serialize, configured credentials, and
 * that each boundary hands what it loaded to every seam it owns.
 */
class BigtableSourceRuntimeCredentialsTest {

    private static final String MISSING_KEY = "/missing/mounted-bigtable-key.json";
    private static final String FAILURE =
            "Failed to load the configured Bigtable service-account key file.";

    @TempDir Path tempDir;

    @Test
    void scanReaderAndEnumeratorLoadCredentialsAtRuntime() {
        BigtableReadRowsSource<String> source =
                (BigtableReadRowsSource<String>)
                        BigtableSource.<String>builder()
                                .table(TestSources.TABLE)
                                .deserializer(new TestSources.RowKeyDeserializer())
                                .serviceAccountKeyFile(MISSING_KEY)
                                .build();
        FakeSplitEnumeratorContext<RowRangeSplit> context = new FakeSplitEnumeratorContext<>(1);

        // Credential loading precedes all reader-context access, so a null context proves that no
        // TaskManager runtime object is created first.
        assertSanitized(() -> source.createReader(null));
        assertSanitized(() -> source.createEnumerator(context));
        assertSanitized(
                () ->
                        source.restoreEnumerator(
                                context,
                                new BigtableScanEnumeratorState(true, Collections.emptyList())));
    }

    @Test
    void changeStreamReaderAndEnumeratorLoadCredentialsAtRuntime() {
        BigtableChangeStreamSource<BigtableChangeStreamMutation> source =
                BigtableChangeStreamSource.<BigtableChangeStreamMutation>builder()
                        .table(TableDestination.of("p", "i", "t"))
                        .appProfileId("single-cluster")
                        .deserializer(new BigtableChangeStreamMutationDeserializationSchema())
                        .serviceAccountKeyFile(MISSING_KEY)
                        .build();
        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context =
                new FakeSplitEnumeratorContext<>(1);
        BigtableChangeStreamEnumeratorState restored =
                new BigtableChangeStreamEnumeratorState(
                        false,
                        Instant.EPOCH,
                        0,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList());

        // Credential loading precedes all reader-context access, so a null context proves that no
        // TaskManager runtime object is created first.
        assertSanitized(() -> source.createReader(null));
        assertSanitized(() -> source.createEnumerator(context));
        assertSanitized(() -> source.restoreEnumerator(context, restored));
    }

    @Test
    void theChangeStreamReaderHandsOneProviderToBothOfItsSeams() throws Exception {
        // The seams the reader owns read through different client families — changes through a
        // data client, retention through a table-admin one — so what has to hold is not that each
        // has *a* provider but that both have the *same* one, scoped for the union.
        CapturingChangeStreamOpener opener = new CapturingChangeStreamOpener();
        CapturingRestoreResolver restoreResolver = new CapturingRestoreResolver();
        BigtableChangeStreamSource<BigtableChangeStreamMutation> source =
                BigtableChangeStreamSource.<BigtableChangeStreamMutation>builder()
                        .table(TableDestination.of("p", "i", "t"))
                        .appProfileId("single-cluster")
                        .deserializer(new BigtableChangeStreamMutationDeserializationSchema())
                        .serviceAccountKeyFile(ServiceAccountKeyFiles.create(tempDir).toString())
                        .opener(opener)
                        .restoreResolver(restoreResolver)
                        .build();

        source.createReader(readerContext()).close();

        assertThat(opener.credentials).isNotNull();
        assertThat(restoreResolver.credentials).isSameAs(opener.credentials);
    }

    @Test
    void theScanReaderAndEnumeratorEachHandTheirSeamAProvider() throws Exception {
        // The scan source's two seams belong to different components — the opener to a reader on a
        // TaskManager, the sampler to the enumerator on the JobManager — which is why this asserts
        // that each was handed a provider rather than that the two are one object: they run in
        // different processes, so whether they share an instance here says nothing.
        CapturingRowStreamOpener opener = new CapturingRowStreamOpener();
        CapturingRowKeySampler sampler = new CapturingRowKeySampler();
        BigtableReadRowsSource<String> source =
                (BigtableReadRowsSource<String>)
                        BigtableSource.<String>builder()
                                .table(TestSources.TABLE)
                                .deserializer(new TestSources.RowKeyDeserializer())
                                .serviceAccountKeyFile(
                                        ServiceAccountKeyFiles.create(tempDir).toString())
                                .opener(opener)
                                .sampler(sampler)
                                .build();

        source.createReader(readerContext()).close();
        source.createEnumerator(new FakeSplitEnumeratorContext<>(1)).close();

        assertThat(opener.credentials).isNotNull();
        assertThat(sampler.credentials).isNotNull();
    }

    private static FakeSourceReaderContext readerContext() {
        return new FakeSourceReaderContext(
                InternalSourceReaderMetricGroup.mock(new MetricListener().getMetricGroup()));
    }

    private static void assertSanitized(ThrowingCall call) {
        assertThatThrownBy(call::run)
                .isInstanceOf(java.io.IOException.class)
                .hasMessage(FAILURE)
                .hasNoCause();
    }

    @FunctionalInterface
    private interface ThrowingCall {
        void run() throws Exception;
    }

    private static final class CapturingChangeStreamOpener implements ChangeStreamOpener {
        private static final long serialVersionUID = 1L;

        @Nullable private CredentialsProvider credentials;

        @Override
        public void open(
                TableDestination table,
                ChangeStreamPartitionSplit split,
                @Nullable Instant endTime,
                ResponseObserver<ChangeStreamRecord> observer) {}

        @Override
        public void useCredentials(@Nullable CredentialsProvider credentials) {
            this.credentials = credentials;
        }

        @Override
        public void close() {}
    }

    private static final class CapturingRestoreResolver implements ChangeStreamRestoreResolver {
        private static final long serialVersionUID = 1L;

        @Nullable private CredentialsProvider credentials;

        @Override
        public ChangeStreamPartitionSplit resolve(
                ChangeStreamPartitionSplit split, @Nullable StartPosition fallback) {
            return split;
        }

        @Override
        public void useCredentials(@Nullable CredentialsProvider credentials) {
            this.credentials = credentials;
        }
    }

    private static final class CapturingRowStreamOpener implements RowStreamOpener {
        private static final long serialVersionUID = 1L;

        @Nullable private CredentialsProvider credentials;

        @Override
        public RowStream open(
                TableDestination table, ByteStringRange range, @Nullable Filters.Filter filter) {
            throw new UnsupportedOperationException("This opener records credentials only.");
        }

        @Override
        public void useCredentials(@Nullable CredentialsProvider credentials) {
            this.credentials = credentials;
        }

        @Override
        public void close() {}
    }

    private static final class CapturingRowKeySampler implements RowKeySampler {
        private static final long serialVersionUID = 1L;

        @Nullable private CredentialsProvider credentials;

        @Override
        public List<RowKeySample> sample(TableDestination table) {
            throw new UnsupportedOperationException("This sampler records credentials only.");
        }

        @Override
        public void useCredentials(@Nullable CredentialsProvider credentials) {
            this.credentials = credentials;
        }

        @Override
        public void close() {}
    }
}
