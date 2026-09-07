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

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.util.InstantiationUtil;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.ResponseObserver;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.ChangeStreamCoordinatorClient;
import io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.ChangeStreamCoordinatorClientFactory;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.ChangeStreamOpener;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.ChangeStreamRestoreResolver;
import io.github.flink.gcp.connector.bigtable.source.readrows.BigtableScanSource;
import io.github.flink.gcp.connector.bigtable.source.readrows.enumerator.RowKeySampler;
import io.github.flink.gcp.connector.bigtable.source.readrows.enumerator.RowKeySamplerFactory;
import io.github.flink.gcp.connector.bigtable.source.readrows.reader.RowStream;
import io.github.flink.gcp.connector.bigtable.source.readrows.reader.RowStreamOpener;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BigtableSourceLineageTest {
    private static final AtomicInteger ACCESS = new AtomicInteger();

    @Test
    void extractionNeverTouchesClientFactoriesReadersOrRestoreMetadata() throws Exception {
        ACCESS.set(0);
        var access = new FakeAccess();
        var scan =
                (BigtableScanSource<String>)
                        BigtableSource.<String>builder()
                                .table(TestSources.TABLE)
                                .deserializer(new TestSources.RowKeyDeserializer())
                                .samplerFactory(new FakeSamplerFactory())
                                .opener(access)
                                .build();
        var changes =
                BigtableChangeStreamSource
                        .<io.github.flink.gcp.connector.bigtable.source.changestream
                                        .BigtableChangeStreamMutation>
                                builder()
                        .table(TestSources.TABLE)
                        .appProfileId("single-cluster")
                        .deserializer(
                                new io.github.flink.gcp.connector.bigtable.source.serializer
                                        .BigtableChangeStreamMutationDeserializationSchema())
                        .opener(access)
                        .restoreResolver(access)
                        .coordinatorClientFactory(new FakeCoordinatorFactory())
                        .build();
        for (Source<?, ?, ?> source :
                List.of(
                        scan,
                        scan.withTableLineage("catalog.db.scan"),
                        changes,
                        changes.withTableLineage("catalog.db.changes"))) {
            Source<?, ?, ?> copy =
                    InstantiationUtil.deserializeObject(
                            InstantiationUtil.serializeObject(source), getClass().getClassLoader());
            for (Source<?, ?, ?> candidate : List.of(source, copy)) {
                assertThat(((LineageVertexProvider) candidate).getLineageVertex().datasets())
                        .hasSize(1);
            }
        }
        assertThat(ACCESS).hasValue(0);
        assertThatThrownBy(() -> new FakeSamplerFactory().create())
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> new FakeCoordinatorFactory().create())
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> access.open(TestSources.TABLE, null, null))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> access.open(TestSources.TABLE, null, null, null))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> access.resolve(null, null)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> access.useCredentials(null)).isInstanceOf(AssertionError.class);
        assertThat(ACCESS).hasValue(6);
    }

    private static void failAccess() {
        ACCESS.incrementAndGet();
        throw new AssertionError("Lineage must not access the runtime");
    }

    private static final class FakeSamplerFactory implements RowKeySamplerFactory {
        @Override
        public RowKeySampler create() {
            failAccess();
            return null;
        }
    }

    private static final class FakeCoordinatorFactory
            implements ChangeStreamCoordinatorClientFactory {
        @Override
        public ChangeStreamCoordinatorClient create() {
            failAccess();
            return null;
        }
    }

    private static final class FakeAccess
            implements RowStreamOpener, ChangeStreamOpener, ChangeStreamRestoreResolver {
        @Override
        public RowStream open(
                TableDestination table, ByteStringRange range, Filters.Filter filter) {
            failAccess();
            return null;
        }

        @Override
        public void open(
                TableDestination table,
                ChangeStreamPartitionSplit split,
                Instant end,
                ResponseObserver<ChangeStreamRecord> observer) {
            failAccess();
        }

        @Override
        public ChangeStreamPartitionSplit resolve(
                ChangeStreamPartitionSplit split, StartPosition fallback) {
            failAccess();
            return null;
        }

        @Override
        public void useCredentials(CredentialsProvider credentials) {
            failAccess();
        }

        @Override
        public void close() {
            failAccess();
        }
    }
}
