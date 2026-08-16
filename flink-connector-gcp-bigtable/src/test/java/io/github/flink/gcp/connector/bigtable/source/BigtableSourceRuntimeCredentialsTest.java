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

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamEnumeratorState;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamMutation;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.DefaultChangeStreamRestoreResolver;
import io.github.flink.gcp.connector.bigtable.source.readrows.BigtableReadRowsSource;
import io.github.flink.gcp.connector.bigtable.source.readrows.BigtableScanEnumeratorState;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRangeSplit;
import io.github.flink.gcp.connector.bigtable.source.serializer.ChangeStreamMutationDeserializationSchema;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies that source runtime boundaries load, rather than serialize, configured credentials. */
class BigtableSourceRuntimeCredentialsTest {

    private static final String MISSING_KEY = "/missing/mounted-bigtable-key.json";
    private static final String FAILURE =
            "Failed to load the configured Bigtable service-account key file.";

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
        BigtableChangeStreamSource<ChangeStreamMutation> source =
                BigtableChangeStreamSource.<ChangeStreamMutation>builder()
                        .table(TableDestination.of("p", "i", "t"))
                        .appProfileId("single-cluster")
                        .deserializer(new ChangeStreamMutationDeserializationSchema())
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
    void changeStreamRestoreResolutionLoadsCredentialsAtRuntime() {
        DefaultChangeStreamRestoreResolver resolver =
                new DefaultChangeStreamRestoreResolver(
                        TableDestination.of("p", "i", "t"), "single-cluster", MISSING_KEY);
        ChangeStreamPartitionSplit split =
                new ChangeStreamPartitionSplit(
                        "partition-0",
                        ByteStringRange.unbounded(),
                        Collections.emptyList(),
                        Instant.EPOCH);

        assertSanitized(() -> resolver.resolve(split, Optional.empty()));
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
}
