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

package io.github.flink.gcp.connector.bigtable.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.ChangeStream;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.ChangeStreamOpener;
import io.github.flink.gcp.connector.bigtable.source.serializer.ChangeStreamMutationDeserializationSchema;
import io.github.flink.gcp.connector.testutils.FakeSourceReaderContext;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BigtableChangeStreamSourceBuilderTest {

    @Test
    void requiresTableDeserializerAndAppProfile() {
        assertThatThrownBy(() -> BigtableChangeStreamSource.<ChangeStreamMutation>builder().build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("table(...)");
        assertThatThrownBy(
                        () ->
                                BigtableChangeStreamSource.<ChangeStreamMutation>builder()
                                        .table(TableDestination.of("p", "i", "t"))
                                        .build())
                .hasMessageContaining("deserializer(...)");
    }

    @Test
    void endTimeMakesOnlyThatSourceBounded() {
        BigtableChangeStreamSource<ChangeStreamMutation> continuous = minimal().build();
        BigtableChangeStreamSource<ChangeStreamMutation> bounded =
                minimal().endTime(Instant.parse("2026-08-11T00:00:00Z")).build();

        assertThat(continuous.getBoundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
        assertThat(bounded.getBoundedness()).isEqualTo(Boundedness.BOUNDED);
        assertThat(bounded.getProducedType())
                .isEqualTo(new ChangeStreamMutationDeserializationSchema().getProducedType());
    }

    @Test
    void readerReplacesAnExpiredRestoredSplitBeforeItStarts() throws Exception {
        Instant fallback = Instant.parse("2026-08-11T01:00:00Z");
        ChangeStreamPartitionSplit restored =
                new ChangeStreamPartitionSplit(
                        "restored",
                        ByteStringRange.unbounded(),
                        Collections.emptyList(),
                        Instant.parse("2026-08-01T00:00:00Z"));
        BigtableChangeStreamSource<ChangeStreamMutation> source =
                minimal()
                        .opener(new NoOpChangeStreamOpener())
                        .restoreResolver((split, ignored) -> split.restartAt(fallback))
                        .build();
        FakeSourceReaderContext context =
                new FakeSourceReaderContext(
                        InternalSourceReaderMetricGroup.mock(
                                new MetricListener().getMetricGroup()));
        SourceReader<ChangeStreamMutation, ChangeStreamPartitionSplit> reader =
                source.createReader(context);

        reader.addSplits(Collections.singletonList(restored));

        assertThat(reader.snapshotState(1L))
                .singleElement()
                .satisfies(
                        split -> {
                            assertThat(split.getContinuationTokens()).isEmpty();
                            assertThat(split.getLowWatermark()).isEqualTo(fallback);
                        });
        reader.close();
    }

    @Test
    void sourceConfigurationSurvivesJobSubmissionSerialization() throws Exception {
        BigtableChangeStreamSource<ChangeStreamMutation> source = minimal().build();

        byte[] serialized = InstantiationUtil.serializeObject(source);
        Object restored =
                InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());

        assertThat(restored).isInstanceOf(BigtableChangeStreamSource.class);
        assertThat(((BigtableChangeStreamSource<?>) restored).getBoundedness())
                .isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
    }

    private static BigtableChangeStreamSourceBuilder<ChangeStreamMutation> minimal() {
        return BigtableChangeStreamSource.<ChangeStreamMutation>builder()
                .table(TableDestination.of("p", "i", "t"))
                .appProfileId("single-cluster")
                .deserializer(new ChangeStreamMutationDeserializationSchema());
    }

    private static final class NoOpChangeStreamOpener implements ChangeStreamOpener {
        @Override
        public ChangeStream open(
                TableDestination table, ChangeStreamPartitionSplit split, Instant endTime) {
            return new ChangeStream() {
                private final CountDownLatch cancelled = new CountDownLatch(1);

                @Override
                public com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord next() {
                    try {
                        cancelled.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return null;
                }

                @Override
                public void cancel() {
                    cancelled.countDown();
                }

                @Override
                public void close() {
                    cancel();
                }
            };
        }

        @Override
        public void close() throws IOException {}
    }
}
