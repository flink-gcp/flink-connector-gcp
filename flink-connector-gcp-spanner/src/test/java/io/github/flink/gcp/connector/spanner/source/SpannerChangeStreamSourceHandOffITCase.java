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

package io.github.flink.gcp.connector.spanner.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;
import io.github.flink.gcp.connector.spanner.source.changestream.enumerator.SpannerChangeStreamCoordinatorClient;
import io.github.flink.gcp.connector.spanner.source.changestream.enumerator.SpannerChangeStreamSplitEnumerator;
import io.github.flink.gcp.connector.spanner.source.changestream.reader.LineageScriptedQueryClientFactory;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerChangeStreamDeserializationSchema;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs the partition hand-off through Flink's own source coordinator, which the coordinator unit
 * tests replace with a fake: its checkpoint-completion notifications and its periodic calls.
 */
@Timeout(120)
class SpannerChangeStreamSourceHandOffITCase {

    private static final String NO_CHECKPOINT_WARNING = "No checkpoint has completed";

    @Test
    void aChildOfAnotherPartitionStartsOnceACheckpointCompletes() throws Exception {
        try (LogCapture capture = LogCapture.of(SpannerChangeStreamSplitEnumerator.class)) {
            List<String> partitions = run(true, Duration.ofMinutes(10));

            assertThat(partitions)
                    .containsExactlyInAnyOrder("parent-0", "parent-1", "child-0", "child-1");
            assertThat(capture.getMessages()).noneMatch(m -> m.contains(NO_CHECKPOINT_WARNING));
        }
    }

    @Test
    void withoutCheckpointingTheChildrenStartAfterTheFirstCheckpointTimeout() throws Exception {
        try (LogCapture capture = LogCapture.of(SpannerChangeStreamSplitEnumerator.class)) {
            List<String> partitions = run(false, Duration.ofSeconds(1));

            assertThat(partitions)
                    .containsExactlyInAnyOrder("parent-0", "parent-1", "child-0", "child-1");
            // The warning proves the children were held, so the builder's settings reached the
            // coordinator, and that Flink ran the periodic check that released them.
            assertThat(capture.getMessages())
                    .filteredOn(m -> m.contains(NO_CHECKPOINT_WARNING))
                    .singleElement()
                    .satisfies(m -> assertThat(m).contains("PT1S"));
        }
    }

    private static List<String> run(boolean checkpointing, Duration firstCheckpointTimeout)
            throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(2);
        if (checkpointing) {
            env.enableCheckpointing(100);
        }
        SpannerChangeStreamSource<String> source =
                SpannerChangeStreamSource.<String>builder()
                        .database(DatabaseDestination.of("project", "instance", "database"))
                        .changeStreamName("changes")
                        .deserializer(new PartitionNames())
                        .startPosition(StartPosition.latest())
                        .boundedTimestamp(Instant.now().plus(Duration.ofHours(1)))
                        .firstCheckpointTimeout(firstCheckpointTimeout)
                        .coordinatorClientFactory(clientMetrics -> new NoOpCoordinatorClient())
                        .queryClientFactory(new LineageScriptedQueryClientFactory(2))
                        .build();
        List<String> partitions = new ArrayList<>();
        try (CloseableIterator<String> collected =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "spanner-change-stream")
                        .executeAndCollect()) {
            collected.forEachRemaining(partitions::add);
        }
        return partitions;
    }

    private static final class NoOpCoordinatorClient
            implements SpannerChangeStreamCoordinatorClient {

        @Override
        public Duration initialize() {
            return Duration.ofDays(7);
        }

        @Override
        public void close() {}
    }

    private static final class PartitionNames
            implements SpannerChangeStreamDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(DataChangeRecord record, Collector<String> out) {
            out.collect(record.getTableName());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }
}
