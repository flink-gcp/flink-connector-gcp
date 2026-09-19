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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import io.github.flink.gcp.connector.bigquery.source.enumerator.ScriptedReadSessionCreator;
import io.github.flink.gcp.connector.bigquery.source.reader.ScriptedRowStreamOpener;
import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializationSchema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Exercises the real-service probe's checkpoint gate against an unpaced, finite read. */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class BigQueryMultiStreamCheckpointITCase {

    @Test
    void aFastReadStillFailsAndResumesFromANonzeroOffset() throws Exception {
        String id = getClass().getName();
        ScriptedRowStreamOpener.reset(id);
        BigQueryMultiStreamRealGcpITCase.resetProbe();
        Map<String, int[]> rows = new HashMap<>();
        rows.put(ScriptedReadSessionCreator.streamName(0), new int[] {0, 100});
        rows.put(ScriptedReadSessionCreator.streamName(1), new int[] {100, 100});

        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 2);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(2);
        env.enableCheckpointing(1000);

        // Both streams fit in a single response and fetch, without sleeps or a server-side gate.
        // Only the probe's reader gate keeps the next row available for failure injection.
        BigQueryMultiStreamRealGcpITCase.CheckpointGatedSource source =
                new BigQueryMultiStreamRealGcpITCase.CheckpointGatedSource(
                        BigQuerySource.<GenericRecord>builder()
                                .table(TestSources.TABLE)
                                .deserializer(
                                        BigQueryRowDeserializationSchema.genericRecord(
                                                TestRows.SCHEMA_JSON))
                                .sessionCreatorFactory(
                                        ScriptedReadSessionCreator.Factory.withStreams(2))
                                .rowStreamOpener(new ScriptedRowStreamOpener(id, rows, 100))
                                .build());
        long count = 0;
        try (CloseableIterator<Byte> records =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "fast-bigquery")
                        .map(new BigQueryMultiStreamRealGcpITCase.FailAfterACheckpoint())
                        .executeAndCollect()) {
            while (records.hasNext()) {
                records.next();
                count++;
            }
        }

        assertThat(BigQueryMultiStreamRealGcpITCase.FAILED_ONCE).isTrue();
        assertThat(count).isEqualTo(200);
        assertThat(BigQueryMultiStreamRealGcpITCase.SUBTASKS_THAT_READ).hasSize(2);
        assertThat(ScriptedRowStreamOpener.opens(id)).hasSizeGreaterThan(2);
        assertThat(ScriptedRowStreamOpener.offsets(id))
                .as("a completed non-empty checkpoint retained a stream with unread rows")
                .anyMatch(offset -> offset > 0 && offset < 100);
    }
}
