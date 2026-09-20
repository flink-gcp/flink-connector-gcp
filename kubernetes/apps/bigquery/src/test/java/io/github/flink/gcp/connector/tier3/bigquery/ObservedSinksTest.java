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

package io.github.flink.gcp.connector.tier3.bigquery;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.util.InstantiationUtil;

import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryBufferedStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.BigQueryBufferedStreamWriter;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.BigQueryDefaultStreamWriter;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.BufferedStreamWriterState;
import io.github.flink.gcp.connector.testutils.LogCapture;
import io.github.flink.gcp.connector.testutils.StubWriterInitContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ResourceLock("bigquery-appender-logger")
class ObservedSinksTest {
    @ParameterizedTest
    @ValueSource(strings = {"ALO", "EO"})
    void serializedSinkKeepsTheProductionGraphAndCreatesAnObserverForEveryWriter(String mode)
            throws Exception {
        var options = RecoveryOptions.parse(RecoveryOptionsTest.arguments("--mode", mode));
        Sink<Long> sink =
                InstantiationUtil.clone(
                        BigQueryRecoveryJob.sink(options, "127.0.0.1:1", "http://127.0.0.1:1"),
                        getClass().getClassLoader());
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        BigQueryRecoveryJob.configure(env, options, sink);
        assertThat(
                        StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph())
                                .getNumberOfVertices())
                .isGreaterThanOrEqualTo(2);
        try (var capture = LogCapture.of(AppenderObservations.class, LogCapture.Level.INFO)) {
            for (int subtask = 0; subtask < 2; subtask++) {
                try (var writer = sink.createWriter(new StubWriterInitContext(subtask))) {
                    assertThat(writer)
                            .isInstanceOf(
                                    mode.equals("ALO")
                                            ? BigQueryDefaultStreamWriter.class
                                            : BigQueryBufferedStreamWriter.class);
                }
            }
            assertThat(capture.getMessages()).hasSize(2);
            assertThat(capture.getMessages().get(0))
                    .contains("mode=" + mode, "subtask=0", "sequence=1");
            assertThat(capture.getMessages().get(1))
                    .contains("mode=" + mode, "subtask=1", "sequence=1");
            var ids =
                    capture.getMessages().stream()
                            .map(line -> line.split(" writer=")[1].split(" ")[0])
                            .toList();
            assertThat(ids).doesNotHaveDuplicates();
        }
    }

    @Test
    void restoredBufferedWriterGetsAFreshObserverAndCommitterDoesNot() throws Exception {
        var options =
                RecoveryOptions.parse(
                        RecoveryOptionsTest.arguments(
                                "--phase", "upgrade", "--require-restored", "true"));
        var sink =
                (BigQueryBufferedStreamSink<Long>)
                        BigQueryRecoveryJob.sink(options, "127.0.0.1:1", "http://127.0.0.1:1");
        try (var capture = LogCapture.of(AppenderObservations.class, LogCapture.Level.INFO)) {
            var restored =
                    new BufferedStreamWriterState(options.table(0), "restored-stream", 123, 7);
            var serializer = sink.getWriterStateSerializer();
            var decoded =
                    serializer.deserialize(serializer.getVersion(), serializer.serialize(restored));
            try (var writer = sink.restoreWriter(new StubWriterInitContext(1), List.of(decoded))) {
                assertThat(writer).isInstanceOf(BigQueryBufferedStreamWriter.class);
                assertThat(writer.snapshotState(8))
                        .singleElement()
                        .satisfies(
                                state -> {
                                    assertThat(state.getDestination()).isEqualTo(options.table(0));
                                    assertThat(state.getStreamName()).isEqualTo("restored-stream");
                                    assertThat(state.getNextOffset()).isEqualTo(123);
                                    assertThat(state.getCheckpointId()).isEqualTo(8);
                                });
            }
            try (var committer = sink.createCommitter(null)) {
                assertThat(committer).isNotNull();
            }
            assertThat(capture.getMessages())
                    .singleElement()
                    .satisfies(
                            line ->
                                    assertThat(line)
                                            .contains(
                                                    "phase=upgrade",
                                                    "mode=EO",
                                                    "subtask=1",
                                                    "operation=writer"));
        }
    }
}
