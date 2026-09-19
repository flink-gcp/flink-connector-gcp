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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.TaskInfoImpl;
import org.apache.flink.api.common.functions.util.RuntimeUDFContext;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.state.FunctionSnapshotContext;

import io.github.flink.gcp.connector.bigquery.source.reader.ScriptedRowStreamOpener;
import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializationSchema;
import io.github.flink.gcp.connector.bigquery.source.split.ReadStreamSplit;
import io.github.flink.gcp.connector.testutils.CollectingReaderOutput;
import io.github.flink.gcp.connector.testutils.FakeSourceReaderContext;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BigQueryMultiStreamCheckpointTest {

    @Test
    void anEmptyCheckpointCannotBorrowRecordsFromALaterBarrier() throws Exception {
        BigQueryMultiStreamRealGcpITCase.resetProbe();
        BigQueryMultiStreamRealGcpITCase.FailAfterACheckpoint function =
                new BigQueryMultiStreamRealGcpITCase.FailAfterACheckpoint();
        function.setRuntimeContext(
                new RuntimeUDFContext(
                        new TaskInfoImpl("probe", 1, 0, 1, 0),
                        getClass().getClassLoader(),
                        new ExecutionConfig(),
                        Collections.emptyMap(),
                        new HashMap<>(),
                        UnregisteredMetricsGroup.createOperatorMetricGroup()));
        function.snapshotState(snapshot(1));
        function.map(TestRows.rows(0, 1).get(0));
        function.snapshotState(snapshot(2));

        function.notifyCheckpointComplete(1);
        assertThat(BigQueryMultiStreamRealGcpITCase.checkpointGate).isNotDone();
        function.map(TestRows.rows(1, 1).get(0));
        assertThat(BigQueryMultiStreamRealGcpITCase.FAILED_ONCE).isFalse();

        function.notifyCheckpointComplete(2);
        assertThat(BigQueryMultiStreamRealGcpITCase.checkpointGate).isDone();
        assertThatThrownBy(() -> function.map(TestRows.rows(2, 1).get(0)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Failing the job once, on purpose.");
        assertThat(BigQueryMultiStreamRealGcpITCase.FAILED_ONCE).isTrue();
        assertThat(function.map(TestRows.rows(3, 1).get(0))).isZero();
    }

    @Test
    void aReplacementReaderCanEmitBeforeTheGateOpens() throws Exception {
        BigQueryMultiStreamRealGcpITCase.resetProbe();
        // The previous attempt emitted, then failed before a non-empty checkpoint completed.
        BigQueryMultiStreamRealGcpITCase.SUBTASKS_THAT_READ.add(0);
        String id = getClass().getName();
        String stream = "projects/p/locations/l/sessions/s/streams/one";
        ScriptedRowStreamOpener.reset(id);
        FakeSourceReaderContext context =
                new FakeSourceReaderContext(
                        InternalSourceReaderMetricGroup.wrap(
                                UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup()));
        BigQueryMultiStreamRealGcpITCase.CheckpointGatedSource source =
                new BigQueryMultiStreamRealGcpITCase.CheckpointGatedSource(
                        BigQuerySource.<GenericRecord>builder()
                                .table(TestSources.TABLE)
                                .deserializer(
                                        BigQueryRowDeserializationSchema.genericRecord(
                                                TestRows.SCHEMA_JSON))
                                .rowStreamOpener(
                                        ScriptedRowStreamOpener.singleStream(id, stream, 2, 2))
                                .build());
        CollectingReaderOutput<GenericRecord> output = new CollectingReaderOutput<>();
        try (SourceReader<GenericRecord, ReadStreamSplit> reader = source.createReader(context)) {
            reader.addSplits(
                    Collections.singletonList(
                            new ReadStreamSplit(stream, 0, TestRows.SCHEMA_JSON, null)));
            reader.start();
            reader.isAvailable().get(5, TimeUnit.SECONDS);
            reader.pollNext(output);
            assertThat(reader.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);
            assertThat(reader.isAvailable()).isNotDone();
            assertThat(output.records()).hasSize(1);
            BigQueryMultiStreamRealGcpITCase.checkpointGate.complete(null);
            assertThat(reader.isAvailable()).isDone();
            reader.pollNext(output);
            assertThat(output.records()).hasSize(2);
        }
    }

    private static FunctionSnapshotContext snapshot(long id) {
        return new FunctionSnapshotContext() {
            @Override
            public long getCheckpointId() {
                return id;
            }

            @Override
            public long getCheckpointTimestamp() {
                return 0;
            }
        };
    }
}
