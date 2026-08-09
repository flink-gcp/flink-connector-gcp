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

package io.github.flink.gcp.connector.bigquery.source.reader;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;

import io.github.flink.gcp.connector.bigquery.source.TestRows;
import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializer;
import io.github.flink.gcp.connector.bigquery.source.split.BigQueryReadStreamSplit;
import io.github.flink.gcp.connector.testutils.Awaits;
import io.github.flink.gcp.connector.testutils.CollectingReaderOutput;
import io.github.flink.gcp.connector.testutils.FakeSourceReaderContext;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class BigQuerySourceReaderTest {

    private static final String STREAM = "projects/p/locations/l/sessions/s/streams/one";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final int ROW_COUNT = 10;

    private String openerId;

    @BeforeEach
    void setUp(org.junit.jupiter.api.TestInfo testInfo) {
        // One recording per test: the opener's recordings are static so that a job's deserialized
        // copies write to the same place.
        openerId = BigQuerySourceReaderTest.class.getName() + "#" + testInfo.getDisplayName();
        ScriptedRowStreamOpener.reset(openerId);
    }

    @Test
    void asksForASplitWhenItStartsWithNone() throws Exception {
        TestReaderMetrics metrics = new TestReaderMetrics();
        FakeSourceReaderContext context = new FakeSourceReaderContext(metrics.metricGroup());

        try (BigQuerySourceReader<String> reader = reader(context, metrics)) {
            reader.start();

            assertThat(context.splitRequests()).isEqualTo(1);
        }
    }

    @Test
    void asksForNothingWhenItStartsWithARestoredSplit() throws Exception {
        TestReaderMetrics metrics = new TestReaderMetrics();
        FakeSourceReaderContext context = new FakeSourceReaderContext(metrics.metricGroup());

        try (BigQuerySourceReader<String> reader = reader(context, metrics)) {
            reader.addSplits(Collections.singletonList(split(0)));
            reader.start();

            assertThat(context.splitRequests()).isZero();
        }
    }

    @Test
    void asksForTheNextSplitWhenOneFinishes() throws Exception {
        TestReaderMetrics metrics = new TestReaderMetrics();
        FakeSourceReaderContext context = new FakeSourceReaderContext(metrics.metricGroup());
        CollectingReaderOutput<String> output = new CollectingReaderOutput<>();

        try (BigQuerySourceReader<String> reader = reader(context, metrics)) {
            reader.addSplits(Collections.singletonList(split(0)));
            reader.start();
            pollUntil(reader, output, ROW_COUNT);

            Awaits.await(
                    "the reader to ask for the next split",
                    TIMEOUT,
                    () -> {
                        poll(reader, output);
                        return context.splitRequests() >= 1;
                    });

            // Exactly one: a reader that asked twice would have the enumerator hand this subtask a
            // second stream while it is still reading the first.
            assertThat(context.splitRequests()).isEqualTo(1);
        }
    }

    @Test
    void resumesAtTheCheckpointedOffsetWithoutDuplicatesOrGaps() throws Exception {
        // The test the emulator cannot stand in for: it ignores ReadRowsRequest.offset and answers
        // every call from row zero, so a resume asserted against it would pass while proving the
        // opposite. Real BigQuery honours the offset (measured 2026-08-09), and so does the opener
        // behind this test.
        TestReaderMetrics metrics = new TestReaderMetrics();
        CollectingReaderOutput<String> first = new CollectingReaderOutput<>();
        List<BigQueryReadStreamSplit> checkpoint;

        try (BigQuerySourceReader<String> reader =
                reader(new FakeSourceReaderContext(metrics.metricGroup()), metrics)) {
            reader.addSplits(Collections.singletonList(split(0)));
            reader.start();
            pollUntil(reader, first, 4);
            checkpoint = reader.snapshotState(1L);
        }

        assertThat(checkpoint).hasSize(1);
        assertThat(checkpoint.get(0).getOffset()).isEqualTo(4);

        TestReaderMetrics restoredMetrics = new TestReaderMetrics();
        CollectingReaderOutput<String> second = new CollectingReaderOutput<>();
        try (BigQuerySourceReader<String> restored =
                reader(
                        new FakeSourceReaderContext(restoredMetrics.metricGroup()),
                        restoredMetrics)) {
            restored.addSplits(checkpoint);
            restored.start();
            pollUntil(restored, second, ROW_COUNT - 4);
        }

        assertThat(first.records()).containsExactly("row-0", "row-1", "row-2", "row-3");
        assertThat(second.records())
                .containsExactly("row-4", "row-5", "row-6", "row-7", "row-8", "row-9");
        assertThat(ScriptedRowStreamOpener.offsets(openerId)).containsExactly(0L, 4L);
    }

    @Test
    void closesTheOpenerOnceAfterItsSplitReaderIsDown() throws Exception {
        // A split has to have been read, or no split reader is ever built and this would pass even
        // if the split reader closed the opener it shares with the others.
        TestReaderMetrics metrics = new TestReaderMetrics();
        CollectingReaderOutput<String> output = new CollectingReaderOutput<>();
        BigQuerySourceReader<String> reader =
                reader(new FakeSourceReaderContext(metrics.metricGroup()), metrics);
        reader.addSplits(Collections.singletonList(split(0)));
        reader.start();
        pollUntil(reader, output, ROW_COUNT);

        reader.close();

        assertThat(ScriptedRowStreamOpener.closeCount(openerId)).isEqualTo(1);
    }

    private BigQueryReadStreamSplit split(long offset) {
        return new BigQueryReadStreamSplit(STREAM, offset, TestRows.SCHEMA_JSON);
    }

    private BigQuerySourceReader<String> reader(
            FakeSourceReaderContext context, TestReaderMetrics metrics) {
        ScriptedRowStreamOpener opener =
                ScriptedRowStreamOpener.singleStream(openerId, STREAM, ROW_COUNT, 3);
        Supplier<SplitReader<GenericRecord, BigQueryReadStreamSplit>> splitReaderSupplier =
                () -> new BigQuerySplitReader(opener, 2, null, metrics.metrics());
        return new BigQuerySourceReader<>(
                splitReaderSupplier,
                new BigQueryRecordEmitter<>(nameDeserializer(), metrics.metrics()),
                new Configuration(),
                context,
                opener);
    }

    private static BigQueryRowDeserializer<String> nameDeserializer() {
        return new BigQueryRowDeserializer<String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public String deserialize(GenericRecord row) {
                return String.valueOf(row.get("name"));
            }

            @Override
            public TypeInformation<String> getProducedType() {
                return Types.STRING;
            }
        };
    }

    private static void pollUntil(
            BigQuerySourceReader<String> reader, CollectingReaderOutput<String> output, int records)
            throws Exception {
        Awaits.await(
                records + " record(s) to be emitted",
                TIMEOUT,
                () -> {
                    if (output.records().size() >= records) {
                        return true;
                    }
                    poll(reader, output);
                    return output.records().size() >= records;
                });
    }

    private static void poll(
            BigQuerySourceReader<String> reader, CollectingReaderOutput<String> output) {
        try {
            reader.pollNext(output);
        } catch (Exception e) {
            throw new AssertionError("pollNext failed", e);
        }
    }
}
