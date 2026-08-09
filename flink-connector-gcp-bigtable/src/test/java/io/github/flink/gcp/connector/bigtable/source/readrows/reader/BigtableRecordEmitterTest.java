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

package io.github.flink.gcp.connector.bigtable.source.readrows.reader;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.BigtableMetricNames;
import io.github.flink.gcp.connector.bigtable.source.TestRows;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRangeSplit;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRangeSplitState;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableRowDeserializationSchema;
import io.github.flink.gcp.connector.testutils.CollectingSourceOutput;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BigtableRecordEmitter}. */
@Timeout(30)
class BigtableRecordEmitterTest {

    private final TestReaderMetrics metrics = new TestReaderMetrics();

    private static RowRangeSplitState state() {
        return new RowRangeSplitState(new RowRangeSplit("0", ByteStringRange.unbounded()));
    }

    private BigtableRecordEmitter<String> emitter(
            BigtableRowDeserializationSchema<String> deserializer) {
        return new BigtableRecordEmitter<>(deserializer, metrics.metrics());
    }

    @Test
    void emitsTheRecordsARowProduced() throws Exception {
        CollectingSourceOutput<String> output = new CollectingSourceOutput<>();
        RowRangeSplitState state = state();

        emitter(fanOut(2)).emitRecord(TestRows.row("a"), output, state);

        assertThat(output.records()).containsExactly("a#0", "a#1");
        assertThat(metrics.counter(BigtableMetricNames.RECORDS_SKIPPED)).isZero();
    }

    @Test
    void advancesTheSplitByOneRowWhateverTheRowProduced() throws Exception {
        // The invariant the whole resume design rests on: progress is measured in rows, not in
        // records, because the range is resumed at a row key.
        for (int records : new int[] {0, 1, 5}) {
            RowRangeSplitState state = state();

            emitter(fanOut(records))
                    .emitRecord(TestRows.row("m"), new CollectingSourceOutput<>(), state);

            assertThat(state.getLastEmittedKey())
                    .as("a row producing %s record(s) still moves the resume point", records)
                    .isEqualTo(ByteString.copyFromUtf8("m"));
        }
    }

    @Test
    void countsARowThatProducedNothingAsSkipped() throws Exception {
        CollectingSourceOutput<String> output = new CollectingSourceOutput<>();

        emitter(fanOut(0)).emitRecord(TestRows.row("a"), output, state());

        assertThat(output.records()).isEmpty();
        assertThat(metrics.counter(BigtableMetricNames.RECORDS_SKIPPED)).isEqualTo(1);
    }

    @Test
    void countsOnlyTheRowsThatProducedNothing() throws Exception {
        BigtableRecordEmitter<String> emitter =
                emitter(
                        new BigtableRowDeserializationSchema<String>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public void deserialize(Row row, Collector<String> out) {
                                if (!TestRows.keyOf(row).startsWith("skip")) {
                                    out.collect(TestRows.keyOf(row));
                                }
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }
                        });
        CollectingSourceOutput<String> output = new CollectingSourceOutput<>();

        emitter.emitRecord(TestRows.row("a"), output, state());
        emitter.emitRecord(TestRows.row("skip-1"), output, state());
        emitter.emitRecord(TestRows.row("b"), output, state());
        emitter.emitRecord(TestRows.row("skip-2"), output, state());

        assertThat(output.records()).containsExactly("a", "b");
        assertThat(metrics.counter(BigtableMetricNames.RECORDS_SKIPPED)).isEqualTo(2);
    }

    @Test
    void emitsRecordsWithoutATimestamp() throws Exception {
        // A Bigtable row has a timestamp per cell rather than one per row, so any row-level event
        // time would be a choice this connector made on the job's behalf.
        CollectingSourceOutput<String> output = new CollectingSourceOutput<>();

        emitter(fanOut(1)).emitRecord(TestRows.row("a"), output, state());

        assertThat(output.timestamps()).containsExactly((Long) null);
    }

    @Test
    void leavesTheSplitWhereItWasWhenTheDownstreamOutputThrows() {
        // A failing chained operator is not a row this source consumed: the throw escapes before
        // the resume point moves, so a restore replays the row rather than skipping it.
        RowRangeSplitState state = state();
        CollectingSourceOutput<String> output = new CollectingSourceOutput<>();
        output.failOnCollect(new IllegalStateException("downstream is full"));

        assertThatThrownBy(() -> emitter(fanOut(1)).emitRecord(TestRows.row("a"), output, state))
                .isInstanceOf(IllegalStateException.class);
        assertThat(state.getLastEmittedKey()).isNull();
    }

    @Test
    void leavesTheSplitWhereItWasWhenTheDeserializerThrows() {
        RowRangeSplitState state = state();
        BigtableRecordEmitter<String> emitter =
                emitter(
                        new BigtableRowDeserializationSchema<String>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public void deserialize(Row row, Collector<String> out)
                                    throws IOException {
                                throw new IOException("bad row");
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }
                        });

        assertThatThrownBy(
                        () ->
                                emitter.emitRecord(
                                        TestRows.row("a"), new CollectingSourceOutput<>(), state))
                .isInstanceOf(IOException.class);
        assertThat(state.getLastEmittedKey()).isNull();
    }

    @Test
    void refusesACollectorUsedOutsideTheCallItWasHandedTo() throws Exception {
        // The collector is one instance retargeted per row, so a deserializer that squirrelled it
        // away would otherwise write into whichever row happened to be current.
        Collector<String>[] escaped = new Collector[1];
        CollectingSourceOutput<String> output = new CollectingSourceOutput<>();
        BigtableRecordEmitter<String> emitter =
                emitter(
                        new BigtableRowDeserializationSchema<String>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public void deserialize(Row row, Collector<String> out) {
                                escaped[0] = out;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }
                        });
        emitter.emitRecord(TestRows.row("a"), output, state());

        assertThatThrownBy(() -> escaped[0].collect("late"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outside the deserialize call");
    }

    /** Returns a deserializer producing the given number of records per row. */
    private static BigtableRowDeserializationSchema<String> fanOut(int records) {
        return new BigtableRowDeserializationSchema<String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void deserialize(Row row, Collector<String> out) {
                for (int i = 0; i < records; i++) {
                    out.collect(TestRows.keyOf(row) + "#" + i);
                }
            }

            @Override
            public TypeInformation<String> getProducedType() {
                return TypeInformation.of(String.class);
            }
        };
    }
}
