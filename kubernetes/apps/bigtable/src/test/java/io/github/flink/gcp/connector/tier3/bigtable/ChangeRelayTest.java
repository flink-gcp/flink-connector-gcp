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

package io.github.flink.gcp.connector.tier3.bigtable;

import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;

import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutation;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutation.AddToCellEntry;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutation.Entry;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutation.Int64Value;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutation.RawTimestamp;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutation.RawValue;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutation.SetCellEntry;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ChangeRelayTest {
    private static final Instant COMMIT = Instant.parse("2026-10-18T00:00:01.234567891Z");

    @Test
    void aStagedCommitIsRelayedByItsRowKeyAndCommitTimeInMicroseconds() {
        // What the staged sink's conditional commit applies: the contribution and its marker.
        var changes =
                deserialize(
                        mutation(
                                "a/0000042",
                                new SetCellEntry(
                                        RecoveryOptions.MARKER_FAMILY,
                                        ByteString.copyFromUtf8("envelope"),
                                        0,
                                        ByteString.copyFromUtf8("1")),
                                contribution(RecoveryOptions.SUM_FAMILY)));
        assertThat(changes).containsExactly(Tuple2.of("a/0000042", 1_792_281_601_234_567L));
    }

    @Test
    void aChangeWithoutAContributionIsSkippedAndAForeignRowIsKept() {
        assertThat(
                        deserialize(
                                mutation(
                                        "a/0000042",
                                        new SetCellEntry(
                                                RecoveryOptions.MARKER_FAMILY,
                                                ByteString.copyFromUtf8("envelope"),
                                                0,
                                                ByteString.copyFromUtf8("1")))))
                .isEmpty();
        assertThat(deserialize(mutation("a/0000042", contribution("other")))).isEmpty();
        // The oracle, not the job, reports a row outside the input.
        assertThat(deserialize(mutation("foreign", contribution(RecoveryOptions.SUM_FAMILY))))
                .extracting(change -> change.f0)
                .containsExactly("foreign");
    }

    @Test
    void everyDeliveryHasItsOwnQualifierSoAnyRedeliveryAddsOne() throws Exception {
        var first = new ChangeRelay.Delivery("upgrade");
        first.open(DefaultOpenContext.INSTANCE);
        var replay = new ChangeRelay.Delivery("upgrade");
        replay.open(DefaultOpenContext.INSTANCE);
        var change = Tuple2.of("a/0000042", 7L);
        var delivered = first.map(change);
        assertThat(delivered.f0).isEqualTo("a/0000042");
        assertThat(delivered.f1).matches("upgrade/[a-f0-9-]{36}/0");
        assertThat(delivered.f2).isEqualTo(7L);
        String instance = delivered.f1.substring(0, delivered.f1.length() - 1);
        // The same change again in the same task instance, as after a partition restart.
        assertThat(first.map(change).f1).isEqualTo(instance + "1");
        assertThat(first.map(Tuple2.of("b/0000043", 8L)).f1).isEqualTo(instance + "2");
        // A new task instance, as after a restore, starts its own count.
        assertThat(replay.map(change).f1)
                .matches("upgrade/[a-f0-9-]{36}/0")
                .doesNotStartWith(instance);
    }

    @Test
    void eachRestoredCopyOfTheSameFunctionDrawsItsOwnInstance() throws Exception {
        // A failover inside one job deserializes the same function again; each copy must still
        // name its own deliveries, or a replay would overwrite the cell it replays.
        var function = new ChangeRelay.Delivery("initial");
        var restarted = InstantiationUtil.clone(function);
        var other = InstantiationUtil.clone(function);
        restarted.open(DefaultOpenContext.INSTANCE);
        other.open(DefaultOpenContext.INSTANCE);
        var change = Tuple2.of("a/0000042", 7L);
        assertThat(restarted.map(change).f1).isNotEqualTo(other.map(change).f1);
    }

    @Test
    void aDeliveryIsOneCellAtAFixedTimestampHoldingTheCommitTime() {
        var entry =
                ChangeRelay.mutation(Tuple3.of("a/0000042", "initial/instance", 1_234L)).toProto();
        assertThat(entry.getRowKey().toStringUtf8()).isEqualTo("a/0000042");
        assertThat(entry.getMutationsList())
                .singleElement()
                .satisfies(
                        mutation -> {
                            assertThat(mutation.getMutationCase())
                                    .isEqualTo(Mutation.MutationCase.SET_CELL);
                            var cell = mutation.getSetCell();
                            assertThat(cell.getFamilyName())
                                    .isEqualTo(RecoveryOptions.RELAY_FAMILY);
                            assertThat(cell.getColumnQualifier().toStringUtf8())
                                    .isEqualTo("initial/instance");
                            assertThat(cell.getTimestampMicros()).isZero();
                            assertThat(cell.getValue().toStringUtf8()).isEqualTo("1234");
                        });
    }

    private static AddToCellEntry contribution(String family) {
        return new AddToCellEntry(
                family,
                new RawValue(ByteString.copyFromUtf8(RecoveryOptions.SUM_QUALIFIER)),
                new RawTimestamp(RecoveryOptions.SUM_TIMESTAMP_MICROS),
                new Int64Value(1));
    }

    private static BigtableChangeStreamMutation mutation(String row, Entry... entries) {
        return new BigtableChangeStreamMutation(
                ByteString.copyFromUtf8(row),
                BigtableChangeStreamMutation.MutationType.USER,
                "c1",
                COMMIT,
                0,
                "token",
                COMMIT,
                List.of(entries));
    }

    private static List<Tuple2<String, Long>> deserialize(BigtableChangeStreamMutation mutation) {
        List<Tuple2<String, Long>> out = new ArrayList<>();
        new ChangeRelay.Deserializer()
                .deserialize(
                        mutation,
                        new Collector<>() {
                            @Override
                            public void collect(Tuple2<String, Long> record) {
                                out.add(record);
                            }

                            @Override
                            public void close() {}
                        });
        return out;
    }
}
