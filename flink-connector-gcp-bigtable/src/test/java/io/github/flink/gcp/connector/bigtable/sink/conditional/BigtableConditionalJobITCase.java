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

package io.github.flink.gcp.connector.bigtable.sink.conditional;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableEmulatorITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Public sink and async entry points exercised by jobs against the emulator. */
class BigtableConditionalJobITCase extends AbstractBigtableEmulatorITCase {
    @Test
    void asyncEmitsBothBranchesAndAHistoricalMatchCannotSatisfyLatestEquality() throws Exception {
        TableDestination table = createTable("conditional-job-latest");
        writeCell(table, "match", FAMILY, "name", 1000, "expected");
        writeCell(table, "history", FAMILY, "name", 1000, "expected");
        writeCell(table, "history", FAMILY, "name", 2000, "newer");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> inputs = env.fromData("match", "history", "skip");
        BigtableConditionalAsync<String> async =
                BigtableConditionalAsync.<String>builder()
                        .table(table)
                        .emulatorEndpoint(
                                EmulatorEndpoint.parse(emulatorEndpoint(), "emulatorEndpoint"))
                        .serializer(
                                (key, context) ->
                                        key.equals("skip")
                                                ? null
                                                : ConditionalRequest.of(
                                                        ByteString.copyFromUtf8(key),
                                                        ConditionalFilter.latestCellValueEquals(
                                                                "cf",
                                                                ByteString.copyFromUtf8("name"),
                                                                ByteString.copyFromUtf8(
                                                                        "expected")),
                                                        List.of(
                                                                ConditionalMutation.deleteCells(
                                                                        "cf",
                                                                        ByteString.copyFromUtf8(
                                                                                "name")),
                                                                ConditionalMutation.setCell(
                                                                        "cf",
                                                                        ByteString.copyFromUtf8(
                                                                                "name"),
                                                                        3000,
                                                                        ByteString.copyFromUtf8(
                                                                                "accepted"))),
                                                        List.of(
                                                                ConditionalMutation.setCell(
                                                                        "cf",
                                                                        ByteString.copyFromUtf8(
                                                                                "result"),
                                                                        3000,
                                                                        ByteString.copyFromUtf8(
                                                                                "rejected")))))
                        .build();
        List<Tuple2<String, ConditionalResult>> results = new ArrayList<>();
        try (CloseableIterator<Tuple2<String, ConditionalResult>> iterator =
                async.orderedWait(inputs, Duration.ofSeconds(30)).executeAndCollect()) {
            iterator.forEachRemaining(results::add);
        }
        assertThat(results).extracting(pair -> pair.f0).containsExactly("match", "history");
        assertThat(results.get(0).f1.isPredicateMatched()).isTrue();
        assertThat(results.get(1).f1.isPredicateMatched()).isFalse();
        assertThat(results)
                .allSatisfy(
                        pair -> {
                            assertThat(pair.f1.getDestination()).isEqualTo(table);
                            assertThat(pair.f1.getRowKey().toStringUtf8()).isEqualTo(pair.f0);
                            assertThat(pair.f1.isSelectedBranchHasMutations()).isTrue();
                        });
        List<com.google.cloud.bigtable.data.v2.models.Row> rows = readRows(table);
        assertThat(rows.get(0).getCells("cf", "name")).hasSize(2);
        assertThat(rows.get(0).getCells("cf", "result").get(0).getValue().toStringUtf8())
                .isEqualTo("rejected");
        assertThat(rows.get(1).getCells("cf", "name")).hasSize(1);
        assertThat(rows.get(1).getCells("cf", "name").get(0).getValue().toStringUtf8())
                .isEqualTo("accepted");
    }

    @Test
    void sinkPreservesBinaryPredicatesAndDiscardsSuccessfulResults() throws Exception {
        TableDestination table = createTable("conditional-job-binary");
        ByteString bytes = ByteString.copyFrom(new byte[] {0, 10, (byte) 255, '.'});
        StreamExecutionEnvironment seed = StreamExecutionEnvironment.getExecutionEnvironment();
        seed.setParallelism(1);
        seed.fromData("row")
                .sinkTo(
                        BigtableConditionalSink.<String>builder()
                                .table(table)
                                .emulatorEndpoint(
                                        EmulatorEndpoint.parse(
                                                emulatorEndpoint(), "emulatorEndpoint"))
                                .serializer(
                                        (key, context) ->
                                                ConditionalRequest.of(
                                                        ByteString.copyFromUtf8(key),
                                                        ConditionalFilter.rowExists(),
                                                        List.of(),
                                                        List.of(
                                                                ConditionalMutation.setCell(
                                                                        "cf", bytes, 1000, bytes))))
                                .build());
        seed.execute();
        StreamExecutionEnvironment update = StreamExecutionEnvironment.getExecutionEnvironment();
        update.setParallelism(1);
        update.fromData("row")
                .sinkTo(
                        BigtableConditionalSink.<String>builder()
                                .table(table)
                                .emulatorEndpoint(
                                        EmulatorEndpoint.parse(
                                                emulatorEndpoint(), "emulatorEndpoint"))
                                .emptyBranchPolicy(EmptyBranchPolicy.FAIL)
                                .serializer(
                                        (key, context) ->
                                                ConditionalRequest.of(
                                                        ByteString.copyFromUtf8(key),
                                                        ConditionalFilter.latestCellValueEquals(
                                                                "cf", bytes, bytes),
                                                        List.of(
                                                                ConditionalMutation.deleteFamily(
                                                                        "cf"),
                                                                ConditionalMutation.setCell(
                                                                        "cf",
                                                                        ByteString.copyFromUtf8(
                                                                                "done"),
                                                                        2000,
                                                                        bytes)),
                                                        List.of()))
                                .build());
        update.execute();
        assertThat(readRows(table).get(0).getCells()).hasSize(1);
        assertThat(readRows(table).get(0).getCells("cf", "done").get(0).getValue())
                .isEqualTo(bytes);
    }
}
