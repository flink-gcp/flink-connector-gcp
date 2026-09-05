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

package io.github.flink.gcp.connector.bigtable.sink.readmodifywrite;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableEmulatorITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Exercises the public job graph, response serialization and production client path. */
class BigtableReadModifyWriteJobITCase extends AbstractBigtableEmulatorITCase {
    @Test
    void asyncPreservesOrderedRulesAndEmitsOnlyChangedCells() throws Exception {
        TableDestination table = createTable("rmw-job-results");
        writeCell(table, "row", "cf", "note", 1000, "some");
        writeCell(table, "row", "cf", "untouched", 1000, "keep");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        BigtableReadModifyWriteAsync<String> async =
                BigtableReadModifyWriteAsync.<String>builder()
                        .table(table)
                        .emulatorEndpoint(
                                EmulatorEndpoint.parse(emulatorEndpoint(), "emulatorEndpoint"))
                        .serializer(
                                (key, context) ->
                                        key.equals("skip")
                                                ? null
                                                : ReadModifyWriteRequest.of(
                                                        bytes(key),
                                                        List.of(
                                                                ReadModifyWriteRule.append(
                                                                        "cf",
                                                                        bytes("note"),
                                                                        bytes("thing")),
                                                                ReadModifyWriteRule.increment(
                                                                        "cf", bytes("count"), 8),
                                                                ReadModifyWriteRule.append(
                                                                        "cf",
                                                                        bytes("note"),
                                                                        bytes("body")),
                                                                ReadModifyWriteRule.increment(
                                                                        "cf", bytes("count"), -3))))
                        .build();
        List<Tuple2<String, ReadModifyWriteResult>> results = new ArrayList<>();
        try (CloseableIterator<Tuple2<String, ReadModifyWriteResult>> iterator =
                async.orderedWait(env.fromData("row", "skip"), Duration.ofSeconds(30))
                        .executeAndCollect()) {
            iterator.forEachRemaining(results::add);
        }
        assertThat(results).hasSize(1);
        ReadModifyWriteResult result = results.get(0).f1;
        assertThat(results.get(0).f0).isEqualTo("row");
        assertThat(result.getDestination()).isEqualTo(table);
        assertThat(result.getRow().getKey()).isEqualTo(bytes("row"));
        assertThat(result.getRow().getCells()).hasSize(2);
        result.getRow()
                .getCells()
                .forEach(
                        cell -> {
                            if (cell.getQualifier().equals(bytes("note"))) {
                                assertThat(cell.getValue()).isEqualTo(bytes("somethingbody"));
                            } else {
                                assertThat(cell.getQualifier()).isEqualTo(bytes("count"));
                                assertThat(ByteBuffer.wrap(cell.getValue().toByteArray()).getLong())
                                        .isEqualTo(5);
                            }
                        });
        assertThat(readRows(table).get(0).getCells("cf", "untouched").get(0).getValue())
                .isEqualTo(bytes("keep"));
    }

    @Test
    void sinkDiscardsResponsesAndPreservesBinaryAppendOperands() throws Exception {
        TableDestination table = createTable("rmw-job-binary");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ByteString binary = ByteString.copyFrom(new byte[] {0, -1, 10});
        env.fromData("row")
                .sinkTo(
                        BigtableReadModifyWriteSink.<String>builder()
                                .table(table)
                                .emulatorEndpoint(
                                        EmulatorEndpoint.parse(
                                                emulatorEndpoint(), "emulatorEndpoint"))
                                .serializer(
                                        (key, context) ->
                                                ReadModifyWriteRequest.of(
                                                        bytes(key),
                                                        List.of(
                                                                ReadModifyWriteRule.append(
                                                                        "cf", binary, binary),
                                                                ReadModifyWriteRule.append(
                                                                        "cf", binary, binary))))
                                .build());
        env.execute();
        assertThat(readRows(table).get(0).getCells().get(0).getQualifier()).isEqualTo(binary);
        assertThat(readRows(table).get(0).getCells().get(0).getValue())
                .isEqualTo(binary.concat(binary));
    }

    private static ByteString bytes(String value) {
        return ByteString.copyFromUtf8(value);
    }
}
