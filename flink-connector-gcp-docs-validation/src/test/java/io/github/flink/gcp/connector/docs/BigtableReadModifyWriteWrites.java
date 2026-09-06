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

package io.github.flink.gcp.connector.docs;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.readmodifywrite.BigtableReadModifyWriteAsync;
import io.github.flink.gcp.connector.bigtable.sink.readmodifywrite.BigtableReadModifyWriteSink;
import io.github.flink.gcp.connector.bigtable.sink.readmodifywrite.ReadModifyWriteRequest;
import io.github.flink.gcp.connector.bigtable.sink.readmodifywrite.ReadModifyWriteResult;
import io.github.flink.gcp.connector.bigtable.sink.readmodifywrite.ReadModifyWriteRule;
import io.github.flink.gcp.connector.bigtable.sink.readmodifywrite.ReadModifyWriteSerializationSchema;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;

final class BigtableReadModifyWriteWrites {
    private BigtableReadModifyWriteWrites() {}

    // tag::rmw-input[]
    public static class AccountChange {
        public String rowKey;
        public String note;
        public long delta;
    }

    // end::rmw-input[]

    static ReadModifyWriteSerializationSchema<AccountChange> schema() {
        // tag::rmw-schema[]
        ReadModifyWriteSerializationSchema<AccountChange> schema =
                (change, context) ->
                        ReadModifyWriteRequest.of(
                                ByteString.copyFromUtf8(change.rowKey),
                                List.of(
                                        ReadModifyWriteRule.append(
                                                "activity",
                                                ByteString.copyFromUtf8("notes"),
                                                ByteString.copyFromUtf8(change.note)),
                                        ReadModifyWriteRule.increment(
                                                "counters",
                                                ByteString.copyFromUtf8("balance"),
                                                change.delta)));
        // end::rmw-schema[]
        return schema;
    }

    static void sink(
            DataStream<AccountChange> changes,
            ReadModifyWriteSerializationSchema<AccountChange> schema) {
        // tag::rmw-sink[]
        BigtableReadModifyWriteSink<AccountChange> sink =
                BigtableReadModifyWriteSink.<AccountChange>builder()
                        .table(TableDestination.of("my-project", "my-instance", "accounts"))
                        .appProfileId("single-cluster")
                        .serializer(schema)
                        .build();
        changes.sinkTo(sink);
        // end::rmw-sink[]
    }

    static DataStream<Tuple2<AccountChange, ReadModifyWriteResult>> async(
            DataStream<AccountChange> changes,
            ReadModifyWriteSerializationSchema<AccountChange> schema) {
        // tag::rmw-async[]
        BigtableReadModifyWriteAsync<AccountChange> requests =
                BigtableReadModifyWriteAsync.<AccountChange>builder()
                        .table(TableDestination.of("my-project", "my-instance", "accounts"))
                        .appProfileId("single-cluster")
                        .serializer(schema)
                        .requestOptions(
                                BigtableRequestOptions.builder()
                                        .requestTimeout(Duration.ofSeconds(10))
                                        .maxInFlightRequests(64)
                                        .build())
                        .build();
        DataStream<Tuple2<AccountChange, ReadModifyWriteResult>> results =
                requests.unorderedWait(changes, Duration.ofSeconds(15));
        // end::rmw-async[]
        return results;
    }

    static DataStream<Long> balances(
            DataStream<Tuple2<AccountChange, ReadModifyWriteResult>> results) {
        // tag::rmw-returned-integer[]
        DataStream<Long> balances =
                results.map(
                        pair -> {
                            ByteString value =
                                    pair.f1.getRow().getCells().stream()
                                            .filter(
                                                    cell ->
                                                            cell.getFamily().equals("counters")
                                                                    && cell.getQualifier()
                                                                            .equals(
                                                                                    ByteString
                                                                                            .copyFromUtf8(
                                                                                                    "balance")))
                                            .findFirst()
                                            .orElseThrow()
                                            .getValue();
                            if (value.size() != Long.BYTES) {
                                throw new IllegalArgumentException(
                                        "The returned balance must contain exactly eight bytes");
                            }
                            return ByteBuffer.wrap(value.toByteArray()).getLong();
                        });
        // end::rmw-returned-integer[]
        return balances;
    }
}
