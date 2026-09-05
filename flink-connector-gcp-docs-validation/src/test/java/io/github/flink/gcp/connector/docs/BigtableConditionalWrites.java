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
import io.github.flink.gcp.connector.bigtable.sink.conditional.BigtableConditionalAsync;
import io.github.flink.gcp.connector.bigtable.sink.conditional.BigtableConditionalSink;
import io.github.flink.gcp.connector.bigtable.sink.conditional.ConditionalFilter;
import io.github.flink.gcp.connector.bigtable.sink.conditional.ConditionalMutation;
import io.github.flink.gcp.connector.bigtable.sink.conditional.ConditionalRequest;
import io.github.flink.gcp.connector.bigtable.sink.conditional.ConditionalResult;
import io.github.flink.gcp.connector.bigtable.sink.conditional.ConditionalSerializationSchema;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;

import java.time.Duration;
import java.util.List;

final class BigtableConditionalWrites {
    private BigtableConditionalWrites() {}

    // tag::conditional-input[]
    public static class ProfileChange {
        public String rowKey;
        public String expectedName;
        public String newName;
        public long timestampMicros;
    }

    // end::conditional-input[]

    static ConditionalSerializationSchema<ProfileChange> schema() {
        // tag::conditional-schema[]
        ConditionalSerializationSchema<ProfileChange> schema =
                (change, context) ->
                        ConditionalRequest.of(
                                ByteString.copyFromUtf8(change.rowKey),
                                ConditionalFilter.latestCellValueEquals(
                                        "profile",
                                        ByteString.copyFromUtf8("name"),
                                        ByteString.copyFromUtf8(change.expectedName)),
                                List.of(
                                        ConditionalMutation.setCell(
                                                "profile",
                                                ByteString.copyFromUtf8("name"),
                                                change.timestampMicros,
                                                ByteString.copyFromUtf8(change.newName))),
                                List.of());
        // end::conditional-schema[]
        return schema;
    }

    static void sink(
            DataStream<ProfileChange> changes,
            ConditionalSerializationSchema<ProfileChange> schema) {
        // tag::conditional-sink[]
        BigtableConditionalSink<ProfileChange> sink =
                BigtableConditionalSink.<ProfileChange>builder()
                        .table(TableDestination.of("my-project", "my-instance", "users"))
                        .appProfileId("single-cluster")
                        .serializer(schema)
                        .build();
        changes.sinkTo(sink);
        // end::conditional-sink[]
    }

    static DataStream<Tuple2<ProfileChange, ConditionalResult>> async(
            DataStream<ProfileChange> changes,
            ConditionalSerializationSchema<ProfileChange> schema) {
        // tag::conditional-async[]
        BigtableConditionalAsync<ProfileChange> conditional =
                BigtableConditionalAsync.<ProfileChange>builder()
                        .table(TableDestination.of("my-project", "my-instance", "users"))
                        .appProfileId("single-cluster")
                        .serializer(schema)
                        .requestOptions(
                                BigtableRequestOptions.builder()
                                        .requestTimeout(Duration.ofSeconds(10))
                                        .maxInFlightRequests(64)
                                        .build())
                        .build();
        DataStream<Tuple2<ProfileChange, ConditionalResult>> results =
                conditional.unorderedWait(changes, Duration.ofSeconds(15));
        // end::conditional-async[]
        return results;
    }
}
