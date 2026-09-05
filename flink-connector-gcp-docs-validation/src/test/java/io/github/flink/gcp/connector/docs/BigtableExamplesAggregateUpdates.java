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

import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Value;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSink;

import java.util.List;

final class BigtableExamplesAggregateUpdates {

    private BigtableExamplesAggregateUpdates() {}

    static void addInputs() {
        // tag::bigtable-examples-aggregate-add[]
        BigtableSink.<CounterUpdate>builder()
                .table(TableDestination.of("my-project", "my-instance", "counters"))
                .serializer(
                        (update, context) ->
                                RowMutationEntry.create(update.key())
                                        .addToCell(
                                                "totals",
                                                "count",
                                                update.bucketStartMillis() * 1_000L,
                                                update.delta()))
                .build();
        // end::bigtable-examples-aggregate-add[]
    }

    static void mergeState() {
        // tag::bigtable-examples-aggregate-merge[]
        BigtableSink.<CounterState>builder()
                .table(TableDestination.of("my-project", "my-instance", "counters"))
                .serializer(
                        (state, context) -> {
                            Value qualifier =
                                    Value.newBuilder()
                                            .setRawValue(ByteString.copyFromUtf8("count"))
                                            .build();
                            Value timestamp =
                                    Value.newBuilder()
                                            .setRawTimestampMicros(
                                                    state.bucketStartMillis() * 1_000L)
                                            .build();
                            Value input =
                                    Value.newBuilder().setBytesValue(state.accumulator()).build();
                            Mutation merge =
                                    Mutation.newBuilder()
                                            .setMergeToCell(
                                                    Mutation.MergeToCell.newBuilder()
                                                            .setFamilyName("totals")
                                                            .setColumnQualifier(qualifier)
                                                            .setTimestamp(timestamp)
                                                            .setInput(input))
                                            .build();
                            return RowMutationEntry.createFromMutationUnsafe(
                                    ByteString.copyFromUtf8(state.key()),
                                    com.google.cloud.bigtable.data.v2.models.Mutation
                                            .fromProtoUnsafe(List.of(merge)));
                        })
                .build();
        // end::bigtable-examples-aggregate-merge[]
    }

    static final class CounterUpdate {
        private final String key;
        private final long bucketStartMillis;
        private final long delta;

        CounterUpdate(String key, long bucketStartMillis, long delta) {
            this.key = key;
            this.bucketStartMillis = bucketStartMillis;
            this.delta = delta;
        }

        String key() {
            return key;
        }

        long bucketStartMillis() {
            return bucketStartMillis;
        }

        long delta() {
            return delta;
        }
    }

    static final class CounterState {
        private final String key;
        private final long bucketStartMillis;
        private final ByteString accumulator;

        CounterState(String key, long bucketStartMillis, ByteString accumulator) {
            this.key = key;
            this.bucketStartMillis = bucketStartMillis;
            this.accumulator = accumulator;
        }

        String key() {
            return key;
        }

        long bucketStartMillis() {
            return bucketStartMillis;
        }

        ByteString accumulator() {
            return accumulator;
        }
    }
}
