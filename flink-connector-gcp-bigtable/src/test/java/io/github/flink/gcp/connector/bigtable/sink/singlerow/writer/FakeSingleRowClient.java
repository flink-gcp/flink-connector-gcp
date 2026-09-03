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

package io.github.flink.gcp.connector.bigtable.sink.singlerow.writer;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link SingleRowClient} recording the client objects it is handed and answering each call from
 * a {@link FakeAnswerFuture} the test completes, which also records whether a cancel reached it the
 * way the client's own answer would reach the wire.
 *
 * <p>One per instance of a {@link FakeSingleRowClientFactory}, as the production factory keeps one
 * data client per instance; the runtime tests assert sharing by identity, which is all a client
 * reports about itself. The request-type tests use it directly, to see the {@code TableId} and the
 * rules a {@link RowRequest} built.
 */
final class FakeSingleRowClient implements SingleRowClient {

    final String instanceKey;
    final List<ConditionalRowMutation> conditionalMutations = new ArrayList<>();
    final List<FakeAnswerFuture<Boolean>> conditionalFutures = new ArrayList<>();
    final List<ReadModifyWriteRow> readModifyWrites = new ArrayList<>();
    final List<FakeAnswerFuture<Row>> readModifyWriteFutures = new ArrayList<>();

    /** Thrown by either call instead of returning a future — the client refusing work. */
    @Nullable RuntimeException callFailure;

    FakeSingleRowClient(String instanceKey) {
        this.instanceKey = instanceKey;
    }

    @Override
    public ApiFuture<Boolean> checkAndMutateRow(ConditionalRowMutation mutation) {
        if (callFailure != null) {
            throw callFailure;
        }
        conditionalMutations.add(mutation);
        FakeAnswerFuture<Boolean> future = new FakeAnswerFuture<>();
        conditionalFutures.add(future);
        return future;
    }

    @Override
    public ApiFuture<Row> readModifyWriteRow(ReadModifyWriteRow mutation) {
        if (callFailure != null) {
            throw callFailure;
        }
        readModifyWrites.add(mutation);
        FakeAnswerFuture<Row> future = new FakeAnswerFuture<>();
        readModifyWriteFutures.add(future);
        return future;
    }

    @Override
    public String toString() {
        return "FakeSingleRowClient{" + instanceKey + "}";
    }
}
