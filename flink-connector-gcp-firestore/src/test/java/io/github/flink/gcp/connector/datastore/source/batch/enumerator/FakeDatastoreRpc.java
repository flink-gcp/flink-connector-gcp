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

package io.github.flink.gcp.connector.datastore.source.batch.enumerator;

import com.google.cloud.datastore.spi.v1.DatastoreRpc;
import com.google.datastore.v1.AllocateIdsRequest;
import com.google.datastore.v1.AllocateIdsResponse;
import com.google.datastore.v1.BeginTransactionRequest;
import com.google.datastore.v1.BeginTransactionResponse;
import com.google.datastore.v1.CommitRequest;
import com.google.datastore.v1.CommitResponse;
import com.google.datastore.v1.LookupRequest;
import com.google.datastore.v1.LookupResponse;
import com.google.datastore.v1.ReserveIdsRequest;
import com.google.datastore.v1.ReserveIdsResponse;
import com.google.datastore.v1.RollbackRequest;
import com.google.datastore.v1.RollbackResponse;
import com.google.datastore.v1.RunAggregationQueryRequest;
import com.google.datastore.v1.RunAggregationQueryResponse;
import com.google.datastore.v1.RunQueryRequest;
import com.google.datastore.v1.RunQueryResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * A {@link DatastoreRpc} that answers {@code RunQuery} as the test scripts it and records every
 * request; every other call is unsupported.
 */
final class FakeDatastoreRpc implements DatastoreRpc {

    private final Function<RunQueryRequest, RunQueryResponse> answer;
    private final List<RunQueryRequest> requests = new ArrayList<>();

    FakeDatastoreRpc(Function<RunQueryRequest, RunQueryResponse> answer) {
        this.answer = answer;
    }

    List<RunQueryRequest> requests() {
        return requests;
    }

    @Override
    public RunQueryResponse runQuery(RunQueryRequest request) {
        requests.add(request);
        return answer.apply(request);
    }

    @Override
    public AllocateIdsResponse allocateIds(AllocateIdsRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BeginTransactionResponse beginTransaction(BeginTransactionRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CommitResponse commit(CommitRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LookupResponse lookup(LookupRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ReserveIdsResponse reserveIds(ReserveIdsRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RollbackResponse rollback(RollbackRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RunAggregationQueryResponse runAggregationQuery(RunAggregationQueryRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {}

    @Override
    public boolean isClosed() {
        return false;
    }
}
