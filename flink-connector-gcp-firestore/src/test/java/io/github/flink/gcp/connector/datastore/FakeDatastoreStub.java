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

package io.github.flink.gcp.connector.datastore;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.datastore.v1.DatastoreClient;
import com.google.cloud.datastore.v1.stub.DatastoreStub;
import com.google.datastore.v1.AllocateIdsRequest;
import com.google.datastore.v1.AllocateIdsResponse;
import com.google.datastore.v1.CommitRequest;
import com.google.datastore.v1.CommitResponse;
import com.google.datastore.v1.LookupRequest;
import com.google.datastore.v1.LookupResponse;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * A generated Datastore client's stub that answers each call as the test scripts it and records
 * every request; a call the test did not script fails. It runs no channel.
 */
public final class FakeDatastoreStub extends DatastoreStub {

    /** Every request the stub received, in order. */
    public final List<Object> requests = new ArrayList<>();

    private Function<CommitRequest, CommitResponse> commit = unscripted("Commit");
    private Function<LookupRequest, LookupResponse> lookup = unscripted("Lookup");
    private Function<AllocateIdsRequest, AllocateIdsResponse> allocateIds =
            unscripted("AllocateIds");
    private volatile boolean shutdown;

    /** Answers {@code Commit}. */
    public FakeDatastoreStub onCommit(Function<CommitRequest, CommitResponse> answer) {
        this.commit = answer;
        return this;
    }

    /** Answers {@code Lookup}. */
    public FakeDatastoreStub onLookup(Function<LookupRequest, LookupResponse> answer) {
        this.lookup = answer;
        return this;
    }

    /** Answers {@code AllocateIds}. */
    public FakeDatastoreStub onAllocateIds(
            Function<AllocateIdsRequest, AllocateIdsResponse> answer) {
        this.allocateIds = answer;
        return this;
    }

    /** Opens a generated client on this stub, under a lease, as production code holds one. */
    public GrpcEventLoops.LeasedClient<DatastoreClient> open() throws IOException {
        return GrpcEventLoops.open(lease -> DatastoreClient.create(this));
    }

    @Override
    public UnaryCallable<CommitRequest, CommitResponse> commitCallable() {
        return immediate(commit);
    }

    @Override
    public UnaryCallable<LookupRequest, LookupResponse> lookupCallable() {
        return immediate(lookup);
    }

    @Override
    public UnaryCallable<AllocateIdsRequest, AllocateIdsResponse> allocateIdsCallable() {
        return immediate(allocateIds);
    }

    private <Q, R> UnaryCallable<Q, R> immediate(Function<Q, R> answer) {
        return new UnaryCallable<Q, R>() {
            @Override
            public ApiFuture<R> futureCall(Q request, ApiCallContext context) {
                synchronized (requests) {
                    requests.add(request);
                }
                try {
                    return ApiFutures.immediateFuture(answer.apply(request));
                } catch (RuntimeException e) {
                    return ApiFutures.immediateFailedFuture(e);
                }
            }
        };
    }

    private static <Q, R> Function<Q, R> unscripted(String method) {
        return request -> {
            throw new UnsupportedOperationException(method);
        };
    }

    @Override
    public void close() {
        shutdown = true;
    }

    @Override
    public void shutdown() {
        shutdown = true;
    }

    @Override
    public boolean isShutdown() {
        return shutdown;
    }

    @Override
    public boolean isTerminated() {
        return shutdown;
    }

    @Override
    public void shutdownNow() {
        shutdown = true;
    }

    @Override
    public boolean awaitTermination(long duration, TimeUnit unit) {
        return shutdown;
    }
}
