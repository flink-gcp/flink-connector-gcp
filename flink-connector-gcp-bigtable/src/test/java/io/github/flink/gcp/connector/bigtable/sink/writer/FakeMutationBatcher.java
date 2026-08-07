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

package io.github.flink.gcp.connector.bigtable.sink.writer;

import org.apache.flink.util.ExceptionUtils;

import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.grpc.Status;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * An in-memory {@link MutationBatcher} recording the mutations it receives and the requests it
 * sends.
 *
 * <p>It models the one property the writer's isolation pass rests on: mutations <b>accumulate</b>
 * until {@link #sendOutstanding()}, which sends what has accumulated as one request. {@link
 * #sentBatches} is that history, so a test can assert that a mutation travelled <em>alone</em>
 * rather than only that it was re-submitted.
 *
 * <p>Outcomes are decided per request rather than per entry, which is what real Cloud Bigtable was
 * measured doing (#239): a request carrying a row key in {@link #rejectedRowKeys} fails
 * <b>every</b> entry of that request with {@code INVALID_ARGUMENT}, the good ones included. The
 * isolation pass's behaviour therefore emerges from the fake rather than being scripted step by
 * step — a writer that routed the batched report, or that re-submitted the park as a batch,
 * produces a visibly different outcome here.
 *
 * <p>Nothing completes until its request is sent, which is what lets a test hold the writer at its
 * in-flight caps: those tests never send. {@link #autoComplete} turns the per-request outcome off
 * entirely, for tests that complete individual futures by hand with {@link #succeed} and {@link
 * #fail}.
 */
final class FakeMutationBatcher implements MutationBatcher {

    final List<RowMutationEntry> entries = new ArrayList<>();
    final List<SettableApiFuture<Void>> futures = new ArrayList<>();

    /** The entry indices of each request actually sent, in send order. */
    final List<List<Integer>> sentBatches = new ArrayList<>();

    /** Row keys the service refuses; a request carrying one is rejected whole. */
    final Set<String> rejectedRowKeys = new HashSet<>();

    int sendOutstandingCalls;
    int closeCalls;
    RuntimeException addFailure;

    /** Typed {@code Throwable} so a test can script an {@code Error}, which is thrown as itself. */
    Throwable closeFailure;

    /** Whether a sent request completes its entries; off for tests driving futures by hand. */
    boolean autoComplete = true;

    /**
     * Indices added since the last send: the request the next {@link #sendOutstanding()} issues.
     */
    private final List<Integer> accumulated = new ArrayList<>();

    @Override
    public ApiFuture<Void> add(RowMutationEntry entry) {
        if (addFailure != null) {
            throw addFailure;
        }
        entries.add(entry);
        SettableApiFuture<Void> future = SettableApiFuture.create();
        futures.add(future);
        accumulated.add(futures.size() - 1);
        return future;
    }

    @Override
    public void sendOutstanding() {
        sendOutstandingCalls++;
        if (accumulated.isEmpty()) {
            // The real batcher returns early on an empty open batch, so an empty send issues no
            // request and must not appear in the history the solo assertions read.
            return;
        }
        List<Integer> batch = new ArrayList<>(accumulated);
        accumulated.clear();
        sentBatches.add(batch);
        if (!autoComplete) {
            return;
        }
        boolean rejected =
                batch.stream().anyMatch(index -> rejectedRowKeys.contains(rowKey(index)));
        for (int index : batch) {
            if (rejected) {
                futures.get(index)
                        .setException(
                                apiException(
                                        StatusCode.Code.INVALID_ARGUMENT,
                                        new RuntimeException(
                                                "scripted rejection of the request carrying "
                                                        + rowKey(index))));
            } else {
                futures.get(index).set(null);
            }
        }
    }

    @Override
    public void close() {
        closeCalls++;
        if (closeFailure != null) {
            ExceptionUtils.rethrow(closeFailure);
        }
    }

    /** The row keys of the entries of each request sent, in send order. */
    List<List<String>> sentRowKeys() {
        List<List<String>> keys = new ArrayList<>();
        for (List<Integer> batch : sentBatches) {
            List<String> batchKeys = new ArrayList<>();
            for (int index : batch) {
                batchKeys.add(rowKey(index));
            }
            keys.add(batchKeys);
        }
        return keys;
    }

    /**
     * Completes the outstanding mutation at the given index successfully.
     *
     * <p>The entry leaves the accumulator, because a future cannot be answered before its request
     * went out. The request itself is not added to {@link #sentBatches}: a test completing futures
     * by hand is standing in for the service, and what those tests assert on is the requests the
     * <em>writer</em> issued.
     */
    void succeed(int index) {
        accumulated.remove(Integer.valueOf(index));
        futures.get(index).set(null);
    }

    /** Fails the outstanding mutation at the given index with the given status. */
    void fail(int index, StatusCode.Code code) {
        accumulated.remove(Integer.valueOf(index));
        futures.get(index).setException(apiException(code));
    }

    private String rowKey(int index) {
        return entries.get(index).toProto().getRowKey().toStringUtf8();
    }

    static Exception apiException(StatusCode.Code code) {
        return apiException(code, new RuntimeException("scripted " + code));
    }

    /** Builds an exception carrying {@code code} over the given cause, for cause-chain tests. */
    static Exception apiException(StatusCode.Code code, Throwable cause) {
        return ApiExceptionFactory.createException(
                cause, GrpcStatusCode.of(Status.Code.valueOf(code.name())), false);
    }
}
