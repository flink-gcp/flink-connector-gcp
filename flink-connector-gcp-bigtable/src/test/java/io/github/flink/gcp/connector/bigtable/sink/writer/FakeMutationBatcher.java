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

package io.github.flink.gcp.connector.bigtable.sink.writer;

import org.apache.flink.util.ExceptionUtils;

import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.grpc.Status;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

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

    /** The table this batcher is bound to, as the real one is. */
    final TableDestination destination;

    /**
     * Teardown events of every batcher of one {@link FakeMutationBatcherFactory}, in call order, so
     * a test can assert the writer starts every shutdown before it waits on any close.
     */
    final List<String> events;

    final List<RowMutationEntry> entries = new ArrayList<>();
    final List<SettableApiFuture<Void>> futures = new ArrayList<>();

    /** The entry indices of each request actually sent, in send order. */
    final List<List<Integer>> sentBatches = new CopyOnWriteArrayList<>();

    /** Row keys the service refuses; a request carrying one is rejected whole. */
    final Set<String> rejectedRowKeys = new HashSet<>();

    /**
     * While set, every sent request fails all its entries with {@code NOT_FOUND} — the
     * request-level fan-out a missing table produces (one RPC-level status reported against each
     * entry of the batch). Cleared by the test, typically from a {@code FakeTableAdmin#onEnsure}
     * hook, so repair convergence emerges from the ensure rather than being scripted turn by turn.
     */
    boolean tableMissing;

    /**
     * Sends up to this count fail with the real service's missing-family description. Zero (the
     * default) disables it; {@link Integer#MAX_VALUE} keeps the family missing indefinitely.
     */
    int columnFamilyMissingThroughSends;

    /**
     * Requests after this many sends fail as {@link #tableMissing} does — for the case where the
     * table vanishes <em>mid-flush</em>, which no test code can inject between two sends of one
     * writer call. {@link Integer#MAX_VALUE} (the default) never triggers.
     */
    int tableMissingAfterSends = Integer.MAX_VALUE;

    /**
     * {@code volatile} because {@code BigtableWriterStallTest} polls it from its scheduler thread
     * while the task thread under test increments it, and nothing else orders those two.
     */
    volatile int sendOutstandingCalls;

    int shutdownCalls;
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

    /** A standalone batcher, for a test that drives one table and reads no teardown order. */
    FakeMutationBatcher(TableDestination destination) {
        this(destination, new ArrayList<>());
    }

    FakeMutationBatcher(TableDestination destination, List<String> events) {
        this.destination = destination;
        this.events = events;
    }

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
        boolean missing = tableMissing || sentBatches.size() > tableMissingAfterSends;
        boolean familyMissing = sentBatches.size() <= columnFamilyMissingThroughSends;
        for (int index : batch) {
            if (familyMissing) {
                futures.get(index)
                        .setException(
                                apiException(
                                        StatusCode.Code.NOT_FOUND,
                                        missingFamilyDescription(index)));
            } else if (missing) {
                futures.get(index)
                        .setException(
                                apiException(
                                        StatusCode.Code.NOT_FOUND,
                                        new RuntimeException(
                                                "scripted missing table for " + rowKey(index))));
            } else if (rejected) {
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
    public void shutdown() {
        shutdownCalls++;
        events.add("shutdown " + destination.getTable());
    }

    @Override
    public void close() throws Exception {
        closeCalls++;
        events.add("close " + destination.getTable());
        if (closeFailure != null) {
            if (closeFailure instanceof Exception) {
                throw (Exception) closeFailure;
            }
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
     * Answers everything still outstanding, so a wait nobody else will answer can end.
     *
     * <p>Only what has already been sent: an entry still accumulating, or one sent after this
     * returns, keeps its wait parked.
     */
    void answerEverythingOutstanding() {
        for (List<Integer> batch : new ArrayList<>(sentBatches)) {
            for (int index : batch) {
                succeed(index);
            }
        }
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

    /**
     * The description the service sends for a mutation naming a family the table lacks: the phrase
     * the classifier looks for, at the <em>end</em> of a sentence naming the row and the table
     * resource. Scripting the bare phrase is what let #948 through — the classifier compared the
     * whole description for equality, which no service response satisfies, and no fake or emulator
     * contradicted it (the emulator answers {@code INTERNAL} to an unknown family entirely).
     */
    private String missingFamilyDescription(int index) {
        return "Error while mutating the row '"
                + rowKey(index)
                + "' (projects/"
                + destination.getProject()
                + "/instances/"
                + destination.getInstance()
                + "/tables/"
                + destination.getTable()
                + ") : "
                + BigtableErrorClassifier.MISSING_COLUMN_FAMILY_PHRASE
                + ".";
    }

    static Exception apiException(StatusCode.Code code) {
        return apiException(code, new RuntimeException("scripted " + code));
    }

    /** Builds an exception whose status carries the given description. */
    static Exception apiException(StatusCode.Code code, String description) {
        Status status =
                Status.fromCode(Status.Code.valueOf(code.name())).withDescription(description);
        return ApiExceptionFactory.createException(
                status.asRuntimeException(), GrpcStatusCode.of(status.getCode()), false);
    }

    /** Builds an exception carrying {@code code} over the given cause, for cause-chain tests. */
    static Exception apiException(StatusCode.Code code, Throwable cause) {
        return ApiExceptionFactory.createException(
                cause, GrpcStatusCode.of(Status.Code.valueOf(code.name())), false);
    }
}
