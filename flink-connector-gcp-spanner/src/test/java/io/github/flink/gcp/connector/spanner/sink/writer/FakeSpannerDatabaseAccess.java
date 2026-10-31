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

package io.github.flink.gcp.connector.spanner.sink.writer;

import org.apache.flink.util.Preconditions;

import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.MutationGroup;
import com.google.rpc.Status;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A scripted {@link SpannerDatabaseAccess}.
 *
 * <p>Each {@link Response} says what one batch write does: which groups the service reported on,
 * with what status, and whether the request then failed. Leaving a group unreported is the case
 * that matters most — it is what a server stream failing part-way through looks like, and the
 * writer has to treat those mutations as undecided rather than applied.
 *
 * <p>Requests are recorded as the flat list of mutations they carried, so a test can assert that a
 * retry re-sent exactly the undecided ones.
 */
final class FakeSpannerDatabaseAccess implements SpannerDatabaseAccess {

    private final Deque<Response> responses = new ArrayDeque<>();
    private final List<List<Mutation>> requests = new ArrayList<>();

    private CellWeights cellWeights = CellWeights.empty();
    private PrimaryKeys primaryKeys = PrimaryKeys.empty();
    private final Deque<Object> primaryKeyOutcomes = new ArrayDeque<>();
    private final List<String> primaryKeyReads = new ArrayList<>();
    private final List<String> cellWeightReads = new ArrayList<>();
    private int closeCalls;

    /** Scripts the next requests, in order. Anything beyond the script is applied in full. */
    FakeSpannerDatabaseAccess script(Response... scripted) {
        for (Response response : scripted) {
            responses.add(response);
        }
        return this;
    }

    FakeSpannerDatabaseAccess withCellWeights(CellWeights cellWeights) {
        this.cellWeights = cellWeights;
        return this;
    }

    FakeSpannerDatabaseAccess withPrimaryKeys(PrimaryKeys primaryKeys) {
        this.primaryKeys = primaryKeys;
        return this;
    }

    /**
     * Scripts the next reads of the primary keys, in order: a {@link PrimaryKeys} is returned and
     * kept for the reads after it, an {@link IOException} is thrown. Unscripted reads return the
     * keys last returned or set.
     */
    FakeSpannerDatabaseAccess thenPrimaryKeyReads(Object... outcomes) {
        for (Object outcome : outcomes) {
            Preconditions.checkArgument(
                    outcome instanceof PrimaryKeys || outcome instanceof IOException,
                    "outcome must be PrimaryKeys or IOException");
            primaryKeyOutcomes.add(outcome);
        }
        return this;
    }

    /**
     * The table every read of the primary keys was narrowed to, {@code null} for none, in order.
     */
    List<String> primaryKeyReads() {
        return primaryKeyReads;
    }

    /**
     * The table every read of the cell weights was narrowed to, {@code null} for none, in order.
     */
    List<String> cellWeightReads() {
        return cellWeightReads;
    }

    /** The mutations of every request, in the order they were sent. */
    List<List<Mutation>> requests() {
        return requests;
    }

    int closeCalls() {
        return closeCalls;
    }

    @Override
    public CellWeights readCellWeights(@Nullable String table) {
        cellWeightReads.add(table);
        return cellWeights;
    }

    @Override
    public PrimaryKeys readPrimaryKeys(@Nullable String table) throws IOException {
        primaryKeyReads.add(table);
        Object next = primaryKeyOutcomes.poll();
        if (next instanceof IOException) {
            throw (IOException) next;
        }
        if (next != null) {
            primaryKeys = (PrimaryKeys) next;
        }
        return primaryKeys;
    }

    @Override
    public void batchWrite(List<MutationGroup> groups, GroupOutcomes outcomes) {
        List<Mutation> sent = new ArrayList<>();
        for (MutationGroup group : groups) {
            sent.addAll(group.getMutations());
        }
        requests.add(sent);
        Response response = responses.isEmpty() ? Response.allApplied() : responses.poll();
        response.apply(groups.size(), outcomes);
    }

    @Override
    public void close() {
        closeCalls++;
    }

    /** What one scripted batch write does. */
    static final class Response {

        private final Map<Integer, Status> statuses = new HashMap<>();
        private boolean applyAll;
        @Nullable private RuntimeException failure;

        private Response() {}

        /**
         * The service applied every group it was not told otherwise about, so a {@link
         * #refused(int, StatusCode.Code, String)} on top of this is one bad mutation in a good
         * batch.
         */
        static Response allApplied() {
            Response response = new Response();
            response.applyAll = true;
            return response;
        }

        /** The service reported nothing at all. */
        static Response nothingReported() {
            return new Response();
        }

        /** The service applied this one group and said nothing about the others. */
        Response applied(int groupIndex) {
            statuses.put(groupIndex, Status.newBuilder().setCode(0).build());
            return this;
        }

        /** The service refused this one group and said nothing about the others. */
        Response refused(int groupIndex, StatusCode.Code code, String message) {
            statuses.put(
                    groupIndex,
                    Status.newBuilder().setCode(canonicalCodeOf(code)).setMessage(message).build());
            return this;
        }

        /** The request itself then failed, after whatever it had already reported. */
        Response andThenFailing(RuntimeException failure) {
            this.failure = failure;
            return this;
        }

        private void apply(int groupCount, GroupOutcomes outcomes) {
            if (applyAll) {
                Status ok = Status.newBuilder().setCode(0).build();
                for (int i = 0; i < groupCount; i++) {
                    outcomes.report(i, statuses.getOrDefault(i, ok));
                }
            } else {
                statuses.forEach(outcomes::report);
            }
            if (failure != null) {
                throw failure;
            }
        }

        private static int canonicalCodeOf(StatusCode.Code code) {
            return io.grpc.Status.Code.valueOf(code.name()).value();
        }
    }
}
