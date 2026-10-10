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

package io.github.flink.gcp.connector.datastore.sink.writer;

import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Key;
import io.github.flink.gcp.connector.datastore.sink.DatastoreMutation;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link DatastoreDatabaseAccess} that decides each commit <b>per request</b>, as the service
 * does: a commit holding a key the test refused fails whole with that key's status and applies
 * nothing, as the emulator measured. A test sets up which keys are refused and lets the writer's
 * solo confirmation emerge, rather than scripting it.
 *
 * <p>Failures are thrown in the client library's shape: a {@link DatastoreException} whose cause is
 * the gax exception carrying the status.
 */
final class FakeDatastoreDatabaseAccess implements DatastoreDatabaseAccess {

    /** The order the client's {@code Batch} sends a commit's mutations in. */
    private static final List<DatastoreMutation.Operation> BATCH_ORDER =
            List.of(
                    DatastoreMutation.Operation.INSERT,
                    DatastoreMutation.Operation.UPDATE,
                    DatastoreMutation.Operation.UPSERT,
                    DatastoreMutation.Operation.DELETE);

    private final Map<Key, StatusCode.Code> refusedKeys = new HashMap<>();
    private final Deque<Scripted> nextCommitFailures = new ArrayDeque<>();
    private final Deque<StatusCode.Code> nextLookupFailures = new ArrayDeque<>();
    private final List<List<Key>> requests = new ArrayList<>();
    private final Map<Key, DatastoreMutation> applied = new LinkedHashMap<>();
    private final List<DatastoreMutation> appliedInOrder = new ArrayList<>();
    private final List<Key> lookups = new ArrayList<>();
    private final Deque<StatusCode.Code> nextAllocationFailures = new ArrayDeque<>();
    private final List<Integer> allocations = new ArrayList<>();
    private long nextId = 1;
    private boolean answersShort;
    private int closeCalls;
    private boolean appliesWritesBeforeARefusal;

    /** Refuses every commit that holds the key, with the status, until told otherwise. */
    FakeDatastoreDatabaseAccess refuse(Key key, StatusCode.Code code) {
        refusedKeys.put(key, code);
        return this;
    }

    /**
     * Makes a refused commit apply the writes ahead of the refused key in the request first, as the
     * emulator measured for an entity over 1 MiB, instead of applying nothing. The request is in
     * the client's {@code Batch} order, grouped by operation: inserts, updates, upserts, deletes.
     */
    FakeDatastoreDatabaseAccess applyWritesBeforeARefusal() {
        appliesWritesBeforeARefusal = true;
        return this;
    }

    /** Fails the next commits, one per code, before deciding any by its keys. */
    FakeDatastoreDatabaseAccess failNextCommits(StatusCode.Code... codes) {
        for (StatusCode.Code code : codes) {
            nextCommitFailures.add(new Scripted(failure(code), false));
        }
        return this;
    }

    /**
     * Applies the next commit and then fails it with the status, as a commit whose answer never
     * arrived — a timeout after the service applied it — looks to the caller.
     */
    FakeDatastoreDatabaseAccess applyThenFailNextCommit(StatusCode.Code code) {
        nextCommitFailures.add(new Scripted(failure(code), true));
        return this;
    }

    /** Fails the next commit with the given exception, applying nothing. */
    FakeDatastoreDatabaseAccess failNextCommitWith(RuntimeException failure) {
        nextCommitFailures.add(new Scripted(failure, false));
        return this;
    }

    /** Fails the next lookups, one per code. */
    FakeDatastoreDatabaseAccess failNextLookups(StatusCode.Code... codes) {
        for (StatusCode.Code code : codes) {
            nextLookupFailures.add(code);
        }
        return this;
    }

    @Override
    public void commit(List<DatastoreMutation> writes) {
        List<Key> keys = new ArrayList<>();
        for (DatastoreMutation write : writes) {
            keys.add(write.getKey());
        }
        requests.add(keys);
        Scripted scripted = nextCommitFailures.poll();
        if (scripted != null) {
            if (scripted.applyFirst) {
                apply(writes);
            }
            throw scripted.failure;
        }
        List<DatastoreMutation> request = new ArrayList<>(writes);
        request.sort(Comparator.comparingInt(write -> BATCH_ORDER.indexOf(write.getOperation())));
        for (int i = 0; i < request.size(); i++) {
            StatusCode.Code refused = refusedKeys.get(request.get(i).getKey());
            if (refused != null) {
                if (appliesWritesBeforeARefusal) {
                    apply(request.subList(0, i));
                }
                throw failure(refused);
            }
        }
        apply(writes);
    }

    private void apply(List<DatastoreMutation> writes) {
        for (DatastoreMutation write : writes) {
            applied.put(write.getKey(), write);
            appliedInOrder.add(write);
        }
    }

    @Override
    public void lookup(Key key) {
        lookups.add(key);
        StatusCode.Code scripted = nextLookupFailures.poll();
        if (scripted != null) {
            throw failure(scripted);
        }
    }

    /** Fails the next id allocations, one per code. */
    FakeDatastoreDatabaseAccess failNextAllocations(StatusCode.Code... codes) {
        for (StatusCode.Code code : codes) {
            nextAllocationFailures.add(code);
        }
        return this;
    }

    /** Answers the next allocations with one key fewer than asked for. */
    FakeDatastoreDatabaseAccess answerAllocationsShort() {
        answersShort = true;
        return this;
    }

    /** Allocates ids counting up from 1, never handing one out twice, as the service promises. */
    @Override
    public List<Key> allocateIds(List<IncompleteKey> keys) {
        allocations.add(keys.size());
        StatusCode.Code scripted = nextAllocationFailures.poll();
        if (scripted != null) {
            throw failure(scripted);
        }
        List<Key> allocated = new ArrayList<>(keys.size());
        for (IncompleteKey key : keys) {
            allocated.add(Key.newBuilder(key, nextId++).build());
        }
        return answersShort ? allocated.subList(0, allocated.size() - 1) : allocated;
    }

    /** Every allocation call, as the number of keys it asked for. */
    List<Integer> allocations() {
        return allocations;
    }

    @Override
    public void close() {
        closeCalls++;
    }

    /** Every commit request, as the keys it carried. */
    List<List<Key>> requests() {
        return requests;
    }

    /** The last write applied to each key. */
    Map<Key, DatastoreMutation> applied() {
        return applied;
    }

    /** Every write applied, in order, a re-applied one again. */
    List<DatastoreMutation> appliedInOrder() {
        return appliedInOrder;
    }

    List<Key> lookups() {
        return lookups;
    }

    int closeCalls() {
        return closeCalls;
    }

    /** A failure in the shape the client library throws. */
    static DatastoreException failure(StatusCode.Code code) {
        StatusCode status =
                new StatusCode() {
                    @Override
                    public Code getCode() {
                        return code;
                    }

                    @Override
                    public Object getTransportCode() {
                        return code;
                    }
                };
        return new DatastoreException(
                0,
                code + ": refused by the fake",
                code.name(),
                ApiExceptionFactory.createException(
                        new RuntimeException(code + ": refused by the fake"), status, false));
    }

    /** A scripted commit failure, and whether the commit is applied before it is thrown. */
    private static final class Scripted {

        private final RuntimeException failure;
        private final boolean applyFirst;

        private Scripted(RuntimeException failure, boolean applyFirst) {
            this.failure = failure;
            this.applyFirst = applyFirst;
        }
    }
}
