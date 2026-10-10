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

package io.github.flink.gcp.connector.datastore.table.source;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.api.gax.rpc.StatusCode;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.EntityResult;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.LookupResponse;
import com.google.datastore.v1.Value;
import com.google.protobuf.Timestamp;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * A scripted {@link DatastoreEntityLookup}: each lookup takes the next outcome, and records the key
 * it was asked for. An outcome is an answer, a failure the future fails with, a {@link Thrown}
 * failure {@code lookupAsync} throws itself, or a future the test completes later.
 */
final class FakeEntityLookup implements DatastoreEntityLookup {
    private static final long serialVersionUID = 1L;

    static final Timestamp CREATED = Timestamp.newBuilder().setSeconds(1_700_000_000L).build();
    static final Timestamp UPDATED =
            Timestamp.newBuilder().setSeconds(1_700_000_001L).setNanos(123_456_000).build();
    static final Timestamp READ = Timestamp.newBuilder().setSeconds(1_700_000_002L).build();

    private final transient Deque<Object> outcomes = new ArrayDeque<>();
    private final transient List<Key> lookups = new ArrayList<>();
    private transient boolean opened;
    private transient boolean closed;

    /** A failure {@code lookupAsync} throws instead of returning a future. */
    static final class Thrown {
        final RuntimeException failure;

        Thrown(RuntimeException failure) {
            this.failure = failure;
        }
    }

    /** Scripts the next lookup's outcome. */
    FakeEntityLookup then(Object outcome) {
        outcomes.add(outcome);
        return this;
    }

    /** A failure in the shape the generated client throws. */
    static ApiException failure(StatusCode.Code code) {
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
        return ApiExceptionFactory.createException(
                new RuntimeException(code + ": refused by the fake"), status, false);
    }

    /** An answer that finds the key, its entity holding {@code n}, at version 7. */
    static LookupResponse found(Key key, Value n) {
        return LookupResponse.newBuilder()
                .addFound(
                        EntityResult.newBuilder()
                                .setEntity(Entity.newBuilder().setKey(key).putProperties("n", n))
                                .setVersion(7)
                                .setCreateTime(CREATED)
                                .setUpdateTime(UPDATED))
                .setReadTime(READ)
                .build();
    }

    /** An answer that reports the key missing. */
    static LookupResponse missing(Key key) {
        return LookupResponse.newBuilder()
                .addMissing(EntityResult.newBuilder().setEntity(Entity.newBuilder().setKey(key)))
                .setReadTime(READ)
                .build();
    }

    /** An answer that defers the key. */
    static LookupResponse deferred(Key key) {
        return LookupResponse.newBuilder().addDeferred(key).build();
    }

    List<Key> lookups() {
        return lookups;
    }

    boolean opened() {
        return opened;
    }

    boolean closed() {
        return closed;
    }

    @Override
    public void open() {
        opened = true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ApiFuture<LookupResponse> lookupAsync(Key key) {
        lookups.add(key);
        Object outcome = outcomes.remove();
        if (outcome instanceof Thrown) {
            throw ((Thrown) outcome).failure;
        }
        if (outcome instanceof SettableApiFuture) {
            return (SettableApiFuture<LookupResponse>) outcome;
        }
        if (outcome instanceof RuntimeException) {
            return ApiFutures.immediateFailedFuture((RuntimeException) outcome);
        }
        return ApiFutures.immediateFuture((LookupResponse) outcome);
    }

    @Override
    public void close() {
        closed = true;
    }
}
