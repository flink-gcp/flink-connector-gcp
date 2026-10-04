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

package io.github.flink.gcp.connector.firestore.table.source;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.firestore.DocumentSnapshot;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * A scripted {@link FirestoreDocumentLookup}: each read takes the next outcome, and records the id
 * it was asked for. An outcome is a snapshot, a failure the future fails with, a {@link Thrown}
 * failure {@code readAsync} throws itself, or a future the test completes later.
 */
final class FakeDocumentLookup implements FirestoreDocumentLookup {
    private static final long serialVersionUID = 1L;

    private final transient Deque<Object> outcomes = new ArrayDeque<>();
    private final transient List<String> reads = new ArrayList<>();
    private transient boolean opened;
    private transient boolean closed;

    /** A failure {@code readAsync} throws instead of returning a future. */
    static final class Thrown {
        final RuntimeException failure;

        Thrown(RuntimeException failure) {
            this.failure = failure;
        }
    }

    /** Scripts the next read's outcome. */
    FakeDocumentLookup then(Object outcome) {
        outcomes.add(outcome);
        return this;
    }

    /** A failure in the shape the client library throws. */
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

    List<String> reads() {
        return reads;
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
    public ApiFuture<DocumentSnapshot> readAsync(String id) {
        reads.add(id);
        Object outcome = outcomes.remove();
        if (outcome instanceof Thrown) {
            throw ((Thrown) outcome).failure;
        }
        if (outcome instanceof SettableApiFuture) {
            return (SettableApiFuture<DocumentSnapshot>) outcome;
        }
        if (outcome instanceof RuntimeException) {
            return ApiFutures.immediateFailedFuture((RuntimeException) outcome);
        }
        return ApiFutures.immediateFuture((DocumentSnapshot) outcome);
    }

    @Override
    public void close() {
        closed = true;
    }
}
