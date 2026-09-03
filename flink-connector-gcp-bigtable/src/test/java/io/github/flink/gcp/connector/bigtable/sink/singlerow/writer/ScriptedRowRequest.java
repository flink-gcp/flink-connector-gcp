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
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.api.gax.rpc.StatusCode;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.RowOperation;
import io.grpc.Status;

import javax.annotation.Nullable;

/**
 * A {@link RowRequest} whose answer a test decides: {@link #start} hands out one settable future,
 * and {@link #succeed}, {@link #fail(StatusCode.Code)} and {@link #fail(Throwable)} complete it.
 *
 * <p>The request records what it was started on, so a test can assert the runtime named the table
 * the resolver chose and the client of that table's instance — the two things {@link
 * RowRequest#start} exists to be told.
 */
final class ScriptedRowRequest implements RowRequest<String> {

    final RowOperation operation;
    final ByteString rowKey;
    final SettableApiFuture<String> future = SettableApiFuture.create();

    /**
     * Thrown by {@link #start} instead of returning the future, for the synchronous-refusal path.
     */
    @Nullable RuntimeException startFailure;

    int starts;
    @Nullable SingleRowClient startedOn;
    @Nullable TableDestination startedFor;

    ScriptedRowRequest(String rowKey) {
        this(RowOperation.CHECK_AND_MUTATE_ROW, rowKey);
    }

    ScriptedRowRequest(RowOperation operation, String rowKey) {
        this.operation = operation;
        this.rowKey = ByteString.copyFromUtf8(rowKey);
    }

    @Override
    public RowOperation operation() {
        return operation;
    }

    @Override
    public ByteString rowKey() {
        return rowKey;
    }

    @Override
    public ApiFuture<String> start(SingleRowClient client, TableDestination destination) {
        starts++;
        if (startFailure != null) {
            throw startFailure;
        }
        startedOn = client;
        startedFor = destination;
        return future;
    }

    void succeed() {
        succeed("answer:" + rowKey.toStringUtf8());
    }

    void succeed(String answer) {
        future.set(answer);
    }

    void fail(StatusCode.Code code) {
        future.setException(apiException(code, code.name()));
    }

    void fail(Throwable failure) {
        future.setException(failure);
    }

    /** Builds the exception the client reports a status under. */
    static Exception apiException(StatusCode.Code code, String description) {
        Status status =
                Status.fromCode(Status.Code.valueOf(code.name())).withDescription(description);
        return ApiExceptionFactory.createException(
                status.asRuntimeException(), GrpcStatusCode.of(status.getCode()), false);
    }
}
