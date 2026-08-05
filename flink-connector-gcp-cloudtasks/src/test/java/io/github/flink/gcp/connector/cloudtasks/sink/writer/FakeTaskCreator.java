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

package io.github.flink.gcp.connector.cloudtasks.sink.writer;

import org.apache.flink.util.ExceptionUtils;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.tasks.v2.CreateTaskRequest;
import com.google.cloud.tasks.v2.Task;
import io.grpc.Status;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * An in-memory {@link TaskCreator} recording the requests it receives and returning scripted
 * futures — immediate success unless the test scripted something else for that call.
 */
final class FakeTaskCreator implements TaskCreator {

    final List<CreateTaskRequest> requests = new ArrayList<>();
    private final ArrayDeque<ApiFuture<Task>> scripted = new ArrayDeque<>();

    int closeCalls;
    RuntimeException createFailure;

    /** Typed {@code Throwable} so a test can script an {@code Error}, which is thrown as itself. */
    Throwable closeFailure;

    /** Scripts the next call to fail with the given status code. */
    void enqueueFailure(StatusCode.Code code) {
        scripted.add(ApiFutures.immediateFailedFuture(apiException(code)));
    }

    /** Scripts the next call to fail with the given throwable, chain and all. */
    void enqueueFailure(Throwable throwable) {
        scripted.add(ApiFutures.immediateFailedFuture(throwable));
    }

    /** Scripts the given number of consecutive calls to fail with the given status code. */
    void enqueueFailures(int count, StatusCode.Code code) {
        for (int i = 0; i < count; i++) {
            enqueueFailure(code);
        }
    }

    /** Scripts the next call to stay outstanding until the returned future is completed. */
    SettableApiFuture<Task> enqueuePending() {
        SettableApiFuture<Task> future = SettableApiFuture.create();
        scripted.add(future);
        return future;
    }

    @Override
    public ApiFuture<Task> createTask(CreateTaskRequest request) {
        if (createFailure != null) {
            throw createFailure;
        }
        requests.add(request);
        ApiFuture<Task> next = scripted.poll();
        return next != null ? next : ApiFutures.immediateFuture(request.getTask());
    }

    @Override
    public void close() {
        closeCalls++;
        if (closeFailure != null) {
            ExceptionUtils.rethrow(closeFailure);
        }
    }

    static Exception apiException(StatusCode.Code code) {
        return apiException(code, new RuntimeException("scripted " + code));
    }

    /** An exception of the given status whose cause is {@code cause}, for cause-chain tests. */
    static Exception apiException(StatusCode.Code code, Throwable cause) {
        return ApiExceptionFactory.createException(
                cause, GrpcStatusCode.of(Status.Code.valueOf(code.name())), false);
    }
}
