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
import java.util.List;

/**
 * An in-memory {@link MutationBatcher} recording the mutations it receives and returning scripted
 * futures.
 *
 * <p>Unlike the real batcher nothing completes on its own: every mutation stays outstanding until
 * the test completes it, which is what lets a test hold the writer at its in-flight caps.
 */
final class FakeMutationBatcher implements MutationBatcher {

    final List<RowMutationEntry> entries = new ArrayList<>();
    final List<SettableApiFuture<Void>> futures = new ArrayList<>();

    int sendOutstandingCalls;
    int closeCalls;
    RuntimeException addFailure;

    /** Typed {@code Throwable} so a test can script an {@code Error}, which is thrown as itself. */
    Throwable closeFailure;

    /** Whether {@link #sendOutstanding()} completes every outstanding mutation successfully. */
    boolean completeOnSend;

    @Override
    public ApiFuture<Void> add(RowMutationEntry entry) {
        if (addFailure != null) {
            throw addFailure;
        }
        entries.add(entry);
        SettableApiFuture<Void> future = SettableApiFuture.create();
        futures.add(future);
        return future;
    }

    @Override
    public void sendOutstanding() {
        sendOutstandingCalls++;
        // The real batcher sends what it has buffered, so this is where a test's mutations may
        // complete — otherwise a writer flush would wait on the mailbox forever.
        if (completeOnSend) {
            for (SettableApiFuture<Void> future : futures) {
                future.set(null);
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

    /** Completes the outstanding mutation at the given index successfully. */
    void succeed(int index) {
        futures.get(index).set(null);
    }

    /** Fails the outstanding mutation at the given index with the given status. */
    void fail(int index, StatusCode.Code code) {
        futures.get(index).setException(apiException(code));
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
