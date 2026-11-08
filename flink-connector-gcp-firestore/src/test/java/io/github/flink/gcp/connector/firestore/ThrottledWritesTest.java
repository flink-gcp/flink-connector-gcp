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

package io.github.flink.gcp.connector.firestore;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.datastore.DatastoreException;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests {@link ThrottledWrites}. */
class ThrottledWritesTest {

    @Test
    void aThrottledWriteIsSentAgainUntilItReturns() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        String result =
                ThrottledWrites.retry(
                        () -> {
                            if (calls.incrementAndGet() == 1) {
                                // As the client library's future reports it.
                                throw new ExecutionException(failure(Status.Code.UNAVAILABLE));
                            }
                            return "applied";
                        });
        assertThat(result).isEqualTo("applied");
        assertThat(calls).hasValue(2);
    }

    @Test
    void anyOtherStatusIsThrownAtOnce() {
        AtomicInteger calls = new AtomicInteger();
        RuntimeException refused = failure(Status.Code.INVALID_ARGUMENT);
        assertThatThrownBy(
                        () ->
                                ThrottledWrites.retry(
                                        () -> {
                                            calls.incrementAndGet();
                                            throw refused;
                                        }))
                .isSameAs(refused);
        assertThat(calls).hasValue(1);
    }

    @Test
    void aWriteThrottledPastTheDeadlineFailsWithItsLastAnswer() {
        AtomicInteger calls = new AtomicInteger();
        RuntimeException throttled = failure(Status.Code.RESOURCE_EXHAUSTED);
        // A deadline shorter than the first backoff: the first answer already ends the retries.
        assertThatThrownBy(
                        () ->
                                ThrottledWrites.retry(
                                        () -> {
                                            calls.incrementAndGet();
                                            throw throttled;
                                        },
                                        Duration.ofMillis(500)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("RESOURCE_EXHAUSTED on attempt 1")
                .hasCause(throttled);
        assertThat(calls).hasValue(1);
    }

    @Test
    void theRetriedStatusesAreReadAnywhereInTheCauseChain() {
        assertThat(
                        ThrottledWrites.retriedCode(
                                new ExecutionException(failure(Status.Code.RESOURCE_EXHAUSTED))))
                .isEqualTo(StatusCode.Code.RESOURCE_EXHAUSTED);
        assertThat(ThrottledWrites.retriedCode(failure(Status.Code.UNAVAILABLE)))
                .isEqualTo(StatusCode.Code.UNAVAILABLE);
        assertThat(ThrottledWrites.retriedCode(failure(Status.Code.ABORTED))).isNull();
        assertThat(ThrottledWrites.retriedCode(new IllegalStateException("no status"))).isNull();
    }

    @Test
    void aDatastoreExceptionIsReadByItsReason() {
        // The client library's put can throw one carrying the status only as its reason.
        assertThat(
                        ThrottledWrites.retriedCode(
                                new DatastoreException(8, "throttled", "RESOURCE_EXHAUSTED")))
                .isEqualTo(StatusCode.Code.RESOURCE_EXHAUSTED);
        assertThat(
                        ThrottledWrites.retriedCode(
                                new DatastoreException(3, "refused", "INVALID_ARGUMENT")))
                .isNull();
        assertThat(ThrottledWrites.retriedCode(new DatastoreException(0, "none", "NOT_A_STATUS")))
                .isNull();
    }

    private static RuntimeException failure(Status.Code code) {
        return ApiExceptionFactory.createException(
                code.toStatus().asRuntimeException(), GrpcStatusCode.of(code), false);
    }
}
