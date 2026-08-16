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

package io.github.flink.gcp.connector.bigquery.source.enumerator;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The sentence a read of a view earns.
 *
 * <p>What BigQuery answers is measured rather than assumed: on 2026-08-10 a logical view and a
 * materialized view both answered {@code CreateReadSession} with {@code INVALID_ARGUMENT: request
 * failed: non-table entities cannot be read with the storage API}. The cases below are the same
 * failure with each half of that changed, because the hint has to stay off every other {@code
 * INVALID_ARGUMENT} the same call produces — a bad projection, an unparsable row restriction, a
 * snapshot outside the time-travel window.
 */
class ReadClientSessionCreatorTest {

    private static final String VIEW_MESSAGE =
            "request failed: non-table entities cannot be read with the storage API";

    @Test
    void namesTheQueryKnobWhenBigQueryRefusedANonTable() {
        String hint = ReadClientSessionCreator.viewHint(invalidArgument(VIEW_MESSAGE));

        assertThat(hint).contains("query(...)").contains("view");
    }

    @Test
    void staysOffAnInvalidArgumentThatIsNotAboutANonTable() {
        assertThat(ReadClientSessionCreator.viewHint(invalidArgument("Invalid row restriction")))
                .isNull();
    }

    @Test
    void staysOffAFailureWithAnotherStatusCode() {
        // PERMISSION_DENIED is what a missing table can answer with (ADR-0030), and it is not a
        // view. Keying on the message alone would claim it was one.
        assertThat(
                        ReadClientSessionCreator.viewHint(
                                ApiExceptionFactory.createException(
                                        new IllegalStateException(VIEW_MESSAGE),
                                        GrpcStatusCode.of(Status.Code.PERMISSION_DENIED),
                                        false)))
                .isNull();
    }

    @Test
    void staysOffAFailureCarryingNoMessageAtAll() {
        // gax promises no message text; keying the hint on it must survive one that is absent.
        assertThat(
                        ReadClientSessionCreator.viewHint(
                                ApiExceptionFactory.createException(
                                        null,
                                        GrpcStatusCode.of(Status.Code.INVALID_ARGUMENT),
                                        false)))
                .isNull();
    }

    @Test
    void refusesToCreateAfterItWasClosed() {
        // The race close() guards against: a cancellation lands while the coordinator worker is
        // still planning. A creator that built a client here would leak it in the JobManager,
        // because the close that should release it has already run.
        //
        // The passing test does no I/O: the guard throws before any client exists. With the
        // guard gone, a real client is built against localhost:1 and the test fails only after
        // gax's ~10-minute retry budget (599.7 s, measured under exactly that mutation) —
        // deliberately not decorated with @Timeout, which was measured against the same
        // mutation: the same-thread interrupt fires at the deadline, gax's retry loop does not
        // abort on it, and the wall clock does not move.
        ReadClientSessionCreator creator =
                new ReadClientSessionCreator(EmulatorEndpoint.parse("localhost:1"));
        creator.close();

        assertThatThrownBy(() -> creator.create(CreateReadSessionRequest.getDefaultInstance()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("was closed");
    }

    private static ApiException invalidArgument(String message) {
        return ApiExceptionFactory.createException(
                new IllegalStateException(message),
                GrpcStatusCode.of(Status.Code.INVALID_ARGUMENT),
                false);
    }
}
