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

package io.github.flink.gcp.connector.pubsub.sink.writer;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiExceptionFactory;
import io.github.flink.gcp.connector.pubsub.sink.writer.PubSubErrorClassifier.Kind;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PubSubErrorClassifier}. */
class PubSubErrorClassifierTest {

    private static StatusRuntimeException grpc(Status.Code code) {
        return new StatusRuntimeException(Status.fromCode(code));
    }

    private static Throwable gax(Status.Code code) {
        return ApiExceptionFactory.createException(grpc(code), GrpcStatusCode.of(code), false);
    }

    @Test
    void aNotFoundIsTheRepairableTopicFailure() {
        assertThat(PubSubErrorClassifier.classify(grpc(Status.Code.NOT_FOUND)))
                .isEqualTo(Kind.TOPIC_NOT_FOUND);
        assertThat(PubSubErrorClassifier.classify(gax(Status.Code.NOT_FOUND)))
                .isEqualTo(Kind.TOPIC_NOT_FOUND);
    }

    @Test
    void aCancellationIsItsOwnClass() {
        assertThat(PubSubErrorClassifier.classify(new CancellationException("key paused")))
                .isEqualTo(Kind.CANCELLATION);
    }

    @Test
    void anInvalidArgumentIsMessageLevel() {
        assertThat(PubSubErrorClassifier.classify(grpc(Status.Code.INVALID_ARGUMENT)))
                .isEqualTo(Kind.MESSAGE_LEVEL);
        assertThat(PubSubErrorClassifier.classify(gax(Status.Code.INVALID_ARGUMENT)))
                .isEqualTo(Kind.MESSAGE_LEVEL);
    }

    @Test
    void aFailureWithoutAStatusIsFatal() {
        assertThat(PubSubErrorClassifier.classify(new RuntimeException("publish exploded")))
                .isEqualTo(Kind.FATAL);
    }

    /**
     * The class that must stay narrow: an outage reaching a dropping handler would bleed the stream
     * message by message while the service is simply unavailable.
     */
    @ParameterizedTest
    @EnumSource(
            value = Status.Code.class,
            names = {
                "UNAVAILABLE",
                "DEADLINE_EXCEEDED",
                "RESOURCE_EXHAUSTED",
                "INTERNAL",
                "ABORTED",
                "UNKNOWN",
                "PERMISSION_DENIED",
                "UNAUTHENTICATED",
                "FAILED_PRECONDITION"
            })
    void everyOtherStatusIsFatal(Status.Code code) {
        assertThat(PubSubErrorClassifier.classify(gax(code))).isEqualTo(Kind.FATAL);
    }

    @Test
    void theStatusIsFoundThroughTheCauseChainTheSdkWrapsItIn() {
        Throwable wrapped =
                new ExecutionException(
                        "publish failed", new IOException("io", gax(Status.Code.INVALID_ARGUMENT)));

        assertThat(PubSubErrorClassifier.classify(wrapped)).isEqualTo(Kind.MESSAGE_LEVEL);
    }

    @Test
    void aNotFoundWinsOverACancellationInTheSameChain() {
        // Precedence, not coincidence: the repair is the writer's answer to a missing topic, and a
        // chain that also mentions a cancellation must not divert it into the cascade branch.
        Throwable wrapped =
                new CancellationException("key paused").initCause(grpc(Status.Code.NOT_FOUND));

        assertThat(PubSubErrorClassifier.classify(wrapped)).isEqualTo(Kind.TOPIC_NOT_FOUND);
    }

    @Test
    void aCancellationWinsOverAnInvalidArgumentInTheSameChain() {
        // A cascade is never a root cause (#78), so it must not be routed to the failure handler on
        // the strength of the root's status turning up in its chain.
        Throwable wrapped =
                new CancellationException("key paused")
                        .initCause(grpc(Status.Code.INVALID_ARGUMENT));

        assertThat(PubSubErrorClassifier.classify(wrapped)).isEqualTo(Kind.CANCELLATION);
    }
}
