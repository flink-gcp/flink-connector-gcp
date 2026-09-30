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

package io.github.flink.gcp.connector.firestore.sink.writer;

import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.Timestamp;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWrite;
import io.github.flink.gcp.connector.firestore.sink.PreconditionFailurePolicy;
import io.github.flink.gcp.connector.firestore.sink.writer.FirestoreErrorClassifier.Kind;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Map;

import static io.github.flink.gcp.connector.firestore.sink.PreconditionFailurePolicy.FAIL_JOB;
import static io.github.flink.gcp.connector.firestore.sink.PreconditionFailurePolicy.ROUTE_TO_FAILURE_HANDLER;
import static io.github.flink.gcp.connector.firestore.sink.writer.FakeFirestoreDatabaseAccess.failure;
import static org.assertj.core.api.Assertions.assertThat;

class FirestoreErrorClassifierTest {

    private static final FirestoreWrite SET = FirestoreWrite.set("c/a", Map.of());
    private static final FirestoreWrite CREATE = FirestoreWrite.create("c/a", Map.of());
    private static final FirestoreWrite CONDITIONAL =
            FirestoreWrite.delete("c/a", Timestamp.ofTimeSecondsAndNanos(1, 0));

    @ParameterizedTest
    @EnumSource(Status.Code.class)
    void everyGrpcCodeMapsToTheGaxCodeOfTheSameName(Status.Code code) {
        assertThat(FirestoreErrorClassifier.toGax(code))
                .isEqualTo(StatusCode.Code.valueOf(code.name()));
    }

    @ParameterizedTest
    @EnumSource(Status.Code.class)
    void onlyTheDocumentedStatusesAreRoutable(Status.Code code) {
        for (PreconditionFailurePolicy policy : PreconditionFailurePolicy.values()) {
            for (FirestoreWrite write : List.of(SET, CREATE, CONDITIONAL)) {
                Kind expected;
                if (code == Status.Code.INVALID_ARGUMENT) {
                    expected = Kind.INVALID;
                } else if (code == Status.Code.ALREADY_EXISTS && write == CREATE) {
                    expected = Kind.REFUSED;
                } else if (code == Status.Code.FAILED_PRECONDITION
                        && write == CONDITIONAL
                        && policy == ROUTE_TO_FAILURE_HANDLER) {
                    expected = Kind.REFUSED;
                } else {
                    expected = Kind.FATAL;
                }

                assertThat(
                                FirestoreErrorClassifier.classify(
                                        failure(Status.fromCode(code)), write, policy))
                        .as("%s for %s under %s", code, write, policy)
                        .isEqualTo(expected);
            }
        }
    }

    @Test
    void aTransientStatusAnywhereInTheChainWins() {
        Throwable chain =
                new StatusRuntimeException(
                        Status.INVALID_ARGUMENT.withCause(
                                new StatusRuntimeException(Status.RESOURCE_EXHAUSTED)));

        assertThat(FirestoreErrorClassifier.classify(chain, SET, FAIL_JOB)).isEqualTo(Kind.FATAL);
        assertThat(FirestoreErrorClassifier.statusCode(chain))
                .isEqualTo(StatusCode.Code.INVALID_ARGUMENT);
    }

    @Test
    void theFirstClassifiableStatusDecidesSoABuriedDataStatusIsNotRouted() {
        Throwable chain =
                new StatusRuntimeException(
                        Status.INTERNAL.withCause(failure(Status.INVALID_ARGUMENT)));

        assertThat(FirestoreErrorClassifier.classify(chain, SET, FAIL_JOB)).isEqualTo(Kind.FATAL);
    }

    @Test
    void aFailureCarryingNoStatusIsFatal() {
        assertThat(FirestoreErrorClassifier.classify(new RuntimeException(), SET, FAIL_JOB))
                .isEqualTo(Kind.FATAL);
        assertThat(FirestoreErrorClassifier.statusCode(new RuntimeException())).isNull();
    }
}
