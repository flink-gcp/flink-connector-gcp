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

import com.google.api.gax.rpc.StatusCode;
import io.github.flink.gcp.connector.datastore.sink.writer.DatastoreErrorClassifier.Kind;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import java.util.EnumMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DatastoreErrorClassifierTest {

    /**
     * Every gax code with the class it is expected to fall into, written out: a code gax adds later
     * is missing here and fails the first test, rather than being called fatal silently.
     */
    private static final Map<StatusCode.Code, Kind> EXPECTED = new EnumMap<>(StatusCode.Code.class);

    static {
        EXPECTED.put(StatusCode.Code.OK, Kind.FATAL);
        EXPECTED.put(StatusCode.Code.CANCELLED, Kind.FATAL);
        EXPECTED.put(StatusCode.Code.UNKNOWN, Kind.FATAL);
        EXPECTED.put(StatusCode.Code.INVALID_ARGUMENT, Kind.CANDIDATE);
        EXPECTED.put(StatusCode.Code.DEADLINE_EXCEEDED, Kind.TRANSIENT);
        EXPECTED.put(StatusCode.Code.NOT_FOUND, Kind.CANDIDATE);
        EXPECTED.put(StatusCode.Code.ALREADY_EXISTS, Kind.CANDIDATE);
        EXPECTED.put(StatusCode.Code.PERMISSION_DENIED, Kind.FATAL);
        EXPECTED.put(StatusCode.Code.RESOURCE_EXHAUSTED, Kind.TRANSIENT);
        EXPECTED.put(StatusCode.Code.FAILED_PRECONDITION, Kind.FATAL);
        EXPECTED.put(StatusCode.Code.ABORTED, Kind.TRANSIENT);
        EXPECTED.put(StatusCode.Code.OUT_OF_RANGE, Kind.FATAL);
        EXPECTED.put(StatusCode.Code.UNIMPLEMENTED, Kind.FATAL);
        EXPECTED.put(StatusCode.Code.INTERNAL, Kind.FATAL);
        EXPECTED.put(StatusCode.Code.UNAVAILABLE, Kind.TRANSIENT);
        EXPECTED.put(StatusCode.Code.DATA_LOSS, Kind.FATAL);
        EXPECTED.put(StatusCode.Code.UNAUTHENTICATED, Kind.FATAL);
    }

    @Test
    void everyCodeHasAClass() {
        assertThat(EXPECTED.keySet()).containsExactlyInAnyOrder(StatusCode.Code.values());
        for (StatusCode.Code code : StatusCode.Code.values()) {
            assertThat(DatastoreErrorClassifier.classify(FakeDatastoreDatabaseAccess.failure(code)))
                    .as(code.name())
                    .isEqualTo(EXPECTED.get(code));
            assertThat(
                            DatastoreErrorClassifier.statusCode(
                                    FakeDatastoreDatabaseAccess.failure(code)))
                    .isEqualTo(code);
        }
    }

    @Test
    void aTransientStatusAnywhereInTheChainWins() {
        RuntimeException wrapped =
                new RuntimeException(
                        "outer",
                        withCause(
                                FakeDatastoreDatabaseAccess.failure(
                                        StatusCode.Code.INVALID_ARGUMENT),
                                FakeDatastoreDatabaseAccess.failure(StatusCode.Code.UNAVAILABLE)));

        assertThat(DatastoreErrorClassifier.classify(wrapped)).isEqualTo(Kind.TRANSIENT);
    }

    @Test
    void theOutermostStatusDecidesOtherwise() {
        RuntimeException internalOverInvalid =
                withCause(
                        FakeDatastoreDatabaseAccess.failure(StatusCode.Code.INTERNAL),
                        FakeDatastoreDatabaseAccess.failure(StatusCode.Code.INVALID_ARGUMENT));

        assertThat(DatastoreErrorClassifier.classify(internalOverInvalid)).isEqualTo(Kind.FATAL);
        assertThat(DatastoreErrorClassifier.statusCode(internalOverInvalid))
                .isEqualTo(StatusCode.Code.INTERNAL);
    }

    @Test
    void aRawGrpcStatusIsReadAndAFailureWithoutOneIsFatal() {
        assertThat(DatastoreErrorClassifier.classify(Status.ABORTED.asRuntimeException()))
                .isEqualTo(Kind.TRANSIENT);
        assertThat(DatastoreErrorClassifier.classify(new IllegalStateException("no status")))
                .isEqualTo(Kind.FATAL);
        assertThat(DatastoreErrorClassifier.statusCode(new IllegalStateException("no status")))
                .isNull();
    }

    /** Appends a cause to the end of the outer failure's chain. */
    private static RuntimeException withCause(RuntimeException outer, Throwable cause) {
        Throwable last = outer;
        while (last.getCause() != null) {
            last = last.getCause();
        }
        last.initCause(cause);
        return outer;
    }
}
