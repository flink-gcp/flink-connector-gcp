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

import com.google.api.gax.rpc.StatusCode;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;

import static io.github.flink.gcp.connector.bigtable.sink.writer.FakeMutationBatcher.apiException;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BigtableErrorClassifier}. */
class BigtableErrorClassifierTest {

    @ParameterizedTest
    @EnumSource(
            value = StatusCode.Code.class,
            names = {"INVALID_ARGUMENT", "FAILED_PRECONDITION"})
    void routesTheMutationRejectionsToTheHandler(StatusCode.Code code) {
        assertThat(BigtableErrorClassifier.classify(apiException(code)))
                .isEqualTo(BigtableErrorClassifier.Kind.ROW_LEVEL);
    }

    @ParameterizedTest
    @EnumSource(
            value = StatusCode.Code.class,
            names = {
                // Configuration-shaped: they fail every record alike, so dropping them would empty
                // the stream into the dead-letter destination under a green job.
                "NOT_FOUND",
                "PERMISSION_DENIED",
                "UNAUTHENTICATED",
                // Outage-shaped: the client already retried these and gave up.
                "UNAVAILABLE",
                "DEADLINE_EXCEEDED",
                "ABORTED",
                "RESOURCE_EXHAUSTED",
                "INTERNAL",
                "UNKNOWN"
            })
    void failsTheJobForEverythingElse(StatusCode.Code code) {
        assertThat(BigtableErrorClassifier.classify(apiException(code)))
                .isEqualTo(BigtableErrorClassifier.Kind.FATAL);
    }

    @Test
    void findsTheStatusAnywhereInTheCauseChain() {
        Throwable wrapped =
                new IOException(
                        "outer",
                        new RuntimeException(apiException(StatusCode.Code.INVALID_ARGUMENT)));

        assertThat(BigtableErrorClassifier.classify(wrapped))
                .isEqualTo(BigtableErrorClassifier.Kind.ROW_LEVEL);
    }

    @Test
    void readsARawGrpcStatusToo() {
        Throwable raw = new StatusRuntimeException(Status.INVALID_ARGUMENT);

        assertThat(BigtableErrorClassifier.classify(raw))
                .isEqualTo(BigtableErrorClassifier.Kind.ROW_LEVEL);
    }

    @Test
    void failsTheJobWhenNothingCarriesAStatus() {
        assertThat(BigtableErrorClassifier.classify(new IllegalStateException("boom")))
                .isEqualTo(BigtableErrorClassifier.Kind.FATAL);
    }
}
