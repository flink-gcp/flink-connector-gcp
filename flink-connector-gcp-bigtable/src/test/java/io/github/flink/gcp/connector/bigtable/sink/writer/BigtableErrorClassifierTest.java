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

    @Test
    void routesTheOneStatusThatIsInvalidRegardlessOfSystemState() {
        assertThat(BigtableErrorClassifier.classify(apiException(StatusCode.Code.INVALID_ARGUMENT)))
                .isEqualTo(BigtableErrorClassifier.Kind.ROW_LEVEL);
    }

    @Test
    void classifiesAMissingTableAsRepairable() {
        assertThat(BigtableErrorClassifier.classify(apiException(StatusCode.Code.NOT_FOUND)))
                .isEqualTo(BigtableErrorClassifier.Kind.TABLE_NOT_FOUND);
    }

    @Test
    void recognizesTheRealServicesMissingFamilyDescription() {
        Throwable failure =
                new IOException(
                        "outer",
                        FakeMutationBatcher.apiException(
                                StatusCode.Code.NOT_FOUND,
                                BigtableErrorClassifier.MISSING_COLUMN_FAMILY_DESCRIPTION));

        assertThat(BigtableErrorClassifier.isMissingColumnFamily(failure)).isTrue();
    }

    @Test
    void doesNotReadTheDescriptionFromADifferentStatusInTheChain() {
        Throwable failure =
                FakeMutationBatcher.apiException(
                        StatusCode.Code.NOT_FOUND,
                        FakeMutationBatcher.apiException(
                                StatusCode.Code.INTERNAL,
                                BigtableErrorClassifier.MISSING_COLUMN_FAMILY_DESCRIPTION));

        assertThat(BigtableErrorClassifier.isMissingColumnFamily(failure)).isFalse();
    }

    @Test
    void findsAMissingTableAheadOfATransientStatus() {
        // The load-bearing precedence: a NOT_FOUND chain that also carries a transient status is
        // still the missing-table failure, or an outage during the incident would turn a
        // repairable park into a job failure. Safe because the repair re-applies and never drops.
        Throwable chain =
                apiException(StatusCode.Code.UNAVAILABLE, apiException(StatusCode.Code.NOT_FOUND));

        assertThat(BigtableErrorClassifier.classify(chain))
                .isEqualTo(BigtableErrorClassifier.Kind.TABLE_NOT_FOUND);
    }

    @Test
    void findsAMissingTableAheadOfADataShapedStatus() {
        Throwable chain =
                apiException(
                        StatusCode.Code.INVALID_ARGUMENT, apiException(StatusCode.Code.NOT_FOUND));

        assertThat(BigtableErrorClassifier.classify(chain))
                .isEqualTo(BigtableErrorClassifier.Kind.TABLE_NOT_FOUND);
    }

    @ParameterizedTest
    @EnumSource(
            value = StatusCode.Code.class,
            names = {
                // State-dependent by gRPC's own definition, so a drop could discard a record the
                // system would have accepted in another state — however data-shaped the failures
                // it names look.
                "FAILED_PRECONDITION",
                "OUT_OF_RANGE",
                // Configuration-shaped: they fail every record alike, so dropping them would empty
                // the stream into the dead-letter destination under a green job.
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
    void findsTheStatusThroughTheWrappersTheClientAdds() {
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
    void neverRoutesAChainThatCarriesATransientStatusAnywhere() {
        // The first classifiable status is data-shaped, but the failure underneath it is the
        // service being unavailable. Routing it would let an outage produce dead letters.
        Throwable chain =
                apiException(
                        StatusCode.Code.INVALID_ARGUMENT,
                        apiException(StatusCode.Code.UNAVAILABLE));

        assertThat(BigtableErrorClassifier.classify(chain))
                .isEqualTo(BigtableErrorClassifier.Kind.FATAL);
    }

    @Test
    void readsTheDataShapedStatusFromTheFirstClassifiableOneOnly() {
        // The mirror-image mistake: an INVALID_ARGUMENT buried under a server-side failure
        // describes the inner call, so dropping the mutation over it would discard a record.
        Throwable chain =
                apiException(
                        StatusCode.Code.INTERNAL, apiException(StatusCode.Code.INVALID_ARGUMENT));

        assertThat(BigtableErrorClassifier.classify(chain))
                .isEqualTo(BigtableErrorClassifier.Kind.FATAL);
    }

    @Test
    void failsTheJobWhenNothingCarriesAStatus() {
        assertThat(BigtableErrorClassifier.classify(new IllegalStateException("boom")))
                .isEqualTo(BigtableErrorClassifier.Kind.FATAL);
    }
}
