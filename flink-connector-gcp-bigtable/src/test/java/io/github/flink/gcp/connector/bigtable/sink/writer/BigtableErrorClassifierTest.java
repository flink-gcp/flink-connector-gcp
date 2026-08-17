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

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.api.gax.rpc.StatusCode;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

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
    void recognizesTheDescriptionTheServiceActuallySends() {
        // Verbatim from the service (2026-08-17, google-cloud-bigtable 2.81.0), caught by
        // BigtableAutoCreationRealGcpITCase and reported in #948; the resource path is redacted.
        // The phrase the classifier looks for ends a sentence naming the row and the table, so
        // comparing it against the whole description — which is what the classifier used to do —
        // never held against the service, and the writer spent its whole recovery budget instead
        // of naming the family.
        Throwable failure =
                new IOException(
                        "outer",
                        FakeMutationBatcher.apiException(
                                StatusCode.Code.NOT_FOUND,
                                "Error while mutating the row 'row-1'"
                                        + " (projects/p/instances/i/tables/unrepairable-family) :"
                                        + " Requested column family not found."));

        assertThat(BigtableErrorClassifier.isMissingColumnFamily(failure)).isTrue();
    }

    @Test
    void recognizesTheBarePhraseAsAWholeDescription() {
        Throwable failure =
                new IOException(
                        "outer",
                        FakeMutationBatcher.apiException(
                                StatusCode.Code.NOT_FOUND,
                                BigtableErrorClassifier.MISSING_COLUMN_FAMILY_PHRASE));

        assertThat(BigtableErrorClassifier.isMissingColumnFamily(failure)).isTrue();
    }

    @Test
    void doesNotReadTheDescriptionFromADifferentStatusInTheChain() {
        // What keeps the phrase from being read out of a failure that is not the missing family is
        // the status of the node carrying it, not how much of that node's message it accounts for.
        Throwable failure =
                FakeMutationBatcher.apiException(
                        StatusCode.Code.NOT_FOUND,
                        FakeMutationBatcher.apiException(
                                StatusCode.Code.INTERNAL,
                                "Error while mutating the row 'row-1' : "
                                        + BigtableErrorClassifier.MISSING_COLUMN_FAMILY_PHRASE));

        assertThat(BigtableErrorClassifier.isMissingColumnFamily(failure)).isFalse();
    }

    @Test
    void answersFalseForANotFoundCarryingNoDescriptionAtAll() {
        // The failure would be an NPE raised inside a mailbox mail, which reaches the job as
        // something that names neither Bigtable nor the mutation.
        Throwable failure =
                FakeMutationBatcher.apiException(
                        StatusCode.Code.NOT_FOUND,
                        ApiExceptionFactory.createException(
                                null, null, GrpcStatusCode.of(Status.Code.NOT_FOUND), false));

        assertThat(BigtableErrorClassifier.isMissingColumnFamily(failure)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                // Both wordings as ADR-0073 measured them, resources redacted: the service's on
                // 2026-08-09, the emulator's on 2026-08-08.
                "No tables found for instance projects/p/instances/i",
                "table projects/p/instances/i/tables/orders not found"
            })
    void doesNotTakeAMissingTableForAMissingFamily(String description) {
        // The other half of what the writer routes on: a table that does not exist is repairable by
        // creation, so it must keep the bounded retry rather than fail fast naming a family. A
        // description matched by containment has to stay narrow enough to tell the two apart.
        Throwable failure =
                FakeMutationBatcher.apiException(StatusCode.Code.NOT_FOUND, description);

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
