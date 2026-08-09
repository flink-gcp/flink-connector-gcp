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

package io.github.flink.gcp.connector.spanner.sink.writer;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import io.github.flink.gcp.connector.spanner.sink.ConstraintViolationPolicy;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SpannerErrorClassifier}. */
class SpannerErrorClassifierTest {

    @ParameterizedTest
    @EnumSource(
            value = StatusCode.Code.class,
            names = {"ABORTED", "UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED"})
    void treatsTheServicesOwnInstabilityAsTransient(StatusCode.Code code) {
        assertThat(SpannerErrorClassifier.classify(code, ConstraintViolationPolicy.FAIL_JOB))
                .isEqualTo(SpannerErrorClassifier.Kind.TRANSIENT);
    }

    @ParameterizedTest
    @EnumSource(
            value = StatusCode.Code.class,
            names = {"INVALID_ARGUMENT", "ALREADY_EXISTS"})
    void routesOnlyWhatIsUnrecoverableByDefinition(StatusCode.Code code) {
        assertThat(SpannerErrorClassifier.classify(code, ConstraintViolationPolicy.FAIL_JOB))
                .isEqualTo(SpannerErrorClassifier.Kind.ROW_LEVEL);
    }

    @ParameterizedTest
    @EnumSource(
            value = StatusCode.Code.class,
            names = {"FAILED_PRECONDITION", "OUT_OF_RANGE"})
    void aConstraintViolationFailsTheJobByDefault(StatusCode.Code code) {
        assertThat(SpannerErrorClassifier.classify(code, ConstraintViolationPolicy.FAIL_JOB))
                .isEqualTo(SpannerErrorClassifier.Kind.FATAL);
    }

    @ParameterizedTest
    @EnumSource(
            value = StatusCode.Code.class,
            names = {"FAILED_PRECONDITION", "OUT_OF_RANGE"})
    void aConstraintViolationIsRoutedWhenTheJobAsksForIt(StatusCode.Code code) {
        // Both statuses, because Spanner splits constraint violations across them — NOT NULL and
        // foreign keys under one, CHECK under the other. Covering only one would leave a job that
        // opted in still dying on its first CHECK violation.
        assertThat(
                        SpannerErrorClassifier.classify(
                                code, ConstraintViolationPolicy.ROUTE_TO_FAILURE_HANDLER))
                .isEqualTo(SpannerErrorClassifier.Kind.ROW_LEVEL);
    }

    @ParameterizedTest
    @EnumSource(
            value = StatusCode.Code.class,
            names = {"UNAVAILABLE", "ABORTED", "NOT_FOUND", "PERMISSION_DENIED"})
    void thePolicyMovesNothingButConstraintViolations(StatusCode.Code code) {
        assertThat(
                        SpannerErrorClassifier.classify(
                                code, ConstraintViolationPolicy.ROUTE_TO_FAILURE_HANDLER))
                .isEqualTo(
                        SpannerErrorClassifier.classify(code, ConstraintViolationPolicy.FAIL_JOB));
    }

    @ParameterizedTest
    @EnumSource(
            value = StatusCode.Code.class,
            names = {
                "FAILED_PRECONDITION",
                "NOT_FOUND",
                "PERMISSION_DENIED",
                "UNAUTHENTICATED",
                "UNIMPLEMENTED",
                "INTERNAL",
                "OUT_OF_RANGE",
                "DATA_LOSS",
                "CANCELLED",
                "UNKNOWN"
            })
    void failsTheJobOnEverythingElse(StatusCode.Code code) {
        // FAILED_PRECONDITION is the one worth naming: Spanner answers a NOT NULL violation with
        // it, which looks data-shaped, but it also covers states that clear on their own — so a
        // dropping policy must not be handed it.
        assertThat(SpannerErrorClassifier.classify(code, ConstraintViolationPolicy.FAIL_JOB))
                .isEqualTo(SpannerErrorClassifier.Kind.FATAL);
    }

    @Test
    void aStatusItCannotClassifyIsFatal() {
        assertThat(
                        SpannerErrorClassifier.classify(
                                (StatusCode.Code) null, ConstraintViolationPolicy.FAIL_JOB))
                .isEqualTo(SpannerErrorClassifier.Kind.FATAL);
    }

    @ParameterizedTest
    @EnumSource(ErrorCode.class)
    void mapsEveryCodeTheClientLibraryCanReport(ErrorCode errorCode) {
        // Checked against the client library's own gRPC code rather than against a restatement of
        // the switch, so a transposed pair fails here too — not only a missing one. A code added
        // to the library lands on the null branch and fails the same assertion.
        assertThat(SpannerErrorClassifier.fromErrorCode(errorCode))
                .as("no gax code is mapped for %s", errorCode)
                .isNotNull()
                .extracting(Enum::name)
                .isEqualTo(errorCode.getGrpcStatusCode().name());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
    void mapsEveryCanonicalStatusNumberAGroupCanCarry(int canonicalCode) {
        // Same independent oracle. Transposing RESOURCE_EXHAUSTED (8) and FAILED_PRECONDITION (9)
        // would otherwise survive: a retryable status would fail the job, and a terminal one would
        // burn the whole retry budget.
        assertThat(SpannerErrorClassifier.fromCanonicalCode(canonicalCode))
                .as("no gax code is mapped for canonical %s", canonicalCode)
                .isNotNull()
                .extracting(Enum::name)
                .isEqualTo(Status.fromCodeValue(canonicalCode).getCode().name());
    }

    @Test
    void okAndUnknownNumbersMapToNothing() {
        assertThat(SpannerErrorClassifier.fromCanonicalCode(0)).isNull();
        assertThat(SpannerErrorClassifier.fromCanonicalCode(99)).isNull();
    }

    @Test
    void readsTheCodeOffASpannerException() {
        SpannerException failure =
                SpannerExceptionFactory.newSpannerException(ErrorCode.UNAVAILABLE, "backend down");

        assertThat(SpannerErrorClassifier.statusCode(failure))
                .isEqualTo(StatusCode.Code.UNAVAILABLE);
        assertThat(SpannerErrorClassifier.classify(failure))
                .isEqualTo(SpannerErrorClassifier.Kind.TRANSIENT);
    }

    @Test
    void readsTheCodeOffAGaxException() {
        ApiException failure = apiException(StatusCode.Code.RESOURCE_EXHAUSTED);

        assertThat(SpannerErrorClassifier.statusCode(failure))
                .isEqualTo(StatusCode.Code.RESOURCE_EXHAUSTED);
        assertThat(SpannerErrorClassifier.classify(failure))
                .isEqualTo(SpannerErrorClassifier.Kind.TRANSIENT);
    }

    @Test
    void readsTheCodeOffARawGrpcException() {
        StatusRuntimeException failure = Status.UNAVAILABLE.asRuntimeException();

        assertThat(SpannerErrorClassifier.statusCode(failure))
                .isEqualTo(StatusCode.Code.UNAVAILABLE);
    }

    @Test
    void findsATransientStatusBuriedUnderAnotherOne() {
        // A wrapper carrying a terminal-looking status must not turn an unstable service into a
        // job failure, so the transient search runs over the whole chain.
        SpannerException buried =
                SpannerExceptionFactory.newSpannerException(
                        ErrorCode.INTERNAL,
                        "wrapped",
                        SpannerExceptionFactory.newSpannerException(
                                ErrorCode.UNAVAILABLE, "backend down"));

        assertThat(SpannerErrorClassifier.classify(buried))
                .isEqualTo(SpannerErrorClassifier.Kind.TRANSIENT);
        // The counter still reports what the failure was reported under, which is the outer one.
        assertThat(SpannerErrorClassifier.statusCode(buried)).isEqualTo(StatusCode.Code.INTERNAL);
    }

    @Test
    void aRequestFailureIsNeverRouted() {
        // INVALID_ARGUMENT on one group is row-level; the same status on the whole request is not,
        // because it says nothing about which of the mutations is at fault.
        SpannerException failure =
                SpannerExceptionFactory.newSpannerException(
                        ErrorCode.INVALID_ARGUMENT, "too many mutations");

        assertThat(SpannerErrorClassifier.classify(failure))
                .isEqualTo(SpannerErrorClassifier.Kind.FATAL);
    }

    @Test
    void aFailureCarryingNoStatusIsFatal() {
        IOException failure = new IOException("channel closed");

        assertThat(SpannerErrorClassifier.statusCode(failure)).isNull();
        assertThat(SpannerErrorClassifier.classify(failure))
                .isEqualTo(SpannerErrorClassifier.Kind.FATAL);
    }

    private static ApiException apiException(StatusCode.Code code) {
        return ApiExceptionFactory.createException(
                new RuntimeException("boom"),
                GrpcStatusCode.of(Status.Code.valueOf(code.name())),
                false);
    }
}
