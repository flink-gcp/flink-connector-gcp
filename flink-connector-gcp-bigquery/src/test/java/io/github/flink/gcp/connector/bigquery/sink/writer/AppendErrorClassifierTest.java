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

package io.github.flink.gcp.connector.bigquery.sink.writer;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AppendErrorClassifier}. */
class AppendErrorClassifierTest {

    private static final Map<Integer, String> ROW_ERRORS = Map.of(1, "bad row");

    @ParameterizedTest
    @EnumSource(
            value = Status.Code.class,
            names = {
                "UNAVAILABLE",
                "ABORTED",
                "INTERNAL",
                "CANCELLED",
                "DEADLINE_EXCEEDED",
                "RESOURCE_EXHAUSTED",
                "UNKNOWN"
            })
    void transientGrpcCodesClassifyAsTransient(Status.Code code) {
        assertThat(AppendErrorClassifier.classify(new StatusRuntimeException(code.toStatus())))
                .isEqualTo(AppendErrorClassifier.Kind.TRANSIENT);
    }

    @ParameterizedTest
    @EnumSource(
            value = Status.Code.class,
            names = {
                "INVALID_ARGUMENT",
                "PERMISSION_DENIED",
                "UNAUTHENTICATED",
                "FAILED_PRECONDITION",
                "NOT_FOUND",
                "OUT_OF_RANGE",
                "ALREADY_EXISTS",
                "UNIMPLEMENTED",
                "DATA_LOSS",
                "OK"
            })
    void terminalGrpcCodesClassifyAsTerminal(Status.Code code) {
        assertThat(AppendErrorClassifier.classify(new StatusRuntimeException(code.toStatus())))
                .isEqualTo(AppendErrorClassifier.Kind.TERMINAL);
    }

    @Test
    void gaxApiExceptionsClassifyByTheirCode() {
        assertThat(
                        AppendErrorClassifier.classify(
                                ApiExceptionFactory.createException(
                                        null, GrpcStatusCode.of(Status.Code.UNAVAILABLE), true)))
                .isEqualTo(AppendErrorClassifier.Kind.TRANSIENT);
        assertThat(
                        AppendErrorClassifier.classify(
                                ApiExceptionFactory.createException(
                                        null,
                                        GrpcStatusCode.of(Status.Code.PERMISSION_DENIED),
                                        false)))
                .isEqualTo(AppendErrorClassifier.Kind.TERMINAL);
    }

    @Test
    void nestedCausesAreWalked() {
        Throwable nested =
                new IOException(
                        "wrapper",
                        new ExecutionException(new StatusRuntimeException(Status.UNAVAILABLE)));
        assertThat(AppendErrorClassifier.classify(nested))
                .isEqualTo(AppendErrorClassifier.Kind.TRANSIENT);
    }

    @Test
    void throwablesWithoutStatusCodeAreTerminal() {
        assertThat(AppendErrorClassifier.classify(new RuntimeException("boom")))
                .isEqualTo(AppendErrorClassifier.Kind.TERMINAL);
    }

    @Test
    void appendErrorWithRowIndicesIsRowLevel() {
        Exceptions.AppendSerializtionError error =
                new Exceptions.AppendSerializtionError(
                        Status.Code.INVALID_ARGUMENT.value(), "bad rows", "stream", ROW_ERRORS);
        assertThat(AppendErrorClassifier.classify(error))
                .isEqualTo(AppendErrorClassifier.Kind.ROW_LEVEL);
        assertThat(AppendErrorClassifier.classify(new IOException("wrapper", error)))
                .isEqualTo(AppendErrorClassifier.Kind.ROW_LEVEL);
        assertThat(AppendErrorClassifier.findRowLevel(error))
                .hasValueSatisfying(
                        e -> assertThat(e.getRowIndexToErrorMessage()).isEqualTo(ROW_ERRORS));
    }

    @Test
    void correctlySpelledAppendSerializationErrorSubclassIsRowLevel() {
        Exceptions.AppendSerializationError error =
                new Exceptions.AppendSerializationError(
                        Status.Code.INVALID_ARGUMENT.value(), "bad rows", "stream", ROW_ERRORS);
        assertThat(AppendErrorClassifier.classify(error))
                .isEqualTo(AppendErrorClassifier.Kind.ROW_LEVEL);
    }

    @Test
    void appendErrorWithoutRowIndicesFallsBackToStatusClassification() {
        Exceptions.AppendSerializtionError error =
                new Exceptions.AppendSerializtionError(
                        Status.Code.INVALID_ARGUMENT.value(),
                        "bad request",
                        "stream",
                        Collections.emptyMap());
        assertThat(AppendErrorClassifier.classify(error))
                .isEqualTo(AppendErrorClassifier.Kind.TERMINAL);
        assertThat(AppendErrorClassifier.findRowLevel(error)).isEmpty();
    }

    @Test
    void transientResponseCodesAreRecognizedByValue() {
        assertThat(AppendErrorClassifier.isTransientCode(Status.Code.UNAVAILABLE.value())).isTrue();
        assertThat(AppendErrorClassifier.isTransientCode(Status.Code.INVALID_ARGUMENT.value()))
                .isFalse();
        assertThat(AppendErrorClassifier.isTransientCode(Status.Code.OK.value())).isFalse();
        // Unrecognized code values must not be treated as transient (fromCodeValue maps them
        // to UNKNOWN).
        assertThat(AppendErrorClassifier.isTransientCode(999)).isFalse();
    }
}
