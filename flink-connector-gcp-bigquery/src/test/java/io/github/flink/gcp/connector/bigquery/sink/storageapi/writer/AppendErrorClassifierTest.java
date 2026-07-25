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

package io.github.flink.gcp.connector.bigquery.sink.storageapi.writer;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.StorageError;
import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
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
    void hasCodeFindsCodesAnywhereInTheChain() {
        Throwable nested = new IOException("wrapper", new StatusRuntimeException(Status.NOT_FOUND));
        assertThat(AppendErrorClassifier.hasCode(nested, Status.Code.NOT_FOUND)).isTrue();
        assertThat(AppendErrorClassifier.hasCode(nested, Status.Code.UNAVAILABLE)).isFalse();
        assertThat(
                        AppendErrorClassifier.hasCode(
                                ApiExceptionFactory.createException(
                                        null, GrpcStatusCode.of(Status.Code.NOT_FOUND), false),
                                Status.Code.NOT_FOUND))
                .isTrue();
    }

    private static com.google.rpc.Status statusWithStorageError(
            Status.Code grpcCode, StorageError.StorageErrorCode errorCode) {
        return com.google.rpc.Status.newBuilder()
                .setCode(grpcCode.value())
                .setMessage("synthesized " + errorCode)
                .addDetails(
                        Any.pack(
                                StorageError.newBuilder()
                                        .setCode(errorCode)
                                        .setEntity("projects/p/datasets/d/tables/t")
                                        .setErrorMessage("synthesized " + errorCode)
                                        .build()))
                .build();
    }

    @Test
    void schemaMismatchIsDetectedFromTypedException() {
        Throwable typed =
                Exceptions.toStorageException(
                        statusWithStorageError(
                                Status.Code.INVALID_ARGUMENT,
                                StorageError.StorageErrorCode.SCHEMA_MISMATCH_EXTRA_FIELDS),
                        null);
        assertThat(typed).isInstanceOf(Exceptions.SchemaMismatchedException.class);
        assertThat(AppendErrorClassifier.isSchemaMismatch(typed)).isTrue();
        assertThat(AppendErrorClassifier.isSchemaMismatch(new IOException("wrapper", typed)))
                .isTrue();
    }

    @Test
    void schemaMismatchIsDetectedFromStatusDetails() {
        // A raw gRPC failure carrying the storage error only in the status trailers, as it
        // surfaces when the SDK has not translated it into a typed exception.
        StatusRuntimeException raw =
                StatusProto.toStatusRuntimeException(
                        statusWithStorageError(
                                Status.Code.INVALID_ARGUMENT,
                                StorageError.StorageErrorCode.SCHEMA_MISMATCH_EXTRA_FIELDS));
        assertThat(AppendErrorClassifier.isSchemaMismatch(raw)).isTrue();
    }

    @Test
    void schemaMismatchIsNotDetectedForOtherFailures() {
        assertThat(
                        AppendErrorClassifier.isSchemaMismatch(
                                new StatusRuntimeException(Status.INVALID_ARGUMENT)))
                .isFalse();
        assertThat(AppendErrorClassifier.isSchemaMismatch(new RuntimeException("boom"))).isFalse();
    }

    @Test
    void staleStreamWriterFailuresRequireWriterRefresh() {
        Throwable finalized =
                Exceptions.toStorageException(
                        statusWithStorageError(
                                Status.Code.INVALID_ARGUMENT,
                                StorageError.StorageErrorCode.STREAM_FINALIZED),
                        null);
        assertThat(finalized).isInstanceOf(Exceptions.StreamFinalizedException.class);
        assertThat(AppendErrorClassifier.requiresWriterRefresh(finalized)).isTrue();
        assertThat(
                        AppendErrorClassifier.requiresWriterRefresh(
                                new IOException("wrapper", finalized)))
                .isTrue();

        StatusRuntimeException invalidState =
                StatusProto.toStatusRuntimeException(
                        statusWithStorageError(
                                Status.Code.INVALID_ARGUMENT,
                                StorageError.StorageErrorCode.INVALID_STREAM_STATE));
        assertThat(AppendErrorClassifier.requiresWriterRefresh(invalidState)).isTrue();
    }

    @Test
    void ordinaryFailuresDoNotRequireWriterRefresh() {
        assertThat(
                        AppendErrorClassifier.requiresWriterRefresh(
                                new StatusRuntimeException(Status.UNAVAILABLE)))
                .isFalse();
        assertThat(AppendErrorClassifier.requiresWriterRefresh(new RuntimeException("boom")))
                .isFalse();
        assertThat(
                        AppendErrorClassifier.requiresWriterRefresh(
                                Exceptions.toStorageException(
                                        statusWithStorageError(
                                                Status.Code.INVALID_ARGUMENT,
                                                StorageError.StorageErrorCode
                                                        .SCHEMA_MISMATCH_EXTRA_FIELDS),
                                        null)))
                .isFalse();
    }

    @Test
    void offsetAlreadyExistsIsDetectedFromTypedExceptionAndStatusDetails() {
        Throwable typed =
                Exceptions.toStorageException(
                        statusWithStorageError(
                                Status.Code.ALREADY_EXISTS,
                                StorageError.StorageErrorCode.OFFSET_ALREADY_EXISTS),
                        null);
        assertThat(typed).isInstanceOf(Exceptions.OffsetAlreadyExists.class);
        assertThat(AppendErrorClassifier.isOffsetAlreadyExists(typed)).isTrue();
        assertThat(AppendErrorClassifier.isOffsetAlreadyExists(new IOException("wrapper", typed)))
                .isTrue();

        StatusRuntimeException raw =
                StatusProto.toStatusRuntimeException(
                        statusWithStorageError(
                                Status.Code.ALREADY_EXISTS,
                                StorageError.StorageErrorCode.OFFSET_ALREADY_EXISTS));
        assertThat(AppendErrorClassifier.isOffsetAlreadyExists(raw)).isTrue();

        assertThat(
                        AppendErrorClassifier.isOffsetAlreadyExists(
                                new StatusRuntimeException(Status.ALREADY_EXISTS)))
                .isFalse();
        assertThat(AppendErrorClassifier.isOffsetAlreadyExists(new RuntimeException("boom")))
                .isFalse();
    }

    @Test
    void offsetOutOfRangeIsDetectedFromTypedExceptionAndStatusDetails() {
        Throwable typed =
                Exceptions.toStorageException(
                        statusWithStorageError(
                                Status.Code.OUT_OF_RANGE,
                                StorageError.StorageErrorCode.OFFSET_OUT_OF_RANGE),
                        null);
        assertThat(typed).isInstanceOf(Exceptions.OffsetOutOfRange.class);
        assertThat(AppendErrorClassifier.isOffsetOutOfRange(typed)).isTrue();
        assertThat(AppendErrorClassifier.isOffsetOutOfRange(new IOException("wrapper", typed)))
                .isTrue();

        StatusRuntimeException raw =
                StatusProto.toStatusRuntimeException(
                        statusWithStorageError(
                                Status.Code.OUT_OF_RANGE,
                                StorageError.StorageErrorCode.OFFSET_OUT_OF_RANGE));
        assertThat(AppendErrorClassifier.isOffsetOutOfRange(raw)).isTrue();

        assertThat(
                        AppendErrorClassifier.isOffsetOutOfRange(
                                new StatusRuntimeException(Status.OUT_OF_RANGE)))
                .isFalse();
    }

    @Test
    void responseErrorsWithoutStorageDetailsYieldNoTypedException() {
        assertThat(
                        AppendErrorClassifier.toStorageException(
                                com.google.rpc.Status.newBuilder()
                                        .setCode(Status.Code.INVALID_ARGUMENT.value())
                                        .setMessage("no details")
                                        .build()))
                .isNull();
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
