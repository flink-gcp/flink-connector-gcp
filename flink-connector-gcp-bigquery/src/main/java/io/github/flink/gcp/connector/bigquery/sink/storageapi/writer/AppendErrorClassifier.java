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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.ExceptionUtils;

import com.google.api.gax.rpc.ApiException;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.StorageError;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

/**
 * Classifies Storage Write API append failures into the error classes the writer routes on.
 *
 * <ul>
 *   <li>{@link Kind#ROW_LEVEL} — the append was rejected because of specific rows, identified by
 *       index ({@link Exceptions.AppendSerializtionError}); the remaining rows of the batch are
 *       valid. Routed to the configured {@code FailedRowHandler}.
 *   <li>{@link Kind#TRANSIENT} — the failure is retriable ({@code UNAVAILABLE}, {@code ABORTED},
 *       {@code INTERNAL}, {@code CANCELLED}, {@code DEADLINE_EXCEEDED}, {@code RESOURCE_EXHAUSTED}
 *       and {@code UNKNOWN}). These are normally retried inside the SDK; when one still surfaces,
 *       the writer re-appends the batch itself within a bounded retry budget. {@code UNKNOWN} is
 *       treated as transient deliberately: retrying is safe under at-least-once semantics and the
 *       budget bounds the delay if the error turns out to be permanent.
 *   <li>{@link Kind#TERMINAL} — everything else ({@code INVALID_ARGUMENT}, {@code
 *       PERMISSION_DENIED}, {@code NOT_FOUND}, failures without any status code, ...): retrying
 *       cannot help, the failure must surface and fail the checkpoint. {@code NOT_FOUND} is
 *       classified terminal here; the writer intercepts it beforehand when table auto-creation
 *       applies.
 * </ul>
 *
 * <p>Orthogonal to the classes above, {@link #isSchemaMismatch} and {@link #requiresWriterRefresh}
 * detect failures the writer intercepts before classification: schema mismatches (repaired by a
 * schema update when enabled) and stale-stream-writer failures (repaired by rebuilding the
 * destination's writer).
 */
@Internal
public final class AppendErrorClassifier {

    /** The error classes appends can fail with. */
    enum Kind {
        TRANSIENT,
        TERMINAL,
        ROW_LEVEL
    }

    private static final Set<Status.Code> TRANSIENT_CODES =
            Collections.unmodifiableSet(
                    EnumSet.of(
                            Status.Code.UNAVAILABLE,
                            Status.Code.ABORTED,
                            Status.Code.INTERNAL,
                            Status.Code.CANCELLED,
                            Status.Code.DEADLINE_EXCEEDED,
                            Status.Code.RESOURCE_EXHAUSTED,
                            Status.Code.UNKNOWN));

    /** Storage error codes cured by rebuilding the destination's stream writer. */
    private static final Set<StorageError.StorageErrorCode> REFRESH_CODES =
            Collections.unmodifiableSet(
                    EnumSet.of(
                            StorageError.StorageErrorCode.STREAM_FINALIZED,
                            StorageError.StorageErrorCode.STREAM_NOT_FOUND,
                            StorageError.StorageErrorCode.INVALID_STREAM_STATE));

    private AppendErrorClassifier() {}

    /**
     * Classifies the given failure by walking its cause chain.
     *
     * @param t the failure
     * @return the error class
     */
    static Kind classify(Throwable t) {
        if (findRowLevel(t).isPresent()) {
            return Kind.ROW_LEVEL;
        }
        Status.Code code =
                ExceptionUtils.findThrowable(t, cause -> codeOf(cause) != null)
                        .map(AppendErrorClassifier::codeOf)
                        .orElse(null);
        return code != null && TRANSIENT_CODES.contains(code) ? Kind.TRANSIENT : Kind.TERMINAL;
    }

    /**
     * Finds a row-level append error (row indices to error messages) in the cause chain.
     *
     * <p>Matches the SDK's legacy-named {@link Exceptions.AppendSerializtionError} base class so
     * the correctly spelled {@code AppendSerializationError} subclass is covered too. An error
     * without row indices is not row-level — it falls through to status-code classification.
     *
     * @param t the failure
     * @return the row-level error, or empty
     */
    static Optional<Exceptions.AppendSerializtionError> findRowLevel(Throwable t) {
        return ExceptionUtils.findThrowable(t, Exceptions.AppendSerializtionError.class)
                .filter(e -> !e.getRowIndexToErrorMessage().isEmpty());
    }

    /**
     * Returns whether the failure is a schema mismatch: the appended rows carry fields the
     * destination table's schema (as cached by the Storage Write API backend) does not have.
     *
     * <p>Detected as the SDK's typed {@link Exceptions.SchemaMismatchedException} or as a {@link
     * StorageError} of code {@code SCHEMA_MISMATCH_EXTRA_FIELDS} attached to a gRPC status in the
     * cause chain.
     *
     * @param t the failure
     * @return whether the failure is a schema mismatch
     */
    static boolean isSchemaMismatch(Throwable t) {
        return ExceptionUtils.findThrowable(t, Exceptions.SchemaMismatchedException.class)
                        .isPresent()
                || hasStorageErrorCode(
                        t, EnumSet.of(StorageError.StorageErrorCode.SCHEMA_MISMATCH_EXTRA_FIELDS));
    }

    /**
     * Returns whether the failure reports the stream writer itself as stale — finalized, unknown to
     * the backend, in an invalid state, or closed client-side — and is cured by rebuilding the
     * destination's writer and re-appending, rather than being terminal.
     *
     * @param t the failure
     * @return whether rebuilding the stream writer repairs the failure
     */
    static boolean requiresWriterRefresh(Throwable t) {
        return ExceptionUtils.findThrowable(
                                t,
                                cause ->
                                        cause instanceof Exceptions.StreamFinalizedException
                                                || cause instanceof Exceptions.StreamNotFound
                                                || cause
                                                        instanceof
                                                        Exceptions.StreamWriterClosedException)
                        .isPresent()
                || hasStorageErrorCode(t, REFRESH_CODES);
    }

    /**
     * Converts a response-embedded error into the SDK's typed storage exception when it carries a
     * packed {@link StorageError} the SDK recognizes.
     *
     * @param error the error of a completed append response
     * @return the typed exception, or {@code null}
     */
    static Exceptions.StorageException toStorageException(com.google.rpc.Status error) {
        return Exceptions.toStorageException(error, null);
    }

    /** Whether the cause chain carries a packed {@link StorageError} with one of the codes. */
    private static boolean hasStorageErrorCode(
            Throwable t, Set<StorageError.StorageErrorCode> codes) {
        // StatusProto walks the cause chain to the nearest gRPC status trailers itself.
        com.google.rpc.Status status = StatusProto.fromThrowable(t);
        if (status == null) {
            return false;
        }
        for (Any detail : status.getDetailsList()) {
            if (detail.is(StorageError.class)) {
                try {
                    return codes.contains(detail.unpack(StorageError.class).getCode());
                } catch (InvalidProtocolBufferException e) {
                    return false;
                }
            }
        }
        return false;
    }

    /**
     * Returns whether the cause chain carries the given gRPC status code (from a gax or gRPC
     * exception).
     *
     * @param t the failure
     * @param code the status code to look for
     * @return whether the code is present in the chain
     */
    public static boolean hasCode(Throwable t, Status.Code code) {
        return ExceptionUtils.findThrowable(t, cause -> codeOf(cause) == code).isPresent();
    }

    /**
     * Returns whether the failure reports an append at an offset that already holds rows ({@link
     * Exceptions.OffsetAlreadyExists} or storage error {@code OFFSET_ALREADY_EXISTS}).
     *
     * <p>In the buffered-stream write path this is a success signal on retries (the original append
     * landed) and an abandon signal on the restore probe (the pre-crash attempt appended past the
     * restored offset).
     *
     * @param t the failure
     * @return whether the failure is an offset-already-exists error
     */
    public static boolean isOffsetAlreadyExists(Throwable t) {
        return ExceptionUtils.findThrowable(t, Exceptions.OffsetAlreadyExists.class).isPresent()
                || hasStorageErrorCode(
                        t, EnumSet.of(StorageError.StorageErrorCode.OFFSET_ALREADY_EXISTS));
    }

    /**
     * Returns whether the failure reports an append beyond the stream's current end ({@link
     * Exceptions.OffsetOutOfRange} or storage error {@code OFFSET_OUT_OF_RANGE}).
     *
     * @param t the failure
     * @return whether the failure is an offset-out-of-range error
     */
    public static boolean isOffsetOutOfRange(Throwable t) {
        return ExceptionUtils.findThrowable(t, Exceptions.OffsetOutOfRange.class).isPresent()
                || hasStorageErrorCode(
                        t, EnumSet.of(StorageError.StorageErrorCode.OFFSET_OUT_OF_RANGE));
    }

    /**
     * Returns whether a numeric gRPC status code (as embedded in {@code
     * AppendRowsResponse.getError()}) is transient. Unrecognized code values are not transient.
     *
     * @param codeValue the numeric gRPC status code
     * @return whether the code is transient
     */
    static boolean isTransientCode(int codeValue) {
        // fromCodeValue maps unrecognized values to UNKNOWN; the round-trip check keeps such
        // values out of the transient class.
        Status.Code code = Status.fromCodeValue(codeValue).getCode();
        return code.value() == codeValue && TRANSIENT_CODES.contains(code);
    }

    /** Returns the gRPC status code carried by a gax or gRPC exception, or {@code null}. */
    private static Status.Code codeOf(Throwable t) {
        if (t instanceof StatusRuntimeException) {
            return ((StatusRuntimeException) t).getStatus().getCode();
        }
        if (t instanceof ApiException) {
            try {
                return Status.Code.valueOf(((ApiException) t).getStatusCode().getCode().name());
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
        return null;
    }
}
