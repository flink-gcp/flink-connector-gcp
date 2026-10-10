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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.ExceptionUtils;

import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.firestore.FirestoreException;
import io.github.flink.gcp.connector.base.rpc.StatusCodes;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWrite;
import io.github.flink.gcp.connector.firestore.sink.PreconditionFailurePolicy;
import io.grpc.Status;

import javax.annotation.Nullable;

import java.util.EnumSet;
import java.util.Set;

/**
 * Classifies a write the client library gave up on into the classes the writer acts on.
 *
 * <ul>
 *   <li>{@link Kind#INVALID} — {@code INVALID_ARGUMENT}: a document over 1 MiB, a field name or
 *       value the service does not accept. gRPC defines it as "problematic regardless of the state
 *       of the system", and AIP-194 lists it as must-not-retry, so it may be routed. But the client
 *       library reports a failure of the whole {@code BatchWrite} request against every write it
 *       carried — including a request refused for its total size — so the status may answer the
 *       request rather than this write. The writer re-sends such a write alone before routing it
 *       (ADR-0045).
 *   <li>{@link Kind#ID_TAKEN} — {@code ALREADY_EXISTS} for a {@code CREATE} whose id the write drew
 *       ({@link FirestoreWrite#hasDrawnId()}): the id names an existing document, or the library
 *       retried a create that was applied but whose answer was lost. Not a failure of the record;
 *       the writer sends it again under a new id.
 *   <li>{@link Kind#REFUSED} — a refusal only this write can have earned, routed as it stands:
 *       {@code ALREADY_EXISTS} for any other {@code CREATE}, which is what a replayed create
 *       answers (ADR-0076), and {@code FAILED_PRECONDITION} for a write carrying a {@code
 *       lastUpdateTime} precondition, when {@link
 *       PreconditionFailurePolicy#ROUTE_TO_FAILURE_HANDLER} asks for it. Neither status is a
 *       request-level answer the service gives a {@code BatchWrite}.
 *   <li>{@link Kind#FATAL} — everything else, and both of those statuses on any other write. That
 *       includes {@code NOT_FOUND}, which a missing database and an update of a missing document
 *       share, so routing it would drop every record of a misconfigured job; {@code
 *       PERMISSION_DENIED}; statuses the client's retries gave up on; and failures carrying no
 *       status at all.
 * </ul>
 *
 * <p>Routing takes <b>both halves</b> of a condition, read from the cause chain differently on
 * purpose, as in the other connectors (ADR-0042): no transient status <em>anywhere</em> in the
 * chain, so an unstable service can never produce a dead letter; and the chain's <em>first</em>
 * classifiable status decides the rest, so a data-shaped status buried under an {@code INTERNAL}
 * does not discard a record over a server-side failure.
 *
 * <p>The client library reports a write it gave up on as a {@code BulkWriterException}, which is
 * neither a gax {@code ApiException} nor a gRPC {@code StatusRuntimeException}: it carries an
 * {@link Status io.grpc.Status}, which this class maps to the gax code of the same name.
 */
@Internal
final class FirestoreErrorClassifier {

    /** The classes a failed write falls into. */
    enum Kind {
        INVALID,
        ID_TAKEN,
        REFUSED,
        FATAL
    }

    /**
     * Statuses that mean the service, not the write; a chain carrying one is never data-shaped. The
     * first, third and fourth are what the client library retries a write on, and the second names
     * a slow service just as clearly.
     */
    private static final Set<StatusCode.Code> TRANSIENT_CODES =
            EnumSet.of(
                    StatusCode.Code.UNAVAILABLE,
                    StatusCode.Code.DEADLINE_EXCEEDED,
                    StatusCode.Code.ABORTED,
                    StatusCode.Code.RESOURCE_EXHAUSTED);

    private FirestoreErrorClassifier() {}

    /**
     * Classifies a failed write.
     *
     * @param throwable the failure the write's future reported
     * @param write the write that failed
     * @param policy what the sink does with a failed precondition
     * @return the class
     */
    static Kind classify(
            Throwable throwable, FirestoreWrite write, PreconditionFailurePolicy policy) {
        if (firstMatching(throwable, TRANSIENT_CODES) != null) {
            return Kind.FATAL;
        }
        StatusCode.Code code = statusCode(throwable);
        if (code == null) {
            return Kind.FATAL;
        }
        switch (code) {
            case INVALID_ARGUMENT:
                return Kind.INVALID;
            case ALREADY_EXISTS:
                if (write.getOperation() != FirestoreWrite.Operation.CREATE) {
                    return Kind.FATAL;
                }
                return write.hasDrawnId() ? Kind.ID_TAKEN : Kind.REFUSED;
            case FAILED_PRECONDITION:
                return write.getLastUpdateTime() != null
                                && policy == PreconditionFailurePolicy.ROUTE_TO_FAILURE_HANDLER
                        ? Kind.REFUSED
                        : Kind.FATAL;
            default:
                return Kind.FATAL;
        }
    }

    /**
     * Returns the status code a failed write is <em>reported</em> under — the chain's outermost
     * classifiable status — or {@code null} when it carries none.
     *
     * @param throwable the failure the write's future reported
     * @return the outermost classifiable status, or {@code null}
     */
    @Nullable
    static StatusCode.Code statusCode(Throwable throwable) {
        return firstMatching(throwable, null);
    }

    /**
     * Returns the status one throwable itself carries, or {@code null}: a Firestore exception's
     * gRPC status, or whatever {@link StatusCodes#codeOf} reads from a gax or gRPC exception.
     */
    @Nullable
    static StatusCode.Code codeOf(Throwable throwable) {
        if (throwable instanceof FirestoreException) {
            Status status = ((FirestoreException) throwable).getStatus();
            if (status != null) {
                return toGax(status.getCode());
            }
        }
        return StatusCodes.codeOf(throwable);
    }

    /**
     * Maps a gRPC status code to the gax one of the same name, or {@code null} for a name gax does
     * not know — which reads as unclassifiable, and so fatal, rather than crashing the
     * classification. {@code FirestoreErrorClassifierTest} maps every gRPC code, so a code gax
     * lacks fails a test rather than a job.
     */
    @Nullable
    static StatusCode.Code toGax(Status.Code code) {
        try {
            return StatusCode.Code.valueOf(code.name());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Returns the first status code in the cause chain that is one of {@code codes}, or {@code
     * null} when the chain carries none; a null {@code codes} accepts any classifiable status.
     */
    @Nullable
    private static StatusCode.Code firstMatching(
            Throwable throwable, @Nullable Set<StatusCode.Code> codes) {
        return ExceptionUtils.findThrowable(
                        throwable,
                        t -> {
                            StatusCode.Code code = codeOf(t);
                            return code != null && (codes == null || codes.contains(code));
                        })
                .map(FirestoreErrorClassifier::codeOf)
                .orElse(null);
    }
}
