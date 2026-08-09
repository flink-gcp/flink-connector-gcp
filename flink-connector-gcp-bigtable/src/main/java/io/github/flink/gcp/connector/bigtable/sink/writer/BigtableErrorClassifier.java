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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.ExceptionUtils;

import com.google.api.gax.rpc.StatusCode;
import io.github.flink.gcp.connector.base.rpc.StatusCodes;

import javax.annotation.Nullable;

import java.util.EnumSet;
import java.util.Set;

/**
 * Classifies failed row mutations into the classes the writer routes on.
 *
 * <ul>
 *   <li>{@link Kind#TABLE_NOT_FOUND} — the table or one of its column families does not exist
 *       ({@code NOT_FOUND}). Checked <em>first</em>, ahead of the transient precedence: a {@code
 *       NOT_FOUND} chain that also carries a transient status is still the missing-table failure,
 *       and acting on it is safe where a drop would not be — the writer either repairs it by
 *       idempotent creation and re-applies the mutation (under {@code CREATE_IF_NEEDED}), or fails
 *       the job (under {@code CREATE_NEVER}, since a missing table fails every record alike);
 *       nothing is discarded either way. {@code PubSubErrorClassifier} orders its {@code
 *       TOPIC_NOT_FOUND} the same way.
 *   <li>{@link Kind#ROW_LEVEL} — the service rejected this mutation as invalid ({@code
 *       INVALID_ARGUMENT}: over the size limit for a cell or a row, more mutations than one row
 *       accepts, a malformed qualifier). Applying the same mutation again cannot succeed and the
 *       other entries of its batch are unaffected, so it is routed to the configured failure
 *       handler.
 *   <li>{@link Kind#FATAL} — everything else. That includes {@code PERMISSION_DENIED} and {@code
 *       UNAUTHENTICATED}, which are configuration-shaped and would fail every record alike;
 *       failures the client's own per-entry retries gave up on ({@code UNAVAILABLE}, {@code
 *       DEADLINE_EXCEEDED}, {@code ABORTED}, {@code RESOURCE_EXHAUSTED}); and failures carrying no
 *       status at all. These fail the ongoing write or checkpoint.
 * </ul>
 *
 * <p><b>Only a status that is unrecoverable by definition may be routed</b>, because the handler
 * may drop what it is given and a dropping policy must never turn an unstable service into silent
 * data loss. {@code INVALID_ARGUMENT} qualifies on gRPC's own definition — <em>"problematic
 * regardless of the state of the system"</em> — and AIP-194 lists it as must-not-retry. {@code
 * FAILED_PRECONDITION} does not, and is deliberately <em>not</em> routed despite naming failures
 * that look data-shaped: gRPC defines it as the system not being in the required state, which is
 * exactly the state-dependence that makes a drop unsafe.
 *
 * <p>Routing therefore takes <b>both halves</b> of a condition, and the two halves read the cause
 * chain differently on purpose:
 *
 * <ul>
 *   <li>no transient status <em>anywhere</em> in the chain — so an unstable service can never
 *       produce a dead letter even when a data-shaped status sits in front of it. That is a
 *       property of this code rather than of the client happening to surface one status per
 *       failure;
 *   <li>and the chain's <em>first</em> classifiable status is {@code INVALID_ARGUMENT} — an {@code
 *       INVALID_ARGUMENT} buried under an {@code INTERNAL} or an {@code UNKNOWN} describes the
 *       inner call, and dropping the mutation over it would discard a record over a server-side
 *       failure. The two mistakes are mirror images.
 * </ul>
 */
@Internal
final class BigtableErrorClassifier {

    /** The classes a failed mutation falls into. */
    enum Kind {
        TABLE_NOT_FOUND,
        ROW_LEVEL,
        FATAL
    }

    /**
     * Statuses that mean the service, not the mutation; a chain carrying one is never data-shaped.
     * The first two are what the client itself retries {@code MutateRows} on, and the other two
     * name an overloaded or contended service just as clearly.
     */
    private static final Set<StatusCode.Code> TRANSIENT_CODES =
            EnumSet.of(
                    StatusCode.Code.UNAVAILABLE,
                    StatusCode.Code.DEADLINE_EXCEEDED,
                    StatusCode.Code.ABORTED,
                    StatusCode.Code.RESOURCE_EXHAUSTED);

    /**
     * What {@link Kind#ROW_LEVEL} means, for the message a routed failure carries. It lives here
     * rather than at the routing call site because it names the status this class is defined by, so
     * widening the class cannot leave a stale reason behind elsewhere.
     */
    static final String ROW_LEVEL_REASON = "the mutation is invalid (INVALID_ARGUMENT)";

    private BigtableErrorClassifier() {}

    /**
     * Classifies a failed mutation.
     *
     * @param throwable the failure reported by the mutation's future
     * @return the error class
     */
    static Kind classify(Throwable throwable) {
        if (firstMatching(throwable, EnumSet.of(StatusCode.Code.NOT_FOUND)) != null) {
            return Kind.TABLE_NOT_FOUND;
        }
        if (firstMatching(throwable, TRANSIENT_CODES) != null) {
            return Kind.FATAL;
        }
        return firstMatching(throwable, null) == StatusCode.Code.INVALID_ARGUMENT
                ? Kind.ROW_LEVEL
                : Kind.FATAL;
    }

    /**
     * Returns the status code a failed mutation is <em>reported</em> under — the chain's outermost
     * classifiable status — or {@code null} when it carries none.
     *
     * <p>Deliberately not the code {@link #classify} acted on. That is a precedence over the whole
     * chain, so a transient status buried under a data-shaped one decides the routing while this
     * returns the outer one; reporting the routing decision instead would answer "why was this not
     * dropped" rather than "what did it fail with", and the latter is what an error-class counter
     * is read for. {@code PubSubErrorClassifier.statusCode} has the same shape and the same
     * divergence.
     *
     * @param throwable the failure reported by the mutation's future
     * @return the outermost classifiable status, or {@code null}
     */
    @Nullable
    static StatusCode.Code statusCode(Throwable throwable) {
        return firstMatching(throwable, null);
    }

    /**
     * Returns the first status code in the cause chain that is one of {@code codes}, or {@code
     * null} when the chain carries none; a null {@code codes} accepts any classifiable status.
     * Statuses are read through {@link StatusCodes#codeOf}, so a gax {@code ApiException} and a raw
     * gRPC {@code StatusRuntimeException} (defense in depth) are both seen.
     *
     * <p>Searching the chain for a <em>specific</em> set is what makes {@link #classify} a
     * precedence rather than a first-match.
     */
    @Nullable
    private static StatusCode.Code firstMatching(
            Throwable throwable, @Nullable Set<StatusCode.Code> codes) {
        return ExceptionUtils.findThrowable(
                        throwable,
                        t -> {
                            StatusCode.Code code = StatusCodes.codeOf(t);
                            return code != null && (codes == null || codes.contains(code));
                        })
                .map(StatusCodes::codeOf)
                .orElse(null);
    }
}
