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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.ExceptionUtils;

import com.google.api.gax.rpc.StatusCode;
import io.github.flink.gcp.connector.base.rpc.StatusCodes;

import javax.annotation.Nullable;

import java.util.EnumSet;
import java.util.Set;

/**
 * Classifies a failed call into the classes the writer acts on.
 *
 * <ul>
 *   <li>{@link Kind#TRANSIENT} — the service, not the request: {@code UNAVAILABLE}, {@code
 *       DEADLINE_EXCEEDED}, {@code ABORTED} and {@code RESOURCE_EXHAUSTED}, anywhere in the cause
 *       chain. The first three are what the client library itself retries a Datastore call on; the
 *       fourth names capacity or quota, and capacity is what the ramp-up throttle paces; an
 *       exhausted quota is not cured by retrying, so it fails the job once the recovery budget is
 *       spent. The writer retries these within that budget.
 *   <li>{@link Kind#CANDIDATE} — a status one mutation of the commit may have earned: {@code
 *       INVALID_ARGUMENT}, {@code ALREADY_EXISTS} and {@code NOT_FOUND}. A commit carries no
 *       per-mutation status, so the writer confirms which mutation earned it by re-sending each
 *       alone before anything is routed, and then routes it only for an operation that can earn it.
 *   <li>{@link Kind#FATAL} — everything else, including {@code INTERNAL}, {@code
 *       PERMISSION_DENIED}, {@code FAILED_PRECONDITION} and a failure carrying no status.
 * </ul>
 *
 * <p>The classification takes <b>both halves</b> of the cause chain, read differently on purpose,
 * as in the other connectors (ADR-0042): a transient status <em>anywhere</em> in the chain makes
 * the failure transient, so an unstable service can never produce a dead letter; otherwise the
 * chain's <em>first</em> classifiable status decides, so a data-shaped status buried under an
 * {@code INTERNAL} does not discard a record over a server-side failure.
 *
 * <p>The client library throws a {@code DatastoreException} whose cause is the gax {@code
 * ApiException} carrying the status, which {@link StatusCodes#codeOf} reads.
 */
@Internal
final class DatastoreErrorClassifier {

    /** The classes a failed call falls into. */
    enum Kind {
        TRANSIENT,
        CANDIDATE,
        FATAL
    }

    private static final Set<StatusCode.Code> TRANSIENT_CODES =
            EnumSet.of(
                    StatusCode.Code.UNAVAILABLE,
                    StatusCode.Code.DEADLINE_EXCEEDED,
                    StatusCode.Code.ABORTED,
                    StatusCode.Code.RESOURCE_EXHAUSTED);

    private static final Set<StatusCode.Code> CANDIDATE_CODES =
            EnumSet.of(
                    StatusCode.Code.INVALID_ARGUMENT,
                    StatusCode.Code.ALREADY_EXISTS,
                    StatusCode.Code.NOT_FOUND);

    private DatastoreErrorClassifier() {}

    /**
     * Classifies a failed call.
     *
     * @param throwable what the call threw
     * @return the class
     */
    static Kind classify(Throwable throwable) {
        if (firstMatching(throwable, TRANSIENT_CODES) != null) {
            return Kind.TRANSIENT;
        }
        StatusCode.Code code = statusCode(throwable);
        return code != null && CANDIDATE_CODES.contains(code) ? Kind.CANDIDATE : Kind.FATAL;
    }

    /**
     * Returns the status code a failure is <em>reported</em> under — the chain's outermost
     * classifiable status — or {@code null} when it carries none.
     *
     * @param throwable what the call threw
     * @return the outermost classifiable status, or {@code null}
     */
    @Nullable
    static StatusCode.Code statusCode(Throwable throwable) {
        return firstMatching(throwable, null);
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
                            StatusCode.Code code = StatusCodes.codeOf(t);
                            return code != null && (codes == null || codes.contains(code));
                        })
                .map(StatusCodes::codeOf)
                .orElse(null);
    }
}
