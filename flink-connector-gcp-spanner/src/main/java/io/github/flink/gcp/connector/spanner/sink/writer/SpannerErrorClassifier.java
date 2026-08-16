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

package io.github.flink.gcp.connector.spanner.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.ExceptionUtils;

import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import io.github.flink.gcp.connector.base.rpc.StatusCodes;
import io.github.flink.gcp.connector.spanner.sink.ConstraintViolationPolicy;

import javax.annotation.Nullable;

import java.util.EnumSet;
import java.util.Set;

/**
 * Classifies the two kinds of failure a batch write reports: a status for one mutation group, and a
 * failure of the request as a whole.
 *
 * <ul>
 *   <li>{@link Kind#ROW_LEVEL} — the service refused this one mutation and would refuse it again.
 *       Routed to the configured failure handler; the rest of the batch is unaffected, which is
 *       what {@code batchWriteAtLeastOnce} reporting per group buys over a plain commit.
 *   <li>{@link Kind#TRANSIENT} — the service, not the mutation. Re-sent on the writer's retry
 *       schedule, and the job fails only once the budget is spent.
 *   <li>{@link Kind#FATAL} — everything else, including every status carrying no classifiable code.
 *       Fails the job.
 * </ul>
 *
 * <p><b>Only a status that is unrecoverable by definition may be routed</b>, because the handler
 * may drop what it is given and a dropping policy must never turn an unstable service into silent
 * data loss. Two statuses qualify:
 *
 * <ul>
 *   <li>{@code INVALID_ARGUMENT} — gRPC defines it as "problematic regardless of the state of the
 *       system", and AIP-194 lists it as must-not-retry. This is the same status, on the same
 *       reasoning, that the Bigtable sink routes.
 *   <li>{@code ALREADY_EXISTS} — an {@code insert} whose row is already there. It <em>is</em>
 *       state-dependent, and that is precisely why routing it is right rather than despite it: the
 *       state it depends on is this mutation's own earlier application, so the row the record
 *       describes is in the database either way. It is the expected outcome of replaying a
 *       non-idempotent mutation, which the sink's at-least-once guarantee makes a normal event
 *       rather than an anomaly.
 * </ul>
 *
 * <p>{@code FAILED_PRECONDITION} and {@code OUT_OF_RANGE} — between them, every constraint
 * violation Spanner reports — are the statuses this class does not decide alone. By default they
 * fail the job, because a constraint violation usually says the mapping from records to columns is
 * wrong rather than that one record is anomalous, and shedding such records one at a time hides a
 * systematic problem behind a green job. There is also one documented condition that produces
 * {@code FAILED_PRECONDITION} on every write and clears on its own — an unreachable CMEK key — and
 * routing that by default would drain a whole stream into a dead-letter queue during a key
 * incident. A pipeline whose stream genuinely carries occasional records the schema will not accept
 * can say so with {@link ConstraintViolationPolicy#ROUTE_TO_FAILURE_HANDLER}, after which the
 * configured failure handler decides. The choice is the job's; see that enum.
 *
 * <p>{@code NOT_FOUND} is not routed either: a missing table or column fails every record alike, so
 * it is a configuration failure rather than a bad row, and this sink creates nothing.
 */
@Internal
final class SpannerErrorClassifier {

    /** The classes a failed mutation or request falls into. */
    enum Kind {
        ROW_LEVEL,
        TRANSIENT,
        FATAL
    }

    /**
     * Statuses that mean the service, not the mutation.
     *
     * <p>{@code ABORTED} is Spanner's own contention signal — the emulator's "only one transaction
     * at a time" refusal arrives under it too. {@code RESOURCE_EXHAUSTED} is here because the
     * client library itself classifies it as retryable for {@code Commit} and {@code ExecuteSql}
     * (its generated {@code retry_policy_3_codes}, checked against google-cloud-spanner 6.119.0);
     * it names an instance at capacity, which passes.
     *
     * <p>{@code INTERNAL} is not here. gRPC reserves it for broken invariants, AIP-194 does not
     * list it as retryable, and the client library does not retry it either — retrying it would
     * spend the whole budget on something that will not clear.
     */
    private static final Set<StatusCode.Code> TRANSIENT_CODES =
            EnumSet.of(
                    StatusCode.Code.ABORTED,
                    StatusCode.Code.UNAVAILABLE,
                    StatusCode.Code.DEADLINE_EXCEEDED,
                    StatusCode.Code.RESOURCE_EXHAUSTED);

    /** Statuses that mean this mutation, and would mean it again. */
    private static final Set<StatusCode.Code> ROW_LEVEL_CODES =
            EnumSet.of(StatusCode.Code.INVALID_ARGUMENT, StatusCode.Code.ALREADY_EXISTS);

    /**
     * The statuses Spanner refuses a constraint violation with. Measured 2026-08-09, one run,
     * emulator v1.5.56: a {@code NULL} in a {@code NOT NULL} column, an over-long value and a
     * foreign-key violation answer {@code FAILED_PRECONDITION}; a {@code CHECK} constraint answers
     * {@code OUT_OF_RANGE}. Both are here because a policy named after constraint violations that
     * covered only one of them would be a lie the first time someone added a {@code CHECK}.
     *
     * <p>Whether they join {@link #ROW_LEVEL_CODES} is the job's decision rather than this class's,
     * because both readings of such a refusal are defensible; see {@link
     * ConstraintViolationPolicy}.
     */
    private static final Set<StatusCode.Code> CONSTRAINT_VIOLATION_CODES =
            EnumSet.of(StatusCode.Code.FAILED_PRECONDITION, StatusCode.Code.OUT_OF_RANGE);

    private SpannerErrorClassifier() {}

    /**
     * Classifies one mutation group's status.
     *
     * <p>A group status is a single code with no cause chain behind it, so this is a lookup rather
     * than the precedence {@link #classify(Throwable)} has to apply.
     *
     * @param code the group's status code, never {@code OK}
     * @return the error class
     */
    static Kind classify(@Nullable StatusCode.Code code, ConstraintViolationPolicy policy) {
        if (code == null) {
            return Kind.FATAL;
        }
        if (TRANSIENT_CODES.contains(code)) {
            return Kind.TRANSIENT;
        }
        if (ROW_LEVEL_CODES.contains(code)) {
            return Kind.ROW_LEVEL;
        }
        if (CONSTRAINT_VIOLATION_CODES.contains(code)
                && policy == ConstraintViolationPolicy.ROUTE_TO_FAILURE_HANDLER) {
            return Kind.ROW_LEVEL;
        }
        return Kind.FATAL;
    }

    /**
     * Classifies a failure of the batch write request as a whole.
     *
     * <p>Never {@link Kind#ROW_LEVEL}: a request-level failure says nothing about which mutation is
     * at fault, and the mutations it carried have no reported outcome — dropping all of them over
     * one status would discard records the service may simply not have looked at yet. So this
     * answers only "retry" or "fail the job".
     *
     * <p>Transient wins over anything in front of it in the cause chain, so an unstable service can
     * never present as terminal because a wrapper exception sat on top of it.
     *
     * @param throwable the failure the request threw
     * @return {@link Kind#TRANSIENT} or {@link Kind#FATAL}
     */
    static Kind classify(Throwable throwable) {
        return firstMatching(throwable, TRANSIENT_CODES) != null ? Kind.TRANSIENT : Kind.FATAL;
    }

    /**
     * Returns the status code a failure is <em>reported</em> under — the chain's outermost
     * classifiable status — or {@code null} when it carries none. Feeds the error-class counter,
     * which is read to learn what the service said rather than what the writer decided.
     *
     * @param throwable the failure
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
                            StatusCode.Code code = codeOf(t);
                            return code != null && (codes == null || codes.contains(code));
                        })
                .map(SpannerErrorClassifier::codeOf)
                .orElse(null);
    }

    /**
     * Returns the status code of one throwable, or {@code null} when it carries none.
     *
     * <p>{@link StatusCodes#codeOf} sees a gax {@code ApiException} and a raw gRPC {@code
     * StatusRuntimeException}; the Spanner client wraps both in a {@link SpannerException}, which
     * carries the code on itself and does not always keep the original in its cause chain — so that
     * case is read directly rather than hoped for.
     */
    @Nullable
    private static StatusCode.Code codeOf(Throwable throwable) {
        if (throwable instanceof SpannerException) {
            return fromErrorCode(((SpannerException) throwable).getErrorCode());
        }
        return StatusCodes.codeOf(throwable);
    }

    /**
     * Maps the client library's error code onto the gax code the rest of this project classifies
     * on.
     *
     * <p>Written out rather than matched by enum name so that a code added to the client library
     * fails {@code SpannerErrorClassifierTest} instead of silently becoming unclassifiable — which
     * would read as {@link Kind#FATAL} and could hide a retryable status.
     */
    @Nullable
    static StatusCode.Code fromErrorCode(ErrorCode errorCode) {
        switch (errorCode) {
            case CANCELLED:
                return StatusCode.Code.CANCELLED;
            case UNKNOWN:
                return StatusCode.Code.UNKNOWN;
            case INVALID_ARGUMENT:
                return StatusCode.Code.INVALID_ARGUMENT;
            case DEADLINE_EXCEEDED:
                return StatusCode.Code.DEADLINE_EXCEEDED;
            case NOT_FOUND:
                return StatusCode.Code.NOT_FOUND;
            case ALREADY_EXISTS:
                return StatusCode.Code.ALREADY_EXISTS;
            case PERMISSION_DENIED:
                return StatusCode.Code.PERMISSION_DENIED;
            case UNAUTHENTICATED:
                return StatusCode.Code.UNAUTHENTICATED;
            case RESOURCE_EXHAUSTED:
                return StatusCode.Code.RESOURCE_EXHAUSTED;
            case FAILED_PRECONDITION:
                return StatusCode.Code.FAILED_PRECONDITION;
            case ABORTED:
                return StatusCode.Code.ABORTED;
            case OUT_OF_RANGE:
                return StatusCode.Code.OUT_OF_RANGE;
            case UNIMPLEMENTED:
                return StatusCode.Code.UNIMPLEMENTED;
            case INTERNAL:
                return StatusCode.Code.INTERNAL;
            case UNAVAILABLE:
                return StatusCode.Code.UNAVAILABLE;
            case DATA_LOSS:
                return StatusCode.Code.DATA_LOSS;
            default:
                return null;
        }
    }

    /**
     * Maps a canonical gRPC status number — what a {@code BatchWriteResponse} reports per group —
     * onto the gax code, or {@code null} for {@code OK} and for a number no code is known for.
     *
     * @param canonicalCode the {@code google.rpc.Code} number
     * @return the code, or {@code null}
     */
    @Nullable
    static StatusCode.Code fromCanonicalCode(int canonicalCode) {
        switch (canonicalCode) {
            case 1:
                return StatusCode.Code.CANCELLED;
            case 2:
                return StatusCode.Code.UNKNOWN;
            case 3:
                return StatusCode.Code.INVALID_ARGUMENT;
            case 4:
                return StatusCode.Code.DEADLINE_EXCEEDED;
            case 5:
                return StatusCode.Code.NOT_FOUND;
            case 6:
                return StatusCode.Code.ALREADY_EXISTS;
            case 7:
                return StatusCode.Code.PERMISSION_DENIED;
            case 8:
                return StatusCode.Code.RESOURCE_EXHAUSTED;
            case 9:
                return StatusCode.Code.FAILED_PRECONDITION;
            case 10:
                return StatusCode.Code.ABORTED;
            case 11:
                return StatusCode.Code.OUT_OF_RANGE;
            case 12:
                return StatusCode.Code.UNIMPLEMENTED;
            case 13:
                return StatusCode.Code.INTERNAL;
            case 14:
                return StatusCode.Code.UNAVAILABLE;
            case 15:
                return StatusCode.Code.DATA_LOSS;
            case 16:
                return StatusCode.Code.UNAUTHENTICATED;
            default:
                // 0 is OK, which never reaches a classification; anything else is a code this
                // client library does not know either.
                return null;
        }
    }
}
