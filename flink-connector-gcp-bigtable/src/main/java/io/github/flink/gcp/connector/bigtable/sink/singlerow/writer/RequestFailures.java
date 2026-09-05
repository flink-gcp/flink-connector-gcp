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

package io.github.flink.gcp.connector.bigtable.sink.singlerow.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.ExceptionUtils;

import com.google.api.gax.rpc.StatusCode;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.RowOperation;
import io.github.flink.gcp.connector.bigtable.sink.writer.BigtableErrorClassifier;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.CancellationException;

/**
 * The failure boundary of a single-row request: which failures concern the row alone, which leave
 * the service's state unknown, and what each is reported as.
 *
 * <ul>
 *   <li>{@link Kind#ROW_LEVEL} — {@code INVALID_ARGUMENT}, with no ambiguous status anywhere in the
 *       chain: the service rejected this request as malformed, nothing was applied, and repeating
 *       it cannot succeed. The sink surface routes it to the failure handler.
 *   <li>{@link Kind#AMBIGUOUS} — {@code DEADLINE_EXCEEDED}, {@code UNAVAILABLE}, {@code ABORTED} or
 *       {@code CANCELLED} anywhere in the chain, or a cancelled future: the call ended before the
 *       service's answer arrived, so the service may or may not have applied the request. Unlike
 *       the batching sink, where the client's own retries make such a failure merely fatal, a
 *       request-response RPC has one attempt and no retry — a retry could apply an increment twice
 *       — so the report says what is unknown and what a replay will do about it.
 *   <li>{@link Kind#FATAL} — everything else: {@code NOT_FOUND} (the table or one of its families
 *       does not exist), the configuration-shaped statuses, {@code RESOURCE_EXHAUSTED}, failures
 *       carrying no status. The job fails.
 * </ul>
 *
 * <p>The two halves of the batching sink's rule hold here too (ADR-0042): only a status that is
 * unrecoverable by definition is routed, and an unstable service must never produce a dead letter.
 * The chain is read through {@link BigtableErrorClassifier}, so the two families cannot drift in
 * how they see a status; this class only draws the ambiguity line the batching sink has no need
 * for.
 */
@Internal
public final class RequestFailures {

    /** The classes a failed request falls into. */
    public enum Kind {
        ROW_LEVEL,
        AMBIGUOUS,
        FATAL
    }

    /**
     * Statuses after which the service's state is unknown: the deadline passed, the channel broke,
     * the service aborted a contended transaction, or the call was cancelled. Any of them may have
     * arrived after the service applied the request.
     */
    private static final Set<StatusCode.Code> AMBIGUOUS_CODES =
            EnumSet.of(
                    StatusCode.Code.DEADLINE_EXCEEDED,
                    StatusCode.Code.UNAVAILABLE,
                    StatusCode.Code.ABORTED,
                    StatusCode.Code.CANCELLED);

    /** What {@link Kind#ROW_LEVEL} means, for the message a routed failure carries. */
    static final String ROW_LEVEL_REASON = "the request is invalid (INVALID_ARGUMENT)";

    private RequestFailures() {}

    /**
     * Classifies a failed request.
     *
     * @param throwable the failure reported by the request's future
     * @return the class
     */
    public static Kind classify(Throwable throwable) {
        BigtableErrorClassifier.Kind kind = BigtableErrorClassifier.classify(throwable);
        if (kind == BigtableErrorClassifier.Kind.TABLE_NOT_FOUND) {
            // Ahead of the ambiguity check, as the classifier orders it: a NOT_FOUND names a
            // request the service did not apply, whatever else the chain carries.
            return Kind.FATAL;
        }
        if (isAmbiguous(throwable)) {
            return Kind.AMBIGUOUS;
        }
        return kind == BigtableErrorClassifier.Kind.ROW_LEVEL ? Kind.ROW_LEVEL : Kind.FATAL;
    }

    /**
     * Returns whether the failure is the request's deadline passing — what {@code requestsTimedOut}
     * counts, on top of {@code requestsFailed}.
     *
     * @param throwable the failure reported by the request's future
     * @return whether the chain carries {@code DEADLINE_EXCEEDED}
     */
    public static boolean isTimeout(Throwable throwable) {
        return BigtableErrorClassifier.carriesAny(
                throwable, EnumSet.of(StatusCode.Code.DEADLINE_EXCEEDED));
    }

    /**
     * Returns the status the failure is reported under, for the error-class counters.
     *
     * @param throwable the failure reported by the request's future
     * @return the outermost classifiable status, or {@code null}
     */
    @Nullable
    public static StatusCode.Code statusCode(Throwable throwable) {
        return BigtableErrorClassifier.statusCode(throwable);
    }

    private static boolean isAmbiguous(Throwable throwable) {
        return BigtableErrorClassifier.carriesAny(throwable, AMBIGUOUS_CODES)
                || ExceptionUtils.findThrowable(throwable, CancellationException.class).isPresent();
    }

    /**
     * Wraps a failure that fails the job — an ambiguous or a fatal one — in the message its class
     * calls for.
     *
     * @param kind the class, {@link Kind#AMBIGUOUS} or {@link Kind#FATAL}
     * @param operation the RPC
     * @param destination the table
     * @param throwable the failure
     * @return the exception to fail the job with
     */
    public static IOException jobFailure(
            Kind kind, RowOperation operation, TableDestination destination, Throwable throwable) {
        String head = "A " + operation.getRpcName() + " request to Bigtable table " + destination;
        if (kind == Kind.AMBIGUOUS) {
            return new IOException(
                    head
                            + " failed with "
                            + describeStatus(throwable)
                            + " before the service answered, so the service may or may not have"
                            + " applied it. The job fails and the record is replayed from the last"
                            + " completed checkpoint (at-least-once). A replayed CheckAndMutateRow"
                            + " re-evaluates its condition against whatever state the first"
                            + " attempt left; ReadModifyWriteRow is not idempotent, so a replay of"
                            + " an applied increment or append applies it again.",
                    throwable);
        }
        String reason =
                BigtableErrorClassifier.classify(throwable)
                                == BigtableErrorClassifier.Kind.TABLE_NOT_FOUND
                        ? " because the table or one of its column families does not exist"
                        : "";
        return new IOException(head + " failed" + reason + "." + routingHint(operation), throwable);
    }

    static String routingHint(RowOperation operation) {
        return operation == RowOperation.CHECK_AND_MUTATE_ROW
                ? " CheckAndMutateRow requires an app profile with single-cluster routing and single-row transactions enabled; inspect the service cause for the rejection reason."
                : "";
    }

    private static String describeStatus(Throwable throwable) {
        StatusCode.Code code = statusCode(throwable);
        return code == null ? "a cancellation" : code.name();
    }
}
