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

/**
 * Classifies failed row mutations into the classes the writer routes on.
 *
 * <ul>
 *   <li>{@link Kind#ROW_LEVEL} — the service rejected this mutation as invalid ({@code
 *       INVALID_ARGUMENT}, {@code FAILED_PRECONDITION}: over the size limit for a cell or a row,
 *       more mutations than a row accepts, a timestamp the table's granularity does not allow).
 *       Applying the same mutation again cannot succeed and the other entries of its batch are
 *       unaffected, so it is routed to the configured failure handler.
 *   <li>{@link Kind#FATAL} — everything else. That includes {@code NOT_FOUND} (a missing table or
 *       column family), {@code PERMISSION_DENIED} and {@code UNAUTHENTICATED}, which are
 *       configuration-shaped and would fail every record alike; failures the client's own per-entry
 *       retries gave up on ({@code UNAVAILABLE}, {@code DEADLINE_EXCEEDED}, {@code ABORTED}, {@code
 *       RESOURCE_EXHAUSTED}); and failures carrying no status at all. These fail the ongoing write
 *       or checkpoint.
 * </ul>
 *
 * <p>The split's purpose is that a dropping failure handler never sees a condition. An outage would
 * otherwise bleed the stream one mutation at a time instead of backpressuring it, and a missing
 * column family would empty the whole stream into the dead-letter destination under a green job.
 *
 * <p>The cause chain is walked, because the client wraps the status-carrying exception.
 */
@Internal
final class BigtableErrorClassifier {

    /** The classes a failed mutation falls into. */
    enum Kind {
        ROW_LEVEL,
        FATAL
    }

    /**
     * What {@link Kind#ROW_LEVEL} means, for the message a routed failure carries. It lives here
     * rather than at the routing call site because it names the status codes this class is defined
     * by, so widening the class cannot leave a stale reason behind elsewhere.
     */
    static final String ROW_LEVEL_REASON =
            "the mutation is invalid (INVALID_ARGUMENT or FAILED_PRECONDITION)";

    private BigtableErrorClassifier() {}

    /**
     * Classifies a failed mutation.
     *
     * @param throwable the failure reported by the mutation's future
     * @return the error class
     */
    static Kind classify(Throwable throwable) {
        if (hasCode(throwable, StatusCode.Code.INVALID_ARGUMENT)
                || hasCode(throwable, StatusCode.Code.FAILED_PRECONDITION)) {
            return Kind.ROW_LEVEL;
        }
        return Kind.FATAL;
    }

    /**
     * Whether the cause chain carries the given status code — as the gax {@code ApiException} the
     * client surfaces, or as a raw gRPC {@code StatusRuntimeException} (defense in depth), both
     * read through {@link StatusCodes#codeOf}.
     */
    private static boolean hasCode(Throwable throwable, StatusCode.Code code) {
        return ExceptionUtils.findThrowable(throwable, t -> StatusCodes.codeOf(t) == code)
                .isPresent();
    }
}
