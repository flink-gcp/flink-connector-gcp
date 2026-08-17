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

package io.github.flink.gcp.connector.pubsub.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.ExceptionUtils;

import com.google.api.gax.rpc.StatusCode;
import io.github.flink.gcp.connector.base.rpc.StatusCodes;

import javax.annotation.Nullable;

import java.util.concurrent.CancellationException;

/**
 * Classifies failed Pub/Sub publishes into the classes the writer routes on.
 *
 * <ul>
 *   <li>{@link Kind#TOPIC_NOT_FOUND} — the destination topic does not exist. Repaired by creating
 *       it and republishing under {@code CreateDisposition.CREATE_IF_NEEDED}; terminal under {@code
 *       CREATE_NEVER}.
 *   <li>{@link Kind#CANCELLATION} — the SDK publisher cancelled a publish queued behind an earlier
 *       failure for the same ordering key. Never a root cause: it always trails another failure of
 *       the same key (see #78), so it is parked alongside that failure rather than classified on
 *       its own merits.
 *   <li>{@link Kind#MESSAGE_LEVEL} — the service rejected the publish as invalid ({@code
 *       INVALID_ARGUMENT}: over the size limit, malformed attributes, an unusable ordering key). A
 *       <em>candidate</em> per-message verdict, not a confirmed one: {@code Publish} is a batch RPC
 *       that rejects all-or-nothing, and the SDK sets the one request-level status on every
 *       co-batched future with nothing naming the offender (measured on real Pub/Sub, 2026-08-06,
 *       one run, #264). How the verdict is confirmed — and that only a message rejected on its own
 *       single-message request reaches the failure handler — is {@link TopicRepairer}'s isolation
 *       pass.
 *   <li>{@link Kind#FATAL} — everything else, including failures the SDK's own retries gave up on
 *       ({@code UNAVAILABLE}, {@code DEADLINE_EXCEEDED}, …), {@code PERMISSION_DENIED} and failures
 *       carrying no status at all. These fail the ongoing write or checkpoint.
 * </ul>
 *
 * <p>{@code MESSAGE_LEVEL} is deliberately {@code INVALID_ARGUMENT} alone. Outage-shaped failures
 * must never reach a dropping failure handler, or an outage would silently bleed the stream; the
 * class is widened only with evidence that a code identifies one message rather than a condition.
 *
 * <p>Every check walks the cause chain, because the SDK wraps the status-carrying exception. Order
 * is precedence: a {@code NOT_FOUND} chain that also carries a {@link CancellationException} is
 * still the repairable topic failure, and a cascade cancellation carries no status of its own.
 */
@Internal
final class PubSubErrorClassifier {

    /** The classes a failed publish falls into. */
    enum Kind {
        TOPIC_NOT_FOUND,
        CANCELLATION,
        MESSAGE_LEVEL,
        FATAL
    }

    /**
     * What {@link Kind#MESSAGE_LEVEL} means, for the message a routed failure carries. It lives
     * here rather than at the routing call site because it names the status code this class is
     * defined by: widening the class and leaving a stale reason behind elsewhere is then not
     * something a reader has to remember not to do.
     */
    static final String MESSAGE_LEVEL_REASON = "the message is invalid (INVALID_ARGUMENT)";

    private PubSubErrorClassifier() {}

    /**
     * Classifies a failed publish.
     *
     * @param throwable the failure reported by the publish future
     * @return the error class
     */
    static Kind classify(Throwable throwable) {
        if (hasCode(throwable, StatusCode.Code.NOT_FOUND)) {
            return Kind.TOPIC_NOT_FOUND;
        }
        if (ExceptionUtils.findThrowable(throwable, CancellationException.class).isPresent()) {
            return Kind.CANCELLATION;
        }
        if (hasCode(throwable, StatusCode.Code.INVALID_ARGUMENT)) {
            return Kind.MESSAGE_LEVEL;
        }
        return Kind.FATAL;
    }

    /**
     * Returns the status code the failure is counted under, or {@code null} when its chain carries
     * none — a raw {@link CancellationException}, or anything the client library surfaced without a
     * status. It lives here rather than at the metric call site because this class owns the
     * connector's cause-chain policy, exactly as {@code StatusCodes.codeOf} leaves that policy to
     * its callers.
     *
     * <p>The <b>outermost</b> classifiable element wins, which is not quite what {@link #classify}
     * does — that searches the whole chain for one specific code. A chain carrying two statuses
     * would therefore be counted under the outer one while being classified by the inner: the
     * metric answers "what did the publish fail with", not "which branch did the writer take". gax
     * surfaces one status per failure, so the two agree in practice, and the counter naming the
     * outermost status is the reading that survives if that ever stops being true.
     *
     * @param throwable the failure reported by the publish future
     * @return the status code, or {@code null}
     */
    @Nullable
    static StatusCode.Code statusCode(Throwable throwable) {
        return ExceptionUtils.findThrowable(throwable, t -> StatusCodes.codeOf(t) != null)
                .map(StatusCodes::codeOf)
                .orElse(null);
    }

    /**
     * Whether the cause chain carries the given status code — as the gax {@code ApiException} the
     * SDK publisher surfaces, or as a raw gRPC {@code StatusRuntimeException} (defense in depth),
     * both read through {@link StatusCodes#codeOf}.
     */
    private static boolean hasCode(Throwable throwable, StatusCode.Code code) {
        return ExceptionUtils.findThrowable(throwable, t -> StatusCodes.codeOf(t) == code)
                .isPresent();
    }
}
