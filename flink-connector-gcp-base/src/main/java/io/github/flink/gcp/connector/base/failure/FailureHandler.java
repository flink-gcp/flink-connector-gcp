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

package io.github.flink.gcp.connector.base.failure;

import org.apache.flink.annotation.PublicEvolving;

import java.io.IOException;
import java.io.Serializable;

/**
 * Pluggable policy for elements that terminally fail to be written by a sink. Only data-shaped,
 * per-element failures reach the handler (each connector's documentation lists its set — for
 * BigQuery, explicit record-specific routing failures, rows rejected by the Storage Write API with
 * per-row error details, rows that fail serialization, and rows exceeding the per-row size limit).
 * Transient failures are retried by the sinks without involving the handler, and terminal
 * request-level failures such as {@code INVALID_ARGUMENT} always fail the job. ({@code
 * PERMISSION_DENIED} was the example here until BigQuery was measured to answer it for a table that
 * is merely missing, which its sink recovers from — a reminder that which codes are terminal is a
 * per-connector fact, not a general one.)
 *
 * <p>The sink writer drives the handler on the Flink task thread only, so implementations need not
 * be thread-safe:
 *
 * <ul>
 *   <li>{@link #open(FailureHandlerContext)} is called once, before the first {@link #handle}, when
 *       the sink writer is created (including after a restore);
 *   <li>{@link #handle(FailedElement)} accepts one terminally failed element — returning normally
 *       drops it, throwing fails the ongoing write or checkpoint (the built-in {@link #failJob()}
 *       policy, the default, does exactly that);
 *   <li>{@link #flush()} is called from the sink writer's own flush — at every checkpoint barrier
 *       and at end of input (and at any additional sink-triggered flush, such as an optional
 *       periodic flush interval), after the write path has drained. When it returns, every element
 *       handled so far must be durably persisted (a handler that persists nothing simply returns);
 *       throwing fails the ongoing checkpoint;
 *   <li>{@link #close()} is called when the sink writer closes, on success and failure paths alike.
 *       It releases resources and must not be relied on for persistence: on the failure path,
 *       elements handled since the last completed checkpoint may be lost with the writer — their
 *       originating records are replayed from the last checkpoint and handled again.
 * </ul>
 *
 * <p>Delivery of handled elements to an external destination is therefore <em>at-least-once, for
 * failures that recur on replay</em> — see {@link DeadLetterQueue} for the full statement.
 *
 * <p>What a successful checkpoint means under each policy, stated once for every connector: under
 * the default {@link #failJob()}, every record up to the barrier was written to the service or
 * skipped by a serializer returning {@code null}; under {@link #logAndDrop()} or {@link
 * #sendToDeadLetterQueue}, written, skipped, or handed to this handler. Each writer's javadoc says
 * which failures reach it.
 *
 * <p>That {@link #handle} drops by returning and fails the job by throwing is why the failure
 * classes a connector does <em>not</em> route matter as much as the ones it does: an outage the
 * client's retries gave up on is never routed, so no drop policy can quietly discard a backlog. And
 * in the mailbox-based writers a handler failing inside a completion callback cannot throw at its
 * caller — the writer captures the failure as its asynchronous error and rethrows it on the task
 * thread from the next write or flush.
 *
 * @param <F> the connector's concrete failure type
 */
@PublicEvolving
@FunctionalInterface
public interface FailureHandler<F extends FailedElement> extends Serializable {

    /**
     * Handles one terminally failed element. Returning normally drops the element; throwing fails
     * the ongoing write or checkpoint.
     *
     * @param element the failed element
     * @throws IOException to fail the job instead of dropping the element
     */
    void handle(F element) throws IOException;

    /**
     * Called once, before the first {@link #handle}, when the sink writer is created.
     *
     * @param context the writer's subtask index and metric group
     * @throws IOException if the handler cannot be opened; this fails the job
     */
    default void open(FailureHandlerContext context) throws IOException {}

    /**
     * Persists every element handled so far; called at every checkpoint barrier and at end of input
     * (and at any additional sink-triggered flush, such as an optional periodic flush interval),
     * after the sink's own write path has drained.
     *
     * @throws IOException if persistence fails; this fails the ongoing checkpoint
     */
    default void flush() throws IOException {}

    /** Releases resources held by the handler when the sink writer closes. */
    default void close() throws Exception {}

    /**
     * Returns the default policy: every per-element terminal failure fails the job.
     *
     * @param <F> the connector's concrete failure type
     * @return the fail-job handler
     */
    @SuppressWarnings("unchecked")
    static <F extends FailedElement> FailureHandler<F> failJob() {
        // Safe: the handler only consumes elements, and FailJob accepts any FailedElement.
        return (FailureHandler<F>) FailureHandlers.FailJob.INSTANCE;
    }

    /**
     * Returns the policy that logs each failed element at WARN level and drops it, letting the
     * pipeline continue.
     *
     * @param <F> the connector's concrete failure type
     * @return the log-and-drop handler
     */
    @SuppressWarnings("unchecked")
    static <F extends FailedElement> FailureHandler<F> logAndDrop() {
        // Safe: the handler only consumes elements, and LogAndDrop accepts any FailedElement.
        return (FailureHandler<F>) FailureHandlers.LogAndDrop.INSTANCE;
    }

    /**
     * Returns a policy that routes each failed element to the given dead-letter queue and drives
     * the queue's lifecycle ({@code open}/{@code flush}/{@code close}) from the handler's own; a
     * failure of the queue itself fails the job.
     *
     * @param <F> the connector's concrete failure type
     * @param deadLetterQueue the dead-letter queue
     * @return the dead-letter-queue handler
     */
    @SuppressWarnings("unchecked")
    static <F extends FailedElement> FailureHandler<F> sendToDeadLetterQueue(
            DeadLetterQueue deadLetterQueue) {
        // Safe: the handler only consumes elements, and the queue accepts any FailedElement.
        return (FailureHandler<F>) new FailureHandlers.SendToDeadLetterQueue(deadLetterQueue);
    }
}
