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

import org.apache.flink.annotation.Experimental;

import java.io.IOException;
import java.io.Serializable;

/**
 * Destination for elements that terminally failed to be written by a sink, used through {@link
 * FailureHandler#sendToDeadLetterQueue(DeadLetterQueue)}. One implementation serves every
 * connector: it sees failures through the shared {@link FailedElement} contract.
 *
 * <p><b>Lifecycle.</b> The {@code sendToDeadLetterQueue} handler drives the queue from the sink
 * writer's own lifecycle, on the Flink task thread only, so implementations need not be
 * thread-safe:
 *
 * <ul>
 *   <li>{@link #open(FailureHandlerContext)} is called once, before the first {@link #offer}, when
 *       the sink writer is created (including after a restore). The context carries the writer's
 *       metric group and subtask index.
 *   <li>{@link #offer(FailedElement)} accepts one terminally failed element. Implementations may
 *       buffer; they need not write durably here.
 *   <li>{@link #flush()} is called from the sink writer's own flush — at every checkpoint barrier
 *       and at end of input (and at any additional sink-triggered flush, such as an optional
 *       periodic flush interval). When it returns, every element offered so far must be durably
 *       persisted; throwing fails the ongoing checkpoint and thereby the job.
 *   <li>{@link #close()} is called when the sink writer closes, on success and failure paths alike.
 *       It releases resources and must not be relied on for persistence: on the failure path,
 *       elements offered since the last completed checkpoint may be lost with the writer — their
 *       originating records are replayed from the last checkpoint and offered again.
 * </ul>
 *
 * <p><b>Delivery guarantee: at-least-once, for failures that recur on replay.</b> Elements are
 * offered before the checkpoint covering their originating records completes, so a restart replays
 * those records and a deterministic failure (malformed data, an oversized payload) is offered again
 * — dead-letter output can therefore contain duplicates and should be consumed idempotently or
 * deduplicated by key. A failure that does <em>not</em> recur on replay is preserved only if it was
 * already flushed by a completed checkpoint or written through synchronously; an implementation
 * that writes through on every {@code offer} narrows the window at the cost of per-element latency.
 * Exactly-once dead-letter output is deliberately not offered: it would require the queue write to
 * join the sink's own commit protocol, which none of these services can enroll an external write
 * in.
 */
@Experimental
public interface DeadLetterQueue extends Serializable {

    /**
     * Accepts one terminally failed element; implementations may buffer until {@link #flush()}.
     *
     * @param element the failed element
     * @throws IOException if the element cannot be accepted; this fails the job
     */
    void offer(FailedElement element) throws IOException;

    /**
     * Called once, before the first {@link #offer}, when the sink writer is created.
     *
     * @param context the writer's subtask index and metric group
     * @throws IOException if the queue cannot be opened; this fails the job
     */
    default void open(FailureHandlerContext context) throws IOException {}

    /**
     * Durably persists every element offered so far; called at every checkpoint barrier and at end
     * of input (and at any additional sink-triggered flush, such as an optional periodic flush
     * interval).
     *
     * @throws IOException if persistence fails; this fails the ongoing checkpoint
     */
    default void flush() throws IOException {}

    /** Releases resources held by the queue when the sink writer closes. */
    default void close() throws Exception {}
}
