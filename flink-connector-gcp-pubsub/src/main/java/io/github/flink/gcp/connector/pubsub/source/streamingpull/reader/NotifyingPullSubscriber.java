/*
 * Copyright 2023 Google LLC
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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import org.apache.flink.annotation.Internal;

import com.google.pubsub.v1.PubsubMessage;

import java.io.IOException;
import java.util.List;

/**
 * Bridges Pub/Sub's push-style streaming pull to the pull-style {@link
 * org.apache.flink.connector.base.source.reader.splitreader.SplitReader} contract: the client
 * library delivers messages on its own threads into an in-memory buffer, and the fetcher thread
 * drains that buffer.
 *
 * <p>Adapted from the Flink connector in <a
 * href="https://github.com/GoogleCloudPlatform/pubsub">GoogleCloudPlatform/pubsub</a> (Apache-2.0),
 * which exposes a single-message pull and its own notification future per subscriber.
 */
@Internal
public interface NotifyingPullSubscriber extends AutoCloseable {

    /**
     * Removes and returns up to {@code maxMessages} buffered messages, in the order the client
     * library delivered them, or an empty list when nothing is buffered.
     *
     * @param maxMessages the maximum number of messages to return
     * @return the drained messages
     * @throws IOException if the subscriber has failed permanently
     */
    List<PubsubMessage> pullMessages(int maxMessages) throws IOException;

    /**
     * Reports a permanent failure without draining anything, so a split nothing is pulling from is
     * still watched.
     *
     * <p>{@link #pullMessages} reports the same failure, and for a split the reader drains that is
     * the only report needed. This exists for the split the reader deliberately does <em>not</em>
     * drain: a paused one (#348). Watermark alignment pauses splits routinely, and a paused split
     * is skipped entirely — so without this, a subscriber that fails while paused is asked for
     * messages by nobody and its failure is read by nobody, leaving the job green with one
     * subscription silently dead, which is the outcome a source exists to fail on.
     *
     * <p>An implementation must report the failure alone and never the absence of messages: a
     * paused split is <em>supposed</em> to produce none, so a check that could not tell "paused and
     * healthy" from "paused and dead" would fail every aligned job.
     *
     * @throws IOException if the subscriber has failed permanently, carrying the failure {@link
     *     #pullMessages} would report
     */
    void checkFailure() throws IOException;

    /**
     * Returns what this subscriber currently holds buffered, so the reader can bound a split it is
     * not draining.
     *
     * <p>Reported rather than bounded here because the bound is the reader's policy and the
     * response is the reader's to make: this buffer is deliberately unbounded, and a subscriber
     * that refused a message would have to block a client-library thread or nack it, which are the
     * two things the implementation's design rules out (see {@link PubSubNotifyingPullSubscriber}).
     *
     * <p><b>Unlike the rest of this interface, this must be safe to call from any thread.</b> The
     * reader calls it on the fetcher thread, and the {@code bufferedMessages} and {@code
     * bufferedBytes} gauges call it on whatever thread the metric reporter runs on — while the
     * client library is appending to the same buffer on one of its own.
     *
     * @return the buffered messages and their total serialized size
     */
    BufferUsage bufferUsage();

    /**
     * Nacks every message this subscriber's split still holds and asks the client to shut down,
     * returning without waiting for it. Buffered messages are discarded — they were never emitted,
     * so Pub/Sub must redeliver them. Idempotent.
     *
     * <p>Named for {@link java.util.concurrent.ExecutorService#shutdown()}, which is the same
     * shape: it starts the shutdown and returns, and a separate call waits for it to finish.
     * Separate from {@link #close()} because the nack is what must not be skipped while the wait is
     * what costs time — a reader owning several splits shuts them all down before waiting on any,
     * so the waits overlap instead of accumulating.
     */
    void shutdown();

    /**
     * Shuts the subscriber down if it is not already shutting down, then waits for the client to
     * finish, up to the configured shutdown timeout.
     *
     * <p>An implementation must not report a failure it has already delivered through {@link
     * #pullMessages} or {@link #checkFailure}. The reader consumes that one and fails the job on
     * it, so a second report here only adds a competing exception to a teardown the first one is
     * already causing — which is why {@link PubSubNotifyingPullSubscriber} absorbs the one its
     * client raises (#325). The repository-wide rule this is an instance of, and what was measured
     * about the other connectors' clients, are in the detailed repository guidance.
     *
     * <p>A failure the client raises <em>during</em> this teardown is a different case, and this
     * contract does not require it to be raised either (#351). Nothing has consumed it — the reader
     * has stopped pulling — but the job is already ending, so an implementation may absorb it, and
     * {@link PubSubNotifyingPullSubscriber} does. What it must not do is report it as the repeat
     * above: they are opposite things to tell an operator, one saying a job failure is coming and
     * the other that none is.
     *
     * @throws Exception if the shutdown itself goes wrong, for some reason other than either of
     *     those failures
     */
    @Override
    void close() throws Exception;
}
