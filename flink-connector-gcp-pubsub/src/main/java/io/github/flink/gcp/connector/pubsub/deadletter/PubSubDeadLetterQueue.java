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

package io.github.flink.gcp.connector.pubsub.deadletter;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.base.failure.DeadLetterQueue;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.failure.FailureHandlerContext;
import io.github.flink.gcp.connector.base.lifecycle.BoundedShutdown;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * A {@link DeadLetterQueue} publishing every terminally failed element to a Pub/Sub topic, used
 * through {@link FailureHandler#sendToDeadLetterQueue(DeadLetterQueue)}.
 *
 * <p>It sees failures through the shared {@link FailedElement} contract, so <b>one instance serves
 * every connector in this repository</b> — a BigQuery or Cloud Tasks job dead-letters to a topic by
 * adding the {@code flink-connector-gcp-pubsub} artifact as a dependency. It does not go through
 * the Pub/Sub sink: it owns an SDK {@link Publisher} of its own, so a job that dead-letters is not
 * also a Pub/Sub sink job.
 *
 * <pre>{@code
 * BigQuerySink.<Order>builder()
 *         .table(TableDestination.of("my-project", "my_dataset", "orders"))
 *         .serializer(serializer)
 *         .failedRowHandler(
 *                 FailureHandler.sendToDeadLetterQueue(
 *                         PubSubDeadLetterQueue.builder()
 *                                 .topic(TopicDestination.of("my-project", "dead-letters"))
 *                                 .build()))
 *         .build();
 * }</pre>
 *
 * <h2>The envelope</h2>
 *
 * <p>The message data is the element's {@link FailedElement#getPayloadBytes() payload bytes} —
 * empty when serialization itself failed, so a consumer distinguishes the two by data length. Every
 * message carries five attributes:
 *
 * <table>
 *   <caption>Attributes of a dead-lettered message</caption>
 *   <tr><th>Attribute</th><th>Value</th></tr>
 *   <tr><td>{@code dlq-connector}</td><td>{@code bigquery}, {@code pubsub} or {@code cloudtasks}
 *       </td></tr>
 *   <tr><td>{@code dlq-destination}</td><td>the resource the element was bound for</td></tr>
 *   <tr><td>{@code dlq-error}</td><td>the failure description, truncated to Pub/Sub's 1024-byte
 *       attribute-value limit</td></tr>
 *   <tr><td>{@code dlq-timestamp}</td><td>when the element was offered, ISO-8601</td></tr>
 *   <tr><td>{@code dlq-subtask}</td><td>the offering sink subtask's index</td></tr>
 * </table>
 *
 * <p>The failure's cause chain is not in the envelope: it has no bounded string form. Enable {@code
 * DEBUG} logging on this class to see each element's untruncated error in the job logs.
 *
 * <p>An element whose payload is close to Pub/Sub's 10 MB message limit may not fit once the
 * attributes are added — an oversized element is exactly the kind a sink rejects, so this is the
 * expected shape of that case. The publish then fails and fails the job, since a dead letter that
 * cannot be delivered must not be silently lost.
 *
 * <h2>Buffering</h2>
 *
 * <p>Publishes are batched by the SDK and awaited in {@link #flush()}, so the usual case — a rare
 * failure among healthy records — costs no round trip per element. Because a systematic failure (a
 * serializer bug rejecting every record) would otherwise let one whole checkpoint interval's
 * publishes accumulate unawaited, the outstanding count is bounded: at {@link
 * Builder#maxOutstandingMessages(int)} the queue awaits what it holds before accepting more.
 *
 * <p>Instances are configured on the job graph and serialized to the tasks; the publisher itself is
 * created in {@link #open(FailureHandlerContext)}. Lifecycle, and the at-least-once guarantee that
 * comes with it, are the {@link DeadLetterQueue} contract's.
 */
@Experimental
public final class PubSubDeadLetterQueue implements DeadLetterQueue {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(PubSubDeadLetterQueue.class);

    /** Pub/Sub's limit on an attribute value, in bytes. */
    @VisibleForTesting static final int MAX_ATTRIBUTE_VALUE_BYTES = 1024;

    /** Marks an error truncated to fit {@link #MAX_ATTRIBUTE_VALUE_BYTES}; ASCII, so 3 bytes. */
    @VisibleForTesting static final String TRUNCATION_MARKER = "...";

    /** The default outstanding bound: high enough that a rare failure never waits. */
    public static final int DEFAULT_MAX_OUTSTANDING_MESSAGES = 1000;

    /** {@link Builder#maxOutstandingMessages(int)} value publishing each element synchronously. */
    public static final int WRITE_THROUGH = 0;

    /** {@link Builder#maxOutstandingMessages(int)} value buffering until {@link #flush()}. */
    public static final int UNBOUNDED = -1;

    /** The default {@link Builder#shutdownTimeout(Duration)}, matching the sink's own. */
    public static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);

    private final TopicDestination topic;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;
    private final int maxOutstandingMessages;
    private final Duration shutdownTimeout;

    private transient Publisher publisher;

    /** The emulator channel this instance owns and shuts down; null on the ADC transport. */
    @Nullable private transient ManagedChannel ownedChannel;

    /**
     * The two steps {@link #close()} runs, held as fields rather than called directly so a test can
     * drive its failure path: {@link Publisher} is final, so there is no other seam. Set together
     * with {@link #publisher} by {@link #open(FailureHandlerContext)} and cleared with it, which is
     * why the first of them stands in for it as the not-open guard.
     *
     * <p>The first is a {@link BoundedShutdown}: the SDK's own {@code shutdown()} is not guaranteed
     * to return, and running it inline on the task thread is what #312 removed.
     */
    @Nullable @VisibleForTesting transient AutoCloseable publisherShutdown;

    /** The second of the two; see {@link #publisherShutdown}. */
    @Nullable @VisibleForTesting transient AutoCloseable channelShutdown;

    /** Publishes not yet awaited; task-thread only, like every other field here. */
    private transient List<ApiFuture<String>> outstanding;

    private transient int subtaskIndex;

    private PubSubDeadLetterQueue(
            TopicDestination topic,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            int maxOutstandingMessages,
            Duration shutdownTimeout) {
        this.topic = Preconditions.checkNotNull(topic, "topic must not be null");
        this.emulatorEndpoint = emulatorEndpoint;
        this.maxOutstandingMessages = maxOutstandingMessages;
        this.shutdownTimeout = shutdownTimeout;
    }

    /**
     * Returns a builder.
     *
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void open(FailureHandlerContext context) throws IOException {
        subtaskIndex = context.getSubtaskIndex();
        outstanding = new ArrayList<>();
        Publisher.Builder builder = Publisher.newBuilder(topic.toTopicPath());
        try {
            if (emulatorEndpoint != null) {
                ownedChannel =
                        ManagedChannelBuilder.forTarget(emulatorEndpoint.getTarget())
                                .usePlaintext()
                                .build();
                builder.setChannelProvider(
                                FixedTransportChannelProvider.create(
                                        GrpcTransportChannel.create(ownedChannel)))
                        .setCredentialsProvider(NoCredentialsProvider.create());
            }
            publisher = builder.build();
            // The channel is released by channelShutdown, the next entry in close()'s list, rather
            // than by the teardown itself: it is graceful shutdown() here, not the sink's
            // shutdownNow(), and Closers.closeAll already runs it whatever the first entry did.
            publisherShutdown =
                    new BoundedShutdown(
                            publisher::shutdown,
                            publisher::awaitTermination,
                            "dead-letter topic " + topic,
                            null,
                            shutdownTimeout);
            channelShutdown = this::shutdownChannel;
        } catch (IOException | RuntimeException e) {
            // The channel is owned here until the publisher takes it over on success.
            if (ownedChannel != null) {
                ownedChannel.shutdownNow();
                ownedChannel = null;
            }
            throw new IOException(
                    "Failed to create the dead-letter publisher for Pub/Sub topic " + topic + ".",
                    e);
        }
    }

    @Override
    public void offer(FailedElement element) throws IOException {
        Preconditions.checkState(publisher != null, "The dead-letter queue is not open.");
        LOG.debug(
                "Dead-lettering a {} element bound for {} to Pub/Sub topic {}: {}",
                element.getConnector(),
                element.describeDestination(),
                topic,
                element.getErrorMessage(),
                element.getCause());
        try {
            outstanding.add(publisher.publish(envelope(element, subtaskIndex, Instant.now())));
        } catch (RuntimeException e) {
            throw new IOException(
                    "Failed to publish a dead letter to Pub/Sub topic " + topic + ".", e);
        }
        if (maxOutstandingMessages >= 0 && outstanding.size() >= maxOutstandingMessages) {
            // Bounds what one checkpoint interval can accumulate when *every* record fails. At
            // zero this is a synchronous publish per element, which is the write-through mode.
            publisher.publishAllOutstanding();
            awaitOutstanding();
        }
    }

    @Override
    public void flush() throws IOException {
        Preconditions.checkState(publisher != null, "The dead-letter queue is not open.");
        publisher.publishAllOutstanding();
        awaitOutstanding();
    }

    @Override
    public void close() throws Exception {
        if (publisherShutdown == null) {
            return;
        }
        try {
            // Through Closers.closeAll, so the channel is shut down even when the publisher's
            // shutdown throws: an emulator channel left running holds a gRPC transport open.
            Closers.closeAll(publisherShutdown, channelShutdown);
        } finally {
            publisher = null;
            ownedChannel = null;
            outstanding = null;
            publisherShutdown = null;
            channelShutdown = null;
        }
    }

    /**
     * Builds the message: the payload as data, the failure's description as attributes. Static and
     * pure — the envelope is the part of this class worth pinning exactly, and taking the clock as
     * an argument is what lets a test do so without a live publisher.
     */
    @VisibleForTesting
    static PubsubMessage envelope(FailedElement element, int subtaskIndex, Instant offeredAt) {
        ByteString payload = element.getPayloadBytes();
        return PubsubMessage.newBuilder()
                // Empty rather than absent when serialization itself failed: the attributes still
                // say which destination and which error, which is what makes the record findable.
                .setData(payload == null ? ByteString.EMPTY : payload)
                .putAttributes("dlq-connector", element.getConnector())
                .putAttributes("dlq-destination", element.describeDestination())
                .putAttributes("dlq-error", truncateToAttributeLimit(element.getErrorMessage()))
                .putAttributes("dlq-timestamp", DateTimeFormatter.ISO_INSTANT.format(offeredAt))
                .putAttributes("dlq-subtask", Integer.toString(subtaskIndex))
                .build();
    }

    /** Awaits every outstanding publish, failing on the first that did not succeed. */
    private void awaitOutstanding() throws IOException {
        try {
            for (ApiFuture<String> future : outstanding) {
                try {
                    future.get();
                } catch (ExecutionException e) {
                    throw new IOException(
                            "Publishing a dead letter to Pub/Sub topic " + topic + " failed.",
                            e.getCause());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "Interrupted while publishing dead letters to Pub/Sub topic "
                                    + topic
                                    + ".",
                            e);
                }
            }
        } finally {
            // Cleared whatever happened: a failure fails the job, and re-awaiting futures that
            // already failed would only report the same one again.
            outstanding.clear();
        }
    }

    /** The publishes not yet awaited, so a test can observe the outstanding bound taking effect. */
    @VisibleForTesting
    int getOutstandingMessages() {
        return outstanding == null ? 0 : outstanding.size();
    }

    private void shutdownChannel() {
        if (ownedChannel != null) {
            ownedChannel.shutdown();
        }
    }

    /**
     * Truncates a value to Pub/Sub's attribute-value limit on a character boundary, marking that it
     * was cut. Cutting the UTF-8 bytes blindly would leave a partial multi-byte character, which
     * the service rejects for not being valid UTF-8 — turning a dead letter into a job failure.
     */
    @VisibleForTesting
    static String truncateToAttributeLimit(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= MAX_ATTRIBUTE_VALUE_BYTES) {
            return value;
        }
        int budget = MAX_ATTRIBUTE_VALUE_BYTES - TRUNCATION_MARKER.length();
        CharsetDecoder decoder =
                StandardCharsets.UTF_8
                        .newDecoder()
                        // A character straddling the cut is malformed input, and dropping it is
                        // exactly the truncation wanted.
                        .onMalformedInput(CodingErrorAction.IGNORE)
                        .onUnmappableCharacter(CodingErrorAction.IGNORE);
        CharBuffer out = CharBuffer.allocate(budget);
        decoder.decode(ByteBuffer.wrap(bytes, 0, budget), out, true);
        decoder.flush(out);
        out.flip();
        return out.toString() + TRUNCATION_MARKER;
    }

    @Override
    public String toString() {
        return "PubSubDeadLetterQueue{topic=" + topic + "}";
    }

    /** Builder for {@link PubSubDeadLetterQueue}. */
    @Experimental
    public static final class Builder {

        private TopicDestination topic;
        @Nullable private EmulatorEndpoint emulatorEndpoint;
        private int maxOutstandingMessages = DEFAULT_MAX_OUTSTANDING_MESSAGES;
        private Duration shutdownTimeout = DEFAULT_SHUTDOWN_TIMEOUT;

        private Builder() {}

        /**
         * Sets the topic every dead letter is published to. Required; the topic must exist, since
         * this queue never creates one — a dead-letter destination created on the fly is one
         * nothing is consuming.
         *
         * @param topic the dead-letter topic
         * @return this builder
         */
        public Builder topic(TopicDestination topic) {
            this.topic = Preconditions.checkNotNull(topic, "topic must not be null");
            return this;
        }

        /**
         * Points the queue at a Pub/Sub emulator instead of the production service. The connection
         * to the given {@code host:port} uses a plaintext channel with no credentials, so this must
         * only ever be used against an emulator. Optional; when unset the queue publishes with
         * application-default credentials.
         *
         * <p>The value is parsed here, so a malformed {@code host:port} is rejected on the client
         * instead of surfacing as a connection failure once the job has been deployed.
         *
         * @param emulatorEndpoint the emulator endpoint as {@code host:port}
         * @return this builder
         * @throws IllegalArgumentException if the endpoint is not {@code host:port} with a port in
         *     1..65535
         */
        public Builder emulatorEndpoint(String emulatorEndpoint) {
            this.emulatorEndpoint = EmulatorEndpoint.parse(emulatorEndpoint);
            return this;
        }

        /**
         * Sets how many publishes may be outstanding before {@link #offer} awaits them, bounding
         * what one checkpoint interval accumulates when every record fails. Defaults to {@value
         * #DEFAULT_MAX_OUTSTANDING_MESSAGES}.
         *
         * <p>{@link #WRITE_THROUGH} (0) publishes each element synchronously — the narrowest loss
         * window the {@link DeadLetterQueue} contract describes, at one round trip per element.
         * {@link #UNBOUNDED} (-1) buffers everything until {@link #flush()}, which is the fastest
         * and the only setting whose memory is not bounded by this queue.
         *
         * @param maxOutstandingMessages the bound, {@link #WRITE_THROUGH} or {@link #UNBOUNDED}
         * @return this builder
         */
        public Builder maxOutstandingMessages(int maxOutstandingMessages) {
            Preconditions.checkArgument(
                    maxOutstandingMessages >= UNBOUNDED,
                    "maxOutstandingMessages must be -1 (unbounded), 0 (write through) or positive");
            this.maxOutstandingMessages = maxOutstandingMessages;
            return this;
        }

        /**
         * Sets how long {@link #close()} waits for the dead-letter publisher to shut down. Defaults
         * to 30 seconds.
         *
         * <p>This is a budget of its own, spent after and on top of the sink's own {@code
         * shutdownTimeout}: a sink that dead-letters closes its publishers first and this queue
         * last. Keep the sum under Flink's {@code task.cancellation.timeout} (180 s by default),
         * past which a cancelling task is a fatal TaskManager error.
         *
         * @param shutdownTimeout the shutdown budget, positive
         * @return this builder
         */
        public Builder shutdownTimeout(Duration shutdownTimeout) {
            Preconditions.checkNotNull(shutdownTimeout, "shutdownTimeout must not be null");
            Preconditions.checkArgument(
                    !shutdownTimeout.isZero() && !shutdownTimeout.isNegative(),
                    "shutdownTimeout must be positive");
            this.shutdownTimeout = shutdownTimeout;
            return this;
        }

        /**
         * Builds the queue.
         *
         * @return the queue
         */
        public PubSubDeadLetterQueue build() {
            Preconditions.checkState(topic != null, "A dead-letter topic is required.");
            return new PubSubDeadLetterQueue(
                    topic, emulatorEndpoint, maxOutstandingMessages, shutdownTimeout);
        }
    }
}
