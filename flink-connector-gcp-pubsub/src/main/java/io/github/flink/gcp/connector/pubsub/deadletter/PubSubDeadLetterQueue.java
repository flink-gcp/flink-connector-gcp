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

package io.github.flink.gcp.connector.pubsub.deadletter;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.base.failure.DeadLetterQueue;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.failure.FailureHandlerContext;
import io.github.flink.gcp.connector.base.lifecycle.BoundedShutdown;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.options.OptionChecks;
import io.github.flink.gcp.connector.base.rpc.EmulatorChannels;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.pubsub.PubSubCredentials;
import io.github.flink.gcp.connector.pubsub.PubSubShutdownResidue;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.grpc.ManagedChannel;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A {@link DeadLetterQueue} publishing every terminally failed element to a Pub/Sub topic, used
 * through {@link FailureHandler#sendToDeadLetterQueue(DeadLetterQueue)}.
 *
 * <p>It sees failures through the shared {@link FailedElement} contract, so <b>one instance serves
 * every connector in this repository</b> — a BigQuery or Cloud Tasks job dead-letters to a topic by
 * adding the {@code flink-connector-gcp-pubsub} artifact as a dependency. It does not go through
 * the Pub/Sub sink: it owns an SDK {@link Publisher} of its own, so a job that dead-letters is not
 * also a Pub/Sub sink job.
 * <!-- javadoc-example file="JavadocPubSubExamples.java" tag="dead-letter-queue" -->
 *
 * <pre>{@code
 * BigQuerySink.<Order>builder()
 *         .destination(TableDestination.of("my-project", "my_dataset", "orders"))
 *         .serializer(serializer)
 *         .failureHandler(
 *                 FailureHandler.sendToDeadLetterQueue(
 *                         PubSubDeadLetterQueue.builder()
 *                                 .topic(
 *                                         TopicDestination.of(
 *                                                 "my-project", "dead-letters"))
 *                                 .build()))
 *         .build();
 * }</pre>
 *
 * <h2>The envelope</h2>
 *
 * <p>The message data is the element's {@link FailedElement#getPayloadBytes() payload bytes}, or
 * empty when that method returns {@code null}. A concrete failure may also supply an intentionally
 * empty payload, so consumers must use the attributes rather than data length alone to classify the
 * failure. Every message carries five attributes:
 *
 * <table>
 *   <caption>Attributes of a dead-lettered message</caption>
 *   <tr><th>Attribute</th><th>Value</th></tr>
 *   <tr><td>{@code dlq-connector}</td><td>{@code bigquery}, {@code bigtable}, {@code cloudtasks},
 *       {@code pubsub} or {@code spanner}</td></tr>
 *   <tr><td>{@code dlq-destination}</td><td>the resource the element was bound for, or a
 *       connector-defined sentinel such as {@code unresolved}</td></tr>
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
 * <p>Both of those waits — the one in {@link #flush()}, which runs at every checkpoint barrier, and
 * the one the outstanding bound triggers inside {@link #offer(FailedElement)} — are bounded by
 * {@link Builder#flushTimeout(Duration)}: one deadline covering all of that wait's publishes, not
 * one per publish, and not what a whole checkpoint interval spends. Expiry fails the job and drops
 * nothing. The queue's <em>close</em> waits for the same publishes under a budget of its own,
 * {@link Builder#shutdownTimeout(Duration)}.
 *
 * <h2>Metrics</h2>
 *
 * <p>{@link #open(FailureHandlerContext)} registers five names on the metric group the context
 * carries — <b>the host sink writer's</b>, so a BigQuery job dead-lettering to a topic reports them
 * beside BigQuery's own. They are {@code deadLettersPublished}, {@code outstandingDeadLetters},
 * {@code deadLetterFlushMillis}, {@code longestDeadLetterFlushMillis} and {@code
 * deadLetterPublisherShutdownsAbandoned}; what each means is on this connector's documentation
 * page, under "Dead-letter metrics". The count of elements <em>offered</em> is not among them
 * because every sink here already reports it as {@code numRecordsSendErrors} on that same group.
 *
 * <h2>Credentials</h2>
 *
 * <p>Production publishers use application-default credentials unless {@link
 * Builder#serviceAccountKeyFile(String)} selects a service-account JSON key. The configured path
 * crosses Flink serialization, and each host sink writer reads the file when it opens the queue.
 * Parsed credentials are never stored in the job graph. The queue does not inherit the host
 * connector's credential setting, so the dead-letter publisher may use a separate identity.
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

    /** Marks an error truncated to fit {@code MAX_ATTRIBUTE_VALUE_BYTES}; ASCII, so 3 bytes. */
    @VisibleForTesting static final String TRUNCATION_MARKER = "...";

    /** The default outstanding bound: high enough that a rare failure never waits. */
    public static final int DEFAULT_MAX_OUTSTANDING_MESSAGES = 1000;

    /** {@link Builder#maxOutstandingMessages(int)} value publishing each element synchronously. */
    public static final int WRITE_THROUGH = 0;

    /** {@link Builder#maxOutstandingMessages(int)} value buffering until {@link #flush()}. */
    public static final int UNBOUNDED = -1;

    /** The default {@link Builder#shutdownTimeout(Duration)}, matching the sink's own. */
    public static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);

    /**
     * The default {@link Builder#flushTimeout(Duration)}: a tenth of Flink's default {@code
     * execution.checkpointing.timeout}, so a dead-letter outage costs a fraction of a checkpoint's
     * budget rather than all of it. It is deliberately not derived from the SDK's retry ladder (5
     * s, 20 s then 60 s per attempt, within 600 s): a batch published on {@code offer} is usually
     * already on its third attempt by the time the barrier's flush waits for it, so no fixed budget
     * corresponds to a whole number of attempts.
     */
    public static final Duration DEFAULT_FLUSH_TIMEOUT = Duration.ofSeconds(60);

    private final TopicDestination topic;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;
    private final int maxOutstandingMessages;
    private final Duration shutdownTimeout;
    private final Duration flushTimeout;

    private transient Publisher publisher;

    /** The emulator channel this instance owns and shuts down; null on the ADC transport. */
    @Nullable private transient ManagedChannel ownedChannel;

    /**
     * The two steps {@link #close()} runs, held as fields rather than called directly so a test can
     * drive its failure path: {@link Publisher} cannot be subclassed (non-final, but its only
     * constructor is private, which forbids a subclass just as effectively — #324), so there is no
     * other seam. Set together with {@code publisher} by {@link #open(FailureHandlerContext)} and
     * cleared with it, which is why the first of them stands in for it as the not-open guard.
     *
     * <p>The first is a {@link BoundedShutdown}: the SDK's own {@code shutdown()} is not guaranteed
     * to return, and running it inline on the task thread is what #312 removed.
     */
    @Nullable @VisibleForTesting transient AutoCloseable publisherShutdown;

    /** The second of the two; see {@code publisherShutdown}. */
    @Nullable @VisibleForTesting transient AutoCloseable channelShutdown;

    /** Publishes not yet awaited; task-thread only, like every other field here. */
    private transient List<ApiFuture<String>> outstanding;

    /**
     * Registered on the host sink writer's metric group by {@link #open(FailureHandlerContext)}.
     */
    private transient PubSubDeadLetterQueueMetrics metrics;

    private transient int subtaskIndex;

    private PubSubDeadLetterQueue(
            TopicDestination topic,
            @Nullable String serviceAccountKeyFile,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            int maxOutstandingMessages,
            Duration shutdownTimeout,
            Duration flushTimeout) {
        this.topic = Preconditions.checkNotNull(topic, "topic must not be null");
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.emulatorEndpoint = emulatorEndpoint;
        this.maxOutstandingMessages = maxOutstandingMessages;
        this.shutdownTimeout = shutdownTimeout;
        this.flushTimeout = flushTimeout;
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
        CredentialsProvider credentials = PubSubCredentials.load(serviceAccountKeyFile);
        Publisher.Builder builder = Publisher.newBuilder(topic.toTopicPath());
        try {
            if (emulatorEndpoint != null) {
                ownedChannel = EmulatorChannels.openPlaintextChannel(emulatorEndpoint);
                builder.setChannelProvider(EmulatorChannels.fixedProvider(ownedChannel))
                        .setCredentialsProvider(NoCredentialsProvider.create());
            } else {
                configureCredentials(builder, credentials);
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
                            shutdownTimeout,
                            // A residue of its own rather than the sink publishers' total: these
                            // metrics register on whichever sink hosts the queue, and a Pub/Sub
                            // sink has already registered `publisherShutdownsAbandoned` there —
                            // Flink would drop the later registration with a warning. Splitting
                            // them also says which publisher is stalling.
                            PubSubShutdownResidue.DEAD_LETTER_PUBLISHER_SHUTDOWNS_ABANDONED);
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
        // Last, so that the names exist exactly when the queue can be used: an open that threw
        // above fails the writer's creation, and the metric group goes with the task.
        metrics =
                new PubSubDeadLetterQueueMetrics(
                        context.getMetricGroup(), this::getOutstandingMessages);
    }

    /** Applies an explicit provider without replacing the SDK's ADC provider when it is absent. */
    @VisibleForTesting
    static void configureCredentials(
            Publisher.Builder builder, @Nullable CredentialsProvider credentials) {
        if (credentials != null) {
            builder.setCredentialsProvider(credentials);
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
            flushOutstanding(
                    publisher::publishAllOutstanding, outstanding, topic, flushTimeout, metrics);
        }
    }

    @Override
    public void flush() throws IOException {
        Preconditions.checkState(publisher != null, "The dead-letter queue is not open.");
        flushOutstanding(
                publisher::publishAllOutstanding, outstanding, topic, flushTimeout, metrics);
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
            // The registered gauges outlive this field — they read through method references —
            // so clearing it releases nothing and only keeps this block's meaning uniform: after
            // close, every field open() set is gone.
            metrics = null;
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
                // Empty rather than absent when the failure has no payload: the attributes still
                // say which destination and which error, which is what makes the record findable.
                .setData(payload == null ? ByteString.EMPTY : payload)
                .putAttributes("dlq-connector", element.getConnector())
                .putAttributes("dlq-destination", element.describeDestination())
                .putAttributes("dlq-error", truncateToAttributeLimit(element.getErrorMessage()))
                .putAttributes("dlq-timestamp", DateTimeFormatter.ISO_INSTANT.format(offeredAt))
                .putAttributes("dlq-subtask", Integer.toString(subtaskIndex))
                .build();
    }

    /**
     * Hands the buffered dead letters to the publisher and awaits them, failing on the first that
     * did not succeed and on the budget running out.
     *
     * <p><b>One deadline covers the whole call, never one per future.</b> {@link
     * Builder#maxOutstandingMessages(int)} defaults to 1000, so a per-future budget would be a
     * thousandfold multiple of the number it claims to be — the shape of the mistake #265's first
     * teardown fix made, where gax hands its full timeout to each background resource in turn. The
     * deadline is taken before {@code publishAll}, so that call's time is charged against the
     * budget — charged rather than bounded, since nothing here can interrupt it.
     *
     * <p>A budget is spent per call, and a checkpoint interval may make several calls (see {@link
     * Builder#flushTimeout(Duration)}). During an outage that costs one budget rather than several,
     * because the first expiry throws and ends the interval — not because a call is the interval.
     *
     * <p>Static, taking its topic, budget and metrics as arguments, for the reason {@link
     * #envelope} is: {@link Publisher} cannot be subclassed (see the teardown fields above), so
     * handing this the futures is the only way a test can reach it, and opening a real publisher to
     * do so would strand a gax executor in the test JVM.
     *
     * <p><b>{@code deadLettersPublished} is counted here, one future at a time</b>, rather than at
     * the offer that handed the publish over: the offered count is already {@code
     * numRecordsSendErrors} on this same metric group, and a partly resolved wait must not report
     * the publishes it never got to.
     */
    @VisibleForTesting
    static void flushOutstanding(
            Runnable publishAll,
            List<ApiFuture<String>> outstanding,
            TopicDestination topic,
            Duration budget,
            PubSubDeadLetterQueueMetrics metrics)
            throws IOException {
        long startNanos = System.nanoTime();
        // Read before the finally clears the list: a flush with nothing buffered is not a wait,
        // and recording it would erase the slow wait an operator is meant to see. `flush()` runs
        // at every barrier, so on a job that dead-letters occasionally almost every call is empty.
        boolean hadPublishesToAwait = !outstanding.isEmpty();
        // Overflows at the ceiling the setter accepts, and is correct anyway: the
        // subtraction below wraps a second time and the two cancel, leaving the true remainder
        // (measured — theLargestExpressibleBudgetIsNotSpentTheInstantTheFlushStarts pins it).
        // Math.addExact here would turn that legal budget into a failed flush.
        long deadlineNanos = startNanos + budget.toNanos();
        int resolved = 0;
        try {
            try {
                publishAll.run();
            } catch (RuntimeException e) {
                throw new IOException(
                        "Failed to hand the buffered dead letters for Pub/Sub topic "
                                + topic
                                + " to the publisher.",
                        e);
            }
            for (ApiFuture<String> future : outstanding) {
                try {
                    // Zero when the budget is already spent, which still returns the value of a
                    // future that is done: the point is bounding the wait, not failing on the
                    // clock.
                    future.get(
                            Math.max(deadlineNanos - System.nanoTime(), 0), TimeUnit.NANOSECONDS);
                    resolved++;
                    // After the get, so only a publish the service confirmed is counted.
                    metrics.deadLetterPublished();
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
                } catch (TimeoutException e) {
                    throw new IOException(
                            "Waiting for dead letters to reach Pub/Sub topic "
                                    + topic
                                    + " ran out of its flushTimeout budget of "
                                    + budget
                                    + " with "
                                    + (outstanding.size() - resolved)
                                    + " of "
                                    + outstanding.size()
                                    + " publishes unresolved. Nothing is dropped: the job fails and"
                                    + " the records behind these dead letters are replayed from the"
                                    + " last completed checkpoint. None resolved usually means the"
                                    + " topic is unreachable or the credentials cannot publish to"
                                    + " it; some resolved means it is slow, and"
                                    + " PubSubDeadLetterQueue.builder().flushTimeout(...) is the"
                                    + " budget to raise — against what a checkpoint interval can"
                                    + " afford, since one interval may spend several of them.",
                            e);
                }
            }
        } finally {
            // Recorded however the wait ended, so that one which spent the whole budget is the one
            // the gauge holds — an expiry fails the job, so that value is rarely scraped, but a
            // duration metric that skipped exactly the interesting case would be worse.
            if (hadPublishesToAwait) {
                metrics.flushCompleted(
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
            }
            // Cleared whatever happened: a failure fails the job, and re-awaiting futures that
            // already failed — or that outlived one budget — would only report the same thing
            // again. The unresolved ones are deliberately not cancelled, so a message the SDK still
            // delivers is a duplicate the DeadLetterQueue contract already covers, not a loss.
            outstanding.clear();
        }
    }

    /** The publishes not yet awaited, so a test can observe the outstanding bound taking effect. */
    @VisibleForTesting
    int getOutstandingMessages() {
        return outstanding == null ? 0 : outstanding.size();
    }

    /**
     * The wait budget, so a test can read which one reached the tasks. Unlike {@link
     * Builder#shutdownTimeout(Duration)}, which is only observable through the teardown {@link
     * #open(FailureHandlerContext)} builds, this needs no publisher.
     */
    @VisibleForTesting
    Duration flushTimeout() {
        return flushTimeout;
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
        @Nullable private String serviceAccountKeyFile;
        @Nullable private EmulatorEndpoint emulatorEndpoint;
        private int maxOutstandingMessages = DEFAULT_MAX_OUTSTANDING_MESSAGES;
        private Duration shutdownTimeout = DEFAULT_SHUTDOWN_TIMEOUT;
        private Duration flushTimeout = DEFAULT_FLUSH_TIMEOUT;

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
         * Authenticates the queue with the service-account JSON key at the given path instead of
         * application-default credentials. The file is read when a host sink writer opens this
         * queue, so the same path must be readable by every TaskManager that can run that sink.
         * Optional; when unset the queue uses application-default credentials.
         *
         * <p>The queue does not inherit credentials from the host connector. A dead-letter topic
         * may intentionally use a different identity, and the shared failure-handler contract does
         * not carry the host connector's credential configuration.
         *
         * <p>Service-account keys are long-lived secrets. Prefer an attached service account or
         * Workload Identity where the deployment supports one. This setting cannot be combined with
         * {@link #emulatorEndpoint(String)}, whose plaintext channel deliberately carries no
         * credentials.
         *
         * @param serviceAccountKeyFile the service-account JSON key-file path
         * @return this builder
         */
        public Builder serviceAccountKeyFile(String serviceAccountKeyFile) {
            String checked =
                    Preconditions.checkNotNull(
                            serviceAccountKeyFile, "serviceAccountKeyFile must not be null");
            Preconditions.checkArgument(
                    !checked.isBlank(), "serviceAccountKeyFile must not be blank");
            this.serviceAccountKeyFile = checked;
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
            this.emulatorEndpoint = EmulatorEndpoint.parse(emulatorEndpoint, "emulatorEndpoint");
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
            OptionChecks.checkPositive(shutdownTimeout, "shutdownTimeout");
            // This budget reaches BoundedShutdown.start(), which converts it with toNanos(): a
            // longer one throws ArithmeticException on a TaskManager, out of a close. The shape of
            // failure this class already rejects at the setter for emulatorEndpoint (ADR-0068).
            this.shutdownTimeout =
                    OptionChecks.checkExpressibleInNanos(shutdownTimeout, "shutdownTimeout");
            return this;
        }

        /**
         * Sets how long the queue waits for its buffered publishes, in {@link #flush()} and in the
         * {@link #maxOutstandingMessages(int)} drain alike. Defaults to 60 seconds.
         *
         * <p>{@code flush()} runs at every checkpoint barrier, so without a budget a wait lasts as
         * long as the SDK keeps retrying — 600 seconds by default, which is also Flink's default
         * {@code execution.checkpointing.timeout}. It is one deadline per wait, covering all of
         * that wait's publishes rather than each of them.
         *
         * <p><b>It bounds one wait, not what a checkpoint interval spends.</b> How many waits an
         * interval makes is {@link #maxOutstandingMessages(int)}: one under {@link #UNBOUNDED}, one
         * per bound-full at a positive value, and one per element under {@link #WRITE_THROUGH}. A
         * slow-but-working topic can therefore spend several budgets in an interval without any of
         * them expiring.
         *
         * <p>On expiry the wait throws — failing the ongoing checkpoint from {@code flush()}, and
         * the task itself from an offer, where no checkpoint is in progress. The queue drops
         * nothing, and since the publishes are not cancelled the SDK may still deliver them — a
         * duplicate, which is what the {@link DeadLetterQueue} guarantee already asks a consumer to
         * expect. A disturbance longer than the budget therefore fails the job where the SDK's
         * retry would have absorbed it, which is the trade a bound buys. There is deliberately no
         * unbounded setting; a {@code Duration} longer than any disturbance worth surviving says
         * the same thing without making waiting forever a mode.
         *
         * @param flushTimeout the wait budget, positive
         * @return this builder
         */
        public Builder flushTimeout(Duration flushTimeout) {
            OptionChecks.checkPositive(flushTimeout, "flushTimeout");
            // This knob's own documentation offers a long budget as the way to say "effectively
            // unbounded", so the ceiling is what keeps that instruction from throwing
            // ArithmeticException out of the first flush on a TaskManager (ADR-0068).
            this.flushTimeout = OptionChecks.checkExpressibleInNanos(flushTimeout, "flushTimeout");
            return this;
        }

        /**
         * Builds the queue.
         *
         * @return the queue
         */
        public PubSubDeadLetterQueue build() {
            Preconditions.checkState(topic != null, "A dead-letter topic is required.");
            Preconditions.checkState(
                    serviceAccountKeyFile == null || emulatorEndpoint == null,
                    "serviceAccountKeyFile(...) cannot be combined with emulatorEndpoint(...): an"
                            + " emulator uses a plaintext channel with no credentials. Remove one"
                            + " of the two settings.");
            return new PubSubDeadLetterQueue(
                    topic,
                    serviceAccountKeyFile,
                    emulatorEndpoint,
                    maxOutstandingMessages,
                    shutdownTimeout,
                    flushTimeout);
        }
    }
}
