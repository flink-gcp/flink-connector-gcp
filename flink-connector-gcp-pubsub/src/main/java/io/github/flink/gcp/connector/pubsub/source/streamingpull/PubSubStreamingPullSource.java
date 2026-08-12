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

package io.github.flink.gcp.connector.pubsub.source.streamingpull;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import com.google.api.gax.core.CredentialsProvider;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.PubSubCredentials;
import io.github.flink.gcp.connector.pubsub.source.PubSubSourceConfig;
import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;
import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.enumerator.PubSubSplitEnumerator;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.DefaultSubscriberFactory;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.MissingCheckpointDetector;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.PubSubAckTracker;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.PubSubRecordEmitter;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.PubSubSourceReader;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.PubSubSourceReaderMetrics;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.PubSubSplitReader;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.SubscriberFactory;
import io.github.flink.gcp.connector.pubsub.source.subscriptions.PubSubSubscriptionAdmin;
import io.github.flink.gcp.connector.pubsub.source.subscriptions.SubscriptionAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.function.Supplier;

/**
 * At-least-once source consuming Pub/Sub subscriptions through {@code google-cloud-pubsub} {@code
 * Subscriber} streaming pull.
 *
 * <p>Delivery state lives on the Pub/Sub server, not in Flink state: a message is acknowledged only
 * once the checkpoint that covers its emission completes, and anything unacknowledged is
 * redelivered. Checkpoints therefore hold no message data, and a restore needs no retained
 * checkpoint to resume.
 *
 * @param <T> type of the records produced by the source
 */
@Internal
public class PubSubStreamingPullSource<T>
        implements Source<T, SubscriptionSplit, PubSubEnumeratorState>, ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(PubSubStreamingPullSource.class);

    private final PubSubSourceConfig<T> config;

    /**
     * Creates the source; called by {@link
     * io.github.flink.gcp.connector.pubsub.source.PubSubSourceBuilder}.
     *
     * @param config the source configuration
     */
    public PubSubStreamingPullSource(PubSubSourceConfig<T> config) {
        this.config = config;
    }

    /** Returns the source configuration. */
    @VisibleForTesting
    public PubSubSourceConfig<T> getConfig() {
        return config;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, SubscriptionSplit> createReader(SourceReaderContext context)
            throws Exception {
        PubSubDeserializationSchema<T> deserializationSchema = config.getDeserializationSchema();
        deserializationSchema.open(new ReaderInitializationContext(context));

        PubSubSubscriberOptions options = config.getSubscriberOptions();
        CredentialsProvider credentials = PubSubCredentials.load(config.getServiceAccountKeyFile());
        String ackExtensionWarning =
                ackExtensionHeadroomWarning(context.getConfiguration(), options);
        if (ackExtensionWarning != null) {
            LOG.warn(ackExtensionWarning);
        }

        PubSubSourceReaderMetrics metrics = new PubSubSourceReaderMetrics(context.metricGroup());
        PubSubAckTracker ackTracker =
                new PubSubAckTracker(metrics, options.getAwaitAckConfirmation());
        metrics.bindAckTracker(ackTracker);
        MissingCheckpointDetector checkpointDetector =
                new MissingCheckpointDetector(
                        options.getFirstCheckpointTimeout(), ackTracker::outstandingAckCount);
        SubscriberFactory subscriberFactory =
                new DefaultSubscriberFactory(
                        options,
                        config.getOrderingMode(),
                        config.getEmulatorEndpoint(),
                        credentials);
        Supplier<SplitReader<PubsubMessage, SubscriptionSplit>> splitReaderSupplier =
                () ->
                        new PubSubSplitReader(
                                subscriberFactory,
                                ackTracker,
                                options,
                                checkpointDetector,
                                metrics);
        return new PubSubSourceReader<>(
                splitReaderSupplier,
                new PubSubRecordEmitter<>(
                        deserializationSchema,
                        ackTracker,
                        config.getDeserializationFailurePolicy(),
                        metrics),
                context.getConfiguration(),
                context,
                ackTracker,
                checkpointDetector);
    }

    /**
     * Returns a warning when the checkpoint interval leaves too little headroom under the client
     * library's acknowledgement-deadline extension budget, or {@code null} when it does not. A
     * message is acknowledged one whole checkpoint after it is emitted, so an interval anywhere
     * near that budget means leases expire and Pub/Sub redelivers everything.
     *
     * <p>Best-effort by construction: a reader is handed the TaskManager configuration, so the
     * interval is visible only when it was set at cluster level. An interval configured with {@code
     * env.enableCheckpointing(...)} lives in the job configuration and cannot be read from here,
     * which is why its absence is not treated as a problem. The reader's first-checkpoint watchdog
     * is what actually catches a job that never checkpoints.
     */
    @Nullable
    @VisibleForTesting
    static String ackExtensionHeadroomWarning(
            Configuration configuration, PubSubSubscriberOptions options) {
        Duration interval =
                configuration.getOptional(CheckpointingOptions.CHECKPOINTING_INTERVAL).orElse(null);
        if (interval == null || interval.isZero() || interval.isNegative()) {
            return null;
        }
        Duration maxAckExtension =
                options.getMaxAckExtensionPeriod() != null
                        ? options.getMaxAckExtensionPeriod()
                        : DefaultSubscriberFactory.DEFAULT_MAX_ACK_EXTENSION_PERIOD;
        if (maxAckExtension.isZero()) {
            // Reachable since the knob started forwarding the client library's own value for
            // "disable auto deadline extension" (ADR-0068). The headroom sentence would be
            // nonsense here — there is no budget to have headroom under — and its advice would
            // tell a user to undo what they deliberately configured, so this state states its
            // consequence instead.
            return "Acknowledgement-deadline extension is disabled"
                    + " (PubSubSubscriberOptions.maxAckExtensionPeriod is zero), so a message's"
                    + " lease expires at the subscription's own acknowledgement deadline. This"
                    + " source acknowledges one whole checkpoint after emitting, and the checkpoint"
                    + " interval is "
                    + interval
                    + ", so expect Pub/Sub to redeliver anything not acknowledged within that"
                    + " deadline.";
        }
        if (interval.multipliedBy(2).compareTo(maxAckExtension) <= 0) {
            return null;
        }
        return "The checkpoint interval ("
                + interval
                + ") leaves little headroom under the acknowledgement-deadline extension budget ("
                + maxAckExtension
                + "). A message is acknowledged one whole checkpoint after it is emitted, so"
                + " leases may expire and Pub/Sub redeliver everything. Shorten the checkpoint"
                + " interval or raise PubSubSubscriberOptions.maxAckExtensionPeriod(...).";
    }

    @Override
    public SplitEnumerator<SubscriptionSplit, PubSubEnumeratorState> createEnumerator(
            SplitEnumeratorContext<SubscriptionSplit> context) throws Exception {
        return new PubSubSplitEnumerator(context, config, newSubscriptionAdmin(), null);
    }

    @Override
    public SplitEnumerator<SubscriptionSplit, PubSubEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<SubscriptionSplit> context, PubSubEnumeratorState checkpoint)
            throws Exception {
        return new PubSubSplitEnumerator(context, config, newSubscriptionAdmin(), checkpoint);
    }

    /**
     * The enumerator owns the admin and closes it; this is the only admin the job manager builds.
     */
    private SubscriptionAdmin newSubscriptionAdmin() throws IOException {
        return new PubSubSubscriptionAdmin(
                config.getEmulatorEndpoint(),
                PubSubCredentials.load(config.getServiceAccountKeyFile()));
    }

    @Override
    public SimpleVersionedSerializer<SubscriptionSplit> getSplitSerializer() {
        return new SubscriptionSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<PubSubEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new PubSubEnumeratorStateSerializer();
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return config.getDeserializationSchema().getProducedType();
    }

    /**
     * Adapts the reader context to the deserialization schema's initialization context. The
     * upstream connector passes a {@code null} user code class loader here, which breaks any schema
     * that resolves classes at startup.
     */
    private static final class ReaderInitializationContext
            implements DeserializationSchema.InitializationContext {

        private final SourceReaderContext context;

        private ReaderInitializationContext(SourceReaderContext context) {
            this.context = context;
        }

        @Override
        public MetricGroup getMetricGroup() {
            return context.metricGroup();
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return context.getUserCodeClassLoader();
        }
    }
}
