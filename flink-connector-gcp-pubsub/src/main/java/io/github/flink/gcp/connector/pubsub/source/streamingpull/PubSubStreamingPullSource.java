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
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.source.PubSubSourceConfig;
import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.enumerator.PubSubSplitEnumerator;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.AckTracker;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.DefaultSubscriberFactory;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.PubSubAckTracker;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.PubSubRecordEmitter;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.PubSubSourceReader;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.PubSubSplitReader;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.SubscriberFactory;

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

    /**
     * Messages drained from one split per fetch. Bounds how much one fetch buffers while still
     * amortizing the element-queue handoff; the client library's flow control is the real limit on
     * in-flight messages. Becomes a tuning knob together with the other subscriber options (#80).
     */
    private static final int MAX_RECORDS_PER_FETCH = 1_000;

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

        AckTracker ackTracker = new PubSubAckTracker();
        SubscriberFactory subscriberFactory =
                new DefaultSubscriberFactory(
                        config.getOrderingMode(), config.getEmulatorEndpoint());
        Supplier<SplitReader<PubsubMessage, SubscriptionSplit>> splitReaderSupplier =
                () -> new PubSubSplitReader(subscriberFactory, ackTracker, MAX_RECORDS_PER_FETCH);
        return new PubSubSourceReader<>(
                splitReaderSupplier,
                new PubSubRecordEmitter<>(deserializationSchema, ackTracker),
                context.getConfiguration(),
                context,
                ackTracker);
    }

    @Override
    public SplitEnumerator<SubscriptionSplit, PubSubEnumeratorState> createEnumerator(
            SplitEnumeratorContext<SubscriptionSplit> context) {
        return new PubSubSplitEnumerator(
                context, config.getSubscriptions(), config.getOrderingMode(), null);
    }

    @Override
    public SplitEnumerator<SubscriptionSplit, PubSubEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<SubscriptionSplit> context, PubSubEnumeratorState checkpoint) {
        return new PubSubSplitEnumerator(
                context, config.getSubscriptions(), config.getOrderingMode(), checkpoint);
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
