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

package io.github.flink.gcp.connector.pubsub.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import com.google.api.gax.core.CredentialsProvider;
import io.github.flink.gcp.connector.base.failure.DefaultFailureHandlerContext;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.pubsub.PubSubCredentials;
import io.github.flink.gcp.connector.pubsub.sink.topics.PubSubTopicAdmin;
import io.github.flink.gcp.connector.pubsub.sink.topics.TopicAdmin;
import io.github.flink.gcp.connector.pubsub.sink.writer.DefaultPublisherFactory;
import io.github.flink.gcp.connector.pubsub.sink.writer.PubSubWriter;
import io.github.flink.gcp.connector.pubsub.sink.writer.PublisherFactory;

import java.io.IOException;

/**
 * At-least-once sink publishing through {@code google-cloud-pubsub} {@code Publisher} instances
 * with dynamic per-record topic destinations.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public class PubSubPublisherSink<T> implements CrossVersionSink<T> {

    private static final long serialVersionUID = 1L;

    private final PubSubSinkConfig<T> config;

    /**
     * Creates the sink; called by {@link PubSubSinkBuilder}.
     *
     * @param config the sink configuration
     */
    public PubSubPublisherSink(PubSubSinkConfig<T> config) {
        this.config = config;
    }

    /** Returns the sink configuration. */
    public PubSubSinkConfig<T> getConfig() {
        return config;
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) throws IOException {
        try {
            config.getSerializer().open(context.asSerializationSchemaInitializationContext());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while opening the Pub/Sub serialization schema.", e);
        } catch (Exception e) {
            throw new IOException("Failed to open the Pub/Sub serialization schema.", e);
        }
        config.getFailedMessageHandler().open(DefaultFailureHandlerContext.of(context));
        EmulatorEndpoint emulatorEndpoint = config.getEmulatorEndpoint();
        TopicAdmin topicAdmin = null;
        try {
            CredentialsProvider credentials =
                    PubSubCredentials.load(config.getServiceAccountKeyFile());
            topicAdmin = new PubSubTopicAdmin(emulatorEndpoint, credentials);
            return createWriter(
                    new DefaultPublisherFactory(
                            config.getPublisherOptions(), emulatorEndpoint, credentials),
                    topicAdmin,
                    context.getMailboxExecutor(),
                    context.metricGroup());
        } catch (Throwable e) {
            // Nothing downstream will ever close these: no writer exists to do it, and the failure
            // handler's contract promises a close on the failure path too — Flink rebuilds the
            // writer on every restart attempt, so an opened handler accumulates per attempt on a
            // task manager that stays alive, holding a publisher and a channel when it is a
            // dead-letter queue. What fails here is the writer's constructor, which rejects
            // in-flight caps a deserialized options object never ran the builder's checks over.
            // The admin is released too although its close() frees nothing today: the writer would
            // have owned it, so this is the same contract rather than a new claim about it.
            //
            // Throwable, not Exception: a client's first classload can fail with a
            // NoClassDefFoundError, which repeats on every attempt and would otherwise walk past
            // this guard. Precise rethrow keeps the declared throws clause honest, and it also
            // means a checked exception added to anything above stays covered.
            Closers.closeAllSuppressing(e, topicAdmin, config.getFailedMessageHandler()::close);
            throw e;
        }
    }

    /**
     * Creates the writer against injected collaborators. Deliberately does <b>not</b> open the
     * failure handler — that belongs to the production path above, so writer tests injecting fakes
     * need no {@link WriterInitContext}.
     */
    @VisibleForTesting
    public SinkWriter<T> createWriter(
            PublisherFactory publisherFactory,
            TopicAdmin topicAdmin,
            MailboxExecutor mailboxExecutor,
            SinkWriterMetricGroup metricGroup) {
        return new PubSubWriter<>(
                config, publisherFactory, topicAdmin, mailboxExecutor, metricGroup);
    }
}
