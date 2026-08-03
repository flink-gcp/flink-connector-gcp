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

package io.github.flink.gcp.connector.cloudtasks.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import io.github.flink.gcp.connector.base.failure.DefaultFailureHandlerContext;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.CloudTasksWriter;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.DefaultTaskCreatorFactory;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.TaskCreator;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.TaskCreatorFactory;

import java.io.IOException;

/**
 * At-least-once sink creating one Cloud Tasks task per record through the {@code CreateTask} RPC,
 * with fixed or per-record queue destinations.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public class CloudTasksCreateTaskSink<T> implements CrossVersionSink<T> {

    private static final long serialVersionUID = 1L;

    private final CloudTasksSinkConfig<T> config;

    /**
     * Creates the sink; called by {@link CloudTasksSinkBuilder}.
     *
     * @param config the sink configuration
     */
    public CloudTasksCreateTaskSink(CloudTasksSinkConfig<T> config) {
        this.config = config;
    }

    /** Returns the sink configuration. */
    public CloudTasksSinkConfig<T> getConfig() {
        return config;
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) throws IOException {
        try {
            config.getSerializer().open(context.asSerializationSchemaInitializationContext());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(
                    "Interrupted while opening the Cloud Tasks serialization schema.", e);
        } catch (Exception e) {
            throw new IOException("Failed to open the Cloud Tasks serialization schema.", e);
        }
        config.getFailedTaskHandler().open(DefaultFailureHandlerContext.of(context));
        TaskCreatorFactory factory = new DefaultTaskCreatorFactory(config.getEmulatorEndpoint());
        return createWriter(factory.create(), context.getMailboxExecutor(), context.metricGroup());
    }

    /**
     * Creates the writer against injected collaborators. Deliberately does <b>not</b> open the
     * failure handler: opening belongs to the production path above, and a test driving this
     * overload opens whatever it needs itself.
     */
    @VisibleForTesting
    public SinkWriter<T> createWriter(
            TaskCreator creator,
            MailboxExecutor mailboxExecutor,
            SinkWriterMetricGroup metricGroup) {
        return new CloudTasksWriter<>(config, creator, mailboxExecutor, metricGroup);
    }
}
