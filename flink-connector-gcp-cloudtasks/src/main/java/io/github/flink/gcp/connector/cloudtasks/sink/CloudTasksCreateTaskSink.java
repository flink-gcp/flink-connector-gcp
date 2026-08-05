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
import io.github.flink.gcp.connector.base.lifecycle.Closers;
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
        return createWriter(context, new DefaultTaskCreatorFactory(config.getEmulatorEndpoint()));
    }

    /**
     * The production path, against an injectable factory.
     *
     * <p>The seam exists for one assertion the sink cannot otherwise make observable: that a failed
     * creation releases the {@link TaskCreator} it had already built, and not only the failure
     * handler. The sink owns a client at that point, which is what makes that half of the guard
     * worth a seam — {@code BigtableMutateRowsSink} carries the same one for the same reason. The
     * production overload above is what a job calls, and it is one line, so the two cannot drift.
     */
    @VisibleForTesting
    SinkWriter<T> createWriter(WriterInitContext context, TaskCreatorFactory factory)
            throws IOException {
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
        TaskCreator creator = null;
        try {
            creator = factory.create();
            return createWriter(creator, context.getMailboxExecutor(), context.metricGroup());
        } catch (Throwable e) {
            // Nothing downstream will ever release these: no writer exists to do it, and the
            // failure handler's contract promises a close on the failure path too — Flink rebuilds
            // the writer on every restart attempt, so one more client and one more opened handler
            // would accumulate per attempt on a task manager that stays alive. The creator is
            // released as well as the handler because the writer's constructor can fail with the
            // client already built: it reads options that a deserialized object never ran the
            // builder's checks over.
            //
            // Throwable, not Exception: a client's first classload can fail with a
            // NoClassDefFoundError, which repeats on every attempt and would otherwise walk past
            // this guard. Precise rethrow keeps the declared throws clause honest, and it also
            // means a checked exception added to anything above stays covered.
            Closers.closeAllSuppressing(e, creator, config.getFailedTaskHandler()::close);
            throw e;
        }
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
