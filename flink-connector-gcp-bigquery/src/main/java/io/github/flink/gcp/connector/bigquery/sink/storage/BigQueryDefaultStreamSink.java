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

package io.github.flink.gcp.connector.bigquery.sink.storage;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import io.github.flink.gcp.connector.base.failure.DefaultFailureHandlerContext;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.CrossVersionSink;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.BigQueryDefaultStreamWriter;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.RowAppenderFactory;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.StreamWriterRowAppenderFactory;
import io.github.flink.gcp.connector.bigquery.sink.tables.BigQueryTableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.tables.RetryingTableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableAdmin;

import java.io.IOException;

/**
 * At-least-once sink appending to Storage Write API default streams with dynamic per-record table
 * destinations ({@link WriteMethod#STORAGE_API_AT_LEAST_ONCE}).
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public class BigQueryDefaultStreamSink<T> implements CrossVersionSink<T> {

    private static final long serialVersionUID = 1L;

    private final BigQuerySinkConfig<T> config;
    private final DefaultStreamOptions options;

    /**
     * Creates the sink; called by {@link
     * io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkBuilder}.
     *
     * @param config the sink configuration
     * @param options the default-stream options
     */
    public BigQueryDefaultStreamSink(BigQuerySinkConfig<T> config, DefaultStreamOptions options) {
        this.config = config;
        this.options = options;
    }

    /** Returns the sink configuration. */
    public BigQuerySinkConfig<T> getConfig() {
        return config;
    }

    /** Returns the default-stream options. */
    public DefaultStreamOptions getOptions() {
        return options;
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) throws IOException {
        config.getFailureHandler().open(DefaultFailureHandlerContext.of(context));
        try {
            // The context's processing-time service fires timer callbacks on the mailbox (task)
            // thread, which is what makes the writer's periodic flush safe against its
            // task-thread-only state.
            return new BigQueryDefaultStreamWriter<>(
                    config,
                    createRowAppenderFactory(),
                    createTableAdmin(),
                    context.metricGroup(),
                    options,
                    context.getProcessingTimeService());
        } catch (Throwable e) {
            // The handler is the only thing to release: the appender factory and the table admin
            // hold no client until the writer asks them for one. Nothing downstream would close it
            // — no writer exists to do it — and Flink rebuilds the writer on every restart attempt,
            // so an opened handler would accumulate per attempt on a task manager that stays alive.
            //
            // Throwable, not Exception: a client's first classload can fail with a
            // NoClassDefFoundError, which repeats on every attempt and would otherwise walk past
            // this guard. Precise rethrow keeps the declared throws clause honest, and it also
            // means a checked exception added to anything above stays covered.
            Closers.closeAllSuppressing(e, config.getFailureHandler()::close);
            throw e;
        }
    }

    /** Returns the runtime appender factory wired from this sink's configuration. */
    @VisibleForTesting
    StreamWriterRowAppenderFactory createRowAppenderFactory() {
        return new StreamWriterRowAppenderFactory(
                options, config.getServiceAccountKeyFile(), config.getEmulatorEndpoint());
    }

    /**
     * The admin the writer creates tables through: the REST one, wrapped so a creation the
     * per-table quota rate-limits is repeated rather than failing the write (#383).
     *
     * <p>A method rather than an inline expression so a test can assert what was wired. The
     * overload below lets a test inject its own admin, which means the wrap is otherwise reachable
     * only by a job against real BigQuery — and a `createWriter` that stopped wrapping would ship
     * green, the failure appearing as a job losing a race it usually wins. Same argument as {@code
     * BufferedStreamCommitter.getCreateDisposition}.
     *
     * @return the admin
     */
    @VisibleForTesting
    TableAdmin createTableAdmin() {
        return new RetryingTableAdmin(
                new BigQueryTableAdmin(
                        config.getServiceAccountKeyFile(),
                        config.getEmulatorRestEndpoint(),
                        config.getLocation()),
                options.toRecoverySchedule());
    }

    /** Test entry point; unlike the production path it does not open the failure handler. */
    @VisibleForTesting
    public SinkWriter<T> createWriter(
            RowAppenderFactory appenderFactory,
            TableAdmin tableAdmin,
            SinkWriterMetricGroup metricGroup) {
        return new BigQueryDefaultStreamWriter<>(
                config, appenderFactory, tableAdmin, metricGroup, options);
    }
}
