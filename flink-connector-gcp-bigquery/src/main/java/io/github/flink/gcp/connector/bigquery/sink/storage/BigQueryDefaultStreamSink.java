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

import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.CrossVersionSink;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.BigQueryDefaultStreamWriter;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.RowAppenderFactory;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.StreamWriterRowAppenderFactory;
import io.github.flink.gcp.connector.bigquery.sink.tables.BigQueryTableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableAdmin;

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
    public SinkWriter<T> createWriter(WriterInitContext context) {
        // The context's processing-time service fires timer callbacks on the mailbox (task)
        // thread, which is what makes the writer's periodic flush safe against its
        // task-thread-only state.
        return new BigQueryDefaultStreamWriter<>(
                config,
                new StreamWriterRowAppenderFactory(options),
                new BigQueryTableAdmin(),
                options,
                context.getProcessingTimeService());
    }

    @VisibleForTesting
    public SinkWriter<T> createWriter(RowAppenderFactory appenderFactory, TableAdmin tableAdmin) {
        return new BigQueryDefaultStreamWriter<>(config, appenderFactory, tableAdmin, options);
    }
}
