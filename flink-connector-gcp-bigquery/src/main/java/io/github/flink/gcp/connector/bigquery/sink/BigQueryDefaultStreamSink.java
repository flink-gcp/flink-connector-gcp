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

package io.github.flink.gcp.connector.bigquery.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import io.github.flink.gcp.connector.bigquery.sink.writer.BigQueryDefaultStreamWriter;
import io.github.flink.gcp.connector.bigquery.sink.writer.BigQueryTableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.writer.RowAppenderFactory;
import io.github.flink.gcp.connector.bigquery.sink.writer.StreamWriterRowAppenderFactory;
import io.github.flink.gcp.connector.bigquery.sink.writer.TableAdmin;

/**
 * At-least-once sink appending to Storage Write API default streams with dynamic per-record table
 * destinations ({@link WriteMethod#STORAGE_API_AT_LEAST_ONCE}).
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public class BigQueryDefaultStreamSink<T> implements Sink<T> {

    private static final long serialVersionUID = 1L;

    private final BigQuerySinkConfig<T> config;

    BigQueryDefaultStreamSink(BigQuerySinkConfig<T> config) {
        this.config = config;
    }

    /** Returns the sink configuration. */
    public BigQuerySinkConfig<T> getConfig() {
        return config;
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) {
        return createWriter(new StreamWriterRowAppenderFactory(), new BigQueryTableAdmin());
    }

    @VisibleForTesting
    public SinkWriter<T> createWriter(RowAppenderFactory appenderFactory, TableAdmin tableAdmin) {
        return new BigQueryDefaultStreamWriter<>(config, appenderFactory, tableAdmin);
    }
}
