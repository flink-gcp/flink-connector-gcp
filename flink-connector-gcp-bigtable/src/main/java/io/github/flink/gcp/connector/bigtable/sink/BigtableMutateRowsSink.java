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

package io.github.flink.gcp.connector.bigtable.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import io.github.flink.gcp.connector.base.failure.DefaultFailureHandlerContext;
import io.github.flink.gcp.connector.bigtable.sink.writer.BigtableWriter;
import io.github.flink.gcp.connector.bigtable.sink.writer.DefaultMutationBatcherFactory;
import io.github.flink.gcp.connector.bigtable.sink.writer.MutationBatcher;
import io.github.flink.gcp.connector.bigtable.sink.writer.MutationBatcherFactory;

import java.io.IOException;

/**
 * At-least-once sink applying one row mutation per record through the {@code MutateRows} bulk
 * batcher of {@code google-cloud-bigtable}.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public class BigtableMutateRowsSink<T> implements CrossVersionSink<T> {

    private static final long serialVersionUID = 1L;

    private final BigtableSinkConfig<T> config;

    /**
     * Creates the sink; called by {@link BigtableSinkBuilder}.
     *
     * @param config the sink configuration
     */
    public BigtableMutateRowsSink(BigtableSinkConfig<T> config) {
        this.config = config;
    }

    /** Returns the sink configuration. */
    public BigtableSinkConfig<T> getConfig() {
        return config;
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) throws IOException {
        try {
            config.getSerializer().open(context.asSerializationSchemaInitializationContext());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(
                    "Interrupted while opening the Bigtable serialization schema.", e);
        } catch (Exception e) {
            throw new IOException("Failed to open the Bigtable serialization schema.", e);
        }
        config.getFailedMutationHandler().open(DefaultFailureHandlerContext.of(context));
        MutationBatcherFactory factory =
                new DefaultMutationBatcherFactory(
                        config.getDestination(),
                        config.getAppProfileId(),
                        config.getWriterOptions(),
                        config.getEmulatorEndpoint());
        return createWriter(factory.create(), context.getMailboxExecutor(), context.metricGroup());
    }

    /**
     * Creates the writer against an injected batcher. Deliberately does <b>not</b> open the failure
     * handler — that belongs to the production path above, so writer tests injecting fakes need no
     * {@link WriterInitContext}.
     */
    @VisibleForTesting
    public SinkWriter<T> createWriter(
            MutationBatcher batcher,
            MailboxExecutor mailboxExecutor,
            SinkWriterMetricGroup metricGroup) {
        return new BigtableWriter<>(config, batcher, mailboxExecutor, metricGroup);
    }
}
