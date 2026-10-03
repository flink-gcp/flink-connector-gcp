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

package io.github.flink.gcp.connector.datastore.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;

import com.google.auth.oauth2.GoogleCredentials;
import io.github.flink.gcp.connector.base.failure.DefaultFailureHandlerContext;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.lineage.internal.Lineage;
import io.github.flink.gcp.connector.datastore.DatastoreCredentials;
import io.github.flink.gcp.connector.datastore.sink.writer.DatastoreDatabaseAccessFactory;
import io.github.flink.gcp.connector.datastore.sink.writer.DatastoreWriter;
import io.github.flink.gcp.connector.datastore.sink.writer.DefaultDatastoreDatabaseAccessFactory;

import java.io.IOException;
import java.util.List;

/**
 * At-least-once sink applying one entity write per record through non-transactional commits of
 * {@code google-cloud-datastore}.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public class DatastoreCommitSink<T> implements CrossVersionSink<T>, LineageVertexProvider {

    private static final long serialVersionUID = 1L;

    private final DatastoreSinkConfig<T> config;

    /**
     * Creates the sink; called by {@link DatastoreSinkBuilder}.
     *
     * @param config the sink configuration
     */
    public DatastoreCommitSink(DatastoreSinkConfig<T> config) {
        this.config = config;
    }

    /** Returns the sink configuration. */
    public DatastoreSinkConfig<T> getConfig() {
        return config;
    }

    @Override
    public LineageVertex getLineageVertex() {
        return Lineage.sink(List.of());
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) throws IOException {
        GoogleCredentials credentials =
                DatastoreCredentials.load(config.getServiceAccountKeyFile());
        return createWriter(
                context,
                new DefaultDatastoreDatabaseAccessFactory(
                        config.getDatabase(),
                        config.getWriterOptions().getRequestTimeout(),
                        config.getEmulatorEndpoint(),
                        credentials));
    }

    /**
     * The production path, against an injectable factory, so a test can observe that a failed
     * creation releases the failure handler it had already opened. The production overload above is
     * one call, so the two cannot drift.
     */
    @VisibleForTesting
    SinkWriter<T> createWriter(WriterInitContext context, DatastoreDatabaseAccessFactory factory)
            throws IOException {
        try {
            config.getSerializer().open(context.asSerializationSchemaInitializationContext());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(
                    "Interrupted while opening the Datastore serialization schema.", e);
        } catch (Exception e) {
            throw new IOException("Failed to open the Datastore serialization schema.", e);
        }
        config.getFailedMutationHandler().open(DefaultFailureHandlerContext.of(context));
        try {
            // The writer opens the database access last in its constructor, so a failure here
            // leaves only the handler to release.
            return new DatastoreWriter<>(
                    config,
                    factory,
                    context.getTaskInfo().getNumberOfParallelSubtasks(),
                    context.metricGroup());
        } catch (Throwable e) {
            // Nothing downstream will ever close the handler: no writer exists to do it, and the
            // failure handler's contract promises a close on the failure path too. Throwable, not
            // Exception: a client's first classload can fail with a NoClassDefFoundError, which
            // repeats on every attempt.
            Closers.closeAllSuppressing(e, config.getFailedMutationHandler()::close);
            throw e;
        }
    }
}
