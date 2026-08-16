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

package io.github.flink.gcp.connector.spanner.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import com.google.auth.oauth2.GoogleCredentials;
import io.github.flink.gcp.connector.base.failure.DefaultFailureHandlerContext;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.spanner.SpannerCredentials;
import io.github.flink.gcp.connector.spanner.sink.writer.CellWeights;
import io.github.flink.gcp.connector.spanner.sink.writer.DefaultSpannerDatabaseAccessFactory;
import io.github.flink.gcp.connector.spanner.sink.writer.SpannerDatabaseAccess;
import io.github.flink.gcp.connector.spanner.sink.writer.SpannerDatabaseAccessFactory;
import io.github.flink.gcp.connector.spanner.sink.writer.SpannerWriter;

import java.io.IOException;

/**
 * At-least-once sink applying one mutation per record through {@code batchWriteAtLeastOnce} of
 * {@code google-cloud-spanner}.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public class SpannerMutationsSink<T> implements CrossVersionSink<T> {

    private static final long serialVersionUID = 1L;

    private final SpannerSinkConfig<T> config;

    /**
     * Creates the sink; called by {@link SpannerSinkBuilder}.
     *
     * @param config the sink configuration
     */
    public SpannerMutationsSink(SpannerSinkConfig<T> config) {
        this.config = config;
    }

    /** Returns the sink configuration. */
    public SpannerSinkConfig<T> getConfig() {
        return config;
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) throws IOException {
        GoogleCredentials credentials = SpannerCredentials.load(config.getServiceAccountKeyFile());
        return createWriter(
                context,
                new DefaultSpannerDatabaseAccessFactory(
                        config.getDatabase(),
                        config.getWriterOptions(),
                        config.getEmulatorEndpoint(),
                        credentials));
    }

    /**
     * The production path, against an injectable factory.
     *
     * <p>The seam exists for one assertion the sink cannot otherwise make observable: that a failed
     * creation releases the {@link SpannerDatabaseAccess} it had already opened, and not only the
     * failure handler. The production overload above is what a job calls, and it is one call, so
     * the two cannot drift.
     */
    @VisibleForTesting
    SinkWriter<T> createWriter(WriterInitContext context, SpannerDatabaseAccessFactory factory)
            throws IOException {
        try {
            config.getSerializer().open(context.asSerializationSchemaInitializationContext());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while opening the Spanner serialization schema.", e);
        } catch (Exception e) {
            throw new IOException("Failed to open the Spanner serialization schema.", e);
        }
        config.getFailedMutationHandler().open(DefaultFailureHandlerContext.of(context));
        SpannerDatabaseAccess access = null;
        try {
            access = factory.create();
            // Read once, here rather than lazily in the writer: a database the sink cannot read the
            // schema of is a misconfiguration, and failing at writer creation makes it a job that
            // never starts instead of one that dies at the first record.
            return createWriter(access, access.readCellWeights(), context.metricGroup());
        } catch (Throwable e) {
            // Nothing downstream will ever close these: no writer exists to do it, and the failure
            // handler's contract promises a close on the failure path too — a restart would
            // otherwise open one more per attempt.
            //
            // Throwable, not Exception: a client's first classload can fail with a
            // NoClassDefFoundError, which repeats on every attempt and would otherwise walk past
            // this guard. Precise rethrow keeps the declared throws clause honest.
            Closers.closeAllSuppressing(e, access, config.getFailedMutationHandler()::close);
            throw e;
        }
    }

    /**
     * Creates the writer against an injected database access. Deliberately does <b>not</b> open the
     * failure handler — that belongs to the production path above, so writer tests injecting fakes
     * need no {@link WriterInitContext}.
     */
    @VisibleForTesting
    public SinkWriter<T> createWriter(
            SpannerDatabaseAccess access,
            CellWeights cellWeights,
            SinkWriterMetricGroup metricGroup) {
        return new SpannerWriter<>(config, access, cellWeights, metricGroup);
    }
}
