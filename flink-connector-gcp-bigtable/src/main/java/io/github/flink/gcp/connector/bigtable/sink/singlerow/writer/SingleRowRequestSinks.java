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

package io.github.flink.gcp.connector.bigtable.sink.singlerow.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import com.google.api.gax.core.CredentialsProvider;
import io.github.flink.gcp.connector.base.failure.DefaultFailureHandlerContext;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.bigtable.BigtableClientMetrics;
import io.github.flink.gcp.connector.bigtable.BigtableCredentials;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.RowOperation;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * The {@code createWriter} path the per-operation single-row sinks share: credentials, the client
 * factory, the serializer's and the handler's {@code open}, and the release of all three when the
 * writer cannot be built.
 *
 * <p>The shape is {@code BigtableMutateRowsSink.createWriter}'s. The production path is one call so
 * the sinks cannot drift from each other, and the injectable overload is the seam the emulator
 * tests drive the production client path through.
 */
@Internal
public final class SingleRowRequestSinks {

    private SingleRowRequestSinks() {}

    /**
     * Creates a writer for a job: loads the credentials, builds the client factory over them and
     * opens the serializer and the failure handler.
     *
     * @param config the sink configuration
     * @param context the writer's initialization context
     * @param operation the one RPC the sink's requests call, whose client metrics the writer
     *     registers when exporting them to Flink
     * @param <T> type of the records written by the sink
     * @return the writer
     * @throws IOException if the credentials cannot be loaded or the serializer or handler cannot
     *     be opened
     */
    public static <T> SinkWriter<T> createWriter(
            SingleRowRequestConfig<T> config, WriterInitContext context, RowOperation operation)
            throws IOException {
        // The credential first, so a key file that cannot be read fails with the context untouched.
        CredentialsProvider credentials =
                BigtableCredentials.loadData(config.getServiceAccountKeyFile());
        // Registered as the writer opens, before any reporter reads its group. Not closed: the
        // group ends with the writer (docs/adr/0186).
        ClientMetrics clientMetrics =
                config.isExportClientMetricsToFlink()
                        ? BigtableClientMetrics.register(
                                context.metricGroup(), operation.getRpcName())
                        : null;
        return createWriter(config, context, clientFactory(config, credentials, clientMetrics));
    }

    /**
     * Builds the production client factory from the sink configuration. Separate so that a test can
     * see each client-metrics choice reach the client's settings.
     */
    @VisibleForTesting
    static DefaultSingleRowClientFactory clientFactory(
            SingleRowRequestConfig<?> config,
            @Nullable CredentialsProvider credentials,
            @Nullable ClientMetrics clientMetrics) {
        return new DefaultSingleRowClientFactory(
                config.getAppProfileId(),
                config.getRequestOptions(),
                config.getEmulatorEndpoint(),
                credentials,
                config.isExportClientMetricsToCloudMonitoring(),
                clientMetrics);
    }

    /**
     * The production path, against an injectable factory. The seam exists for one assertion the
     * production overload cannot make observable: that a failed creation releases the factory it
     * had already built, and not only the failure handler.
     */
    @VisibleForTesting
    static <T> SinkWriter<T> createWriter(
            SingleRowRequestConfig<T> config,
            WriterInitContext context,
            SingleRowClientFactory factory)
            throws IOException {
        try {
            config.getSerializer().open(context.asSerializationSchemaInitializationContext());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while opening the Bigtable request serializer.", e);
        } catch (Exception e) {
            throw new IOException("Failed to open the Bigtable request serializer.", e);
        }
        config.getFailedRequestHandler().open(DefaultFailureHandlerContext.of(context));
        try {
            return new SingleRowRequestWriter<>(
                    config, factory, context.getMailboxExecutor(), context.metricGroup());
        } catch (Throwable e) {
            // Nothing downstream will ever close these: no writer exists to do it, and the failure
            // handler's contract promises a close on the failure path too. Throwable, not
            // Exception: a client's first classload can fail with a NoClassDefFoundError, which
            // repeats on every attempt and would otherwise walk past this guard.
            Closers.closeAllSuppressing(e, factory, config.getFailedRequestHandler()::close);
            throw e;
        }
    }
}
