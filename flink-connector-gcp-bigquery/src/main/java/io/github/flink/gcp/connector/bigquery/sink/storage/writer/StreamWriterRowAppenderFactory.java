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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.core.ApiFuture;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ConnectionWorkerPool;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.ProtoSchemaConverter;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.protobuf.Descriptors;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.github.flink.gcp.connector.bigquery.sink.storage.DefaultStreamOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Default {@link RowAppenderFactory} backed by Storage Write API {@link StreamWriter}s on the
 * destination's default write stream.
 *
 * <p>Connection multiplexing is delegated to the client's connection pool ({@code
 * setEnableConnectionPool(true)}): the pool is JVM-static per (location, credentials), shares
 * connections across destination tables, scales with load, and transparently reconnects on
 * server-side idle disconnects. One lightweight {@code StreamWriter} exists per destination. The
 * SDK-facing tuning — in-stream retry settings, per-connection in-flight limits and the pool's
 * connection bounds — comes from the {@link DefaultStreamOptions} this factory is constructed with;
 * see that class for the JVM-global first-writer-wins caveats.
 */
@Internal
public class StreamWriterRowAppenderFactory implements RowAppenderFactory {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(StreamWriterRowAppenderFactory.class);

    /**
     * The SDK requires the "A:B" trace-id format (an interior colon is mandatory). Shared with the
     * buffered-stream write path ({@link WriteClientBufferedStreamService}).
     */
    static final String TRACE_ID = "flink-gcp:flink-connector-gcp-bigquery";

    /**
     * The pool bounds applied to the JVM-global {@code ConnectionWorkerPool} settings, or {@code
     * null} while no factory has applied any. {@code ConnectionWorkerPool.setOptions} overwrites a
     * process-wide static that pools read at construction (the floor) and on scale-up decisions
     * (the ceiling), so it is applied once per JVM by whichever factory creates an appender first;
     * a later factory carrying different bounds logs a warning and changes nothing — the SDK could
     * not honor a second value set anyway, exactly as it silently keeps the first writer's
     * in-flight limits.
     */
    private static final AtomicReference<PoolBounds> APPLIED_POOL_BOUNDS = new AtomicReference<>();

    private final DefaultStreamOptions options;

    /**
     * Creates the factory.
     *
     * @param options the SDK-facing tuning knobs; the factory is shipped inside the job graph, so
     *     the options travel with it
     */
    public StreamWriterRowAppenderFactory(DefaultStreamOptions options) {
        this.options = Objects.requireNonNull(options, "options must not be null");
    }

    @Override
    public RowAppender create(
            TableDestination destination, Descriptors.Descriptor rowDescriptor, String location)
            throws IOException {
        applyPoolBoundsOnce(options);
        StreamWriter.Builder builder =
                StreamWriter.newBuilder(destination.toTablePath() + "/_default")
                        .setWriterSchema(ProtoSchemaConverter.convert(rowDescriptor))
                        .setEnableConnectionPool(true)
                        .setRetrySettings(toRetrySettings(options))
                        .setMaxRetryDuration(options.getMaxRetryDuration())
                        .setMaxInflightRequests(options.getMaxInflightRequests())
                        .setMaxInflightBytes(options.getMaxInflightBytes())
                        .setTraceId(TRACE_ID);
        if (location != null) {
            builder.setLocation(location);
        }
        StreamWriter streamWriter = builder.build();
        return new StreamWriterRowAppender(streamWriter);
    }

    /**
     * Builds the SDK's in-stream {@link RetrySettings} from the buffered path's {@code retry*}
     * knobs. The same in-stream retries apply to offset appends there, where a retry of an append
     * that already landed answers {@code ALREADY_EXISTS} — which that writer treats as success.
     */
    static RetrySettings toRetrySettings(BufferedStreamOptions options) {
        return toRetrySettings(
                options.getRetryInitialDelay(),
                options.getRetryDelayMultiplier(),
                options.getRetryMaxDelay(),
                options.getRetryMaxAttempts());
    }

    /** Builds the SDK's in-stream {@link RetrySettings} from the {@code retry*} knobs. */
    static RetrySettings toRetrySettings(DefaultStreamOptions options) {
        return toRetrySettings(
                options.getRetryInitialDelay(),
                options.getRetryDelayMultiplier(),
                options.getRetryMaxDelay(),
                options.getRetryMaxAttempts());
    }

    private static RetrySettings toRetrySettings(
            Duration initialDelay, double multiplier, Duration maxDelay, int maxAttempts) {
        return RetrySettings.newBuilder()
                .setInitialRetryDelayDuration(initialDelay)
                .setRetryDelayMultiplier(multiplier)
                .setMaxRetryDelayDuration(maxDelay)
                .setMaxAttempts(maxAttempts)
                .build();
    }

    /**
     * Applies the pool bounds to the JVM-global {@code ConnectionWorkerPool} settings, first
     * factory wins; see {@link #APPLIED_POOL_BOUNDS}. Called before {@code StreamWriter.build()} so
     * the bounds are in place before this factory can create the pool — though another BigQuery
     * client in the JVM may already have created it, in which case the floor is latched and only
     * the ceiling (read live) still applies.
     */
    @VisibleForTesting
    static void applyPoolBoundsOnce(DefaultStreamOptions options) {
        PoolBounds requested =
                new PoolBounds(
                        options.getMinConnectionsPerRegion(), options.getMaxConnectionsPerRegion());
        if (APPLIED_POOL_BOUNDS.compareAndSet(null, requested)) {
            ConnectionWorkerPool.setOptions(
                    ConnectionWorkerPool.Settings.builder()
                            .setMinConnectionsPerRegion(requested.minConnectionsPerRegion)
                            .setMaxConnectionsPerRegion(requested.maxConnectionsPerRegion)
                            .build());
            LOG.info("Applied Storage Write API connection pool bounds: {}", requested);
            return;
        }
        PoolBounds applied = APPLIED_POOL_BOUNDS.get();
        if (!requested.equals(applied)) {
            LOG.warn(
                    "Storage Write API connection pool bounds are JVM-global and already applied"
                            + " as {}; ignoring the different bounds {} requested by {}.",
                    applied,
                    requested,
                    options);
        }
    }

    /** Returns the applied pool bounds, or {@code null} if none were applied yet. */
    @VisibleForTesting
    static PoolBounds appliedPoolBounds() {
        return APPLIED_POOL_BOUNDS.get();
    }

    /** Clears the applied pool bounds so a test can exercise the first-application path. */
    @VisibleForTesting
    static void resetAppliedPoolBoundsForTests() {
        APPLIED_POOL_BOUNDS.set(null);
    }

    /** The (floor, ceiling) pair handed to {@code ConnectionWorkerPool.setOptions}. */
    static final class PoolBounds {

        final int minConnectionsPerRegion;
        final int maxConnectionsPerRegion;

        PoolBounds(int minConnectionsPerRegion, int maxConnectionsPerRegion) {
            this.minConnectionsPerRegion = minConnectionsPerRegion;
            this.maxConnectionsPerRegion = maxConnectionsPerRegion;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PoolBounds that = (PoolBounds) o;
            return minConnectionsPerRegion == that.minConnectionsPerRegion
                    && maxConnectionsPerRegion == that.maxConnectionsPerRegion;
        }

        @Override
        public int hashCode() {
            return Objects.hash(minConnectionsPerRegion, maxConnectionsPerRegion);
        }

        @Override
        public String toString() {
            return "PoolBounds{minConnectionsPerRegion="
                    + minConnectionsPerRegion
                    + ", maxConnectionsPerRegion="
                    + maxConnectionsPerRegion
                    + "}";
        }
    }

    private static final class StreamWriterRowAppender implements RowAppender {

        private final StreamWriter streamWriter;

        StreamWriterRowAppender(StreamWriter streamWriter) {
            this.streamWriter = streamWriter;
        }

        @Override
        public ApiFuture<AppendRowsResponse> append(ProtoRows rows) {
            return streamWriter.append(rows);
        }

        @Override
        public void close() {
            streamWriter.close();
        }
    }
}
