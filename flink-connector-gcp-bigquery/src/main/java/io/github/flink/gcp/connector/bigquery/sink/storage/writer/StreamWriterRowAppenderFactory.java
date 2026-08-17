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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.ConnectionWorkerPool;
import com.google.cloud.bigquery.storage.v1.GetWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.ProtoSchemaConverter;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.protobuf.Descriptors;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigquery.BigQueryCredentials;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.github.flink.gcp.connector.bigquery.sink.storage.DefaultStreamOptions;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

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
 *
 * <p>An {@link EmulatorEndpoint} switches all of that off and opens a per-destination client
 * instead, because the pool speaks to the production service with production credentials and cannot
 * be pointed elsewhere. That branch also carries three deviations the goccy emulator requires; each
 * is tracked upstream separately, and the removal schedule is {@code docs/adr/0029}:
 *
 * <ul>
 *   <li>the emulator registers a table's default stream only when {@code GetWriteStream} is called
 *       with the {@code .../streams/_default} name form, and {@code AppendRows} then matches that
 *       exact name — so the stream is primed and that name form is used, where the production path
 *       uses the {@code .../_default} short form the service also accepts
 *       (goccy/bigquery-emulator#342 — merged upstream but unreleased: v0.8.1 shipped 2026-06-13,
 *       the issue closed the day after)
 *   <li>a missing table surfaces from {@code GetWriteStream} as {@code UNKNOWN} instead of {@code
 *       NOT_FOUND}; that one status is translated so {@link
 *       io.github.flink.gcp.connector.bigquery.sink.CreateDisposition#CREATE_IF_NEEDED} handling
 *       reacts to it, and every other status is left alone (goccy/bigquery-emulator#504 — not
 *       covered by the goccy/bigquery-emulator#342 fix, and pinned by {@code
 *       BigQueryEmulatorMissingTableDeviationITCase})
 *   <li>appends on a connection opened after an earlier one closed are silently dropped past the
 *       first, so no connection pool is enabled and no JVM-global pool bounds are applied (the
 *       goccy/bigquery-emulator#342 fix also covers this: 0.8.1 routed a follow-up request's empty
 *       stream name to an arbitrary registered stream)
 * </ul>
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
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;
    private transient CredentialsProvider credentialsProvider;

    /**
     * Creates the factory against the production service.
     *
     * @param options the SDK-facing tuning knobs; the factory is shipped inside the job graph, so
     *     the options travel with it
     */
    public StreamWriterRowAppenderFactory(DefaultStreamOptions options) {
        this(options, null, null);
    }

    /**
     * Creates the factory.
     *
     * @param options the SDK-facing tuning knobs; the factory is shipped inside the job graph, so
     *     the options travel with it
     * @param emulatorEndpoint the emulator to append to, or {@code null} for the production service
     */
    public StreamWriterRowAppenderFactory(
            DefaultStreamOptions options, @Nullable EmulatorEndpoint emulatorEndpoint) {
        this(options, null, emulatorEndpoint);
    }

    /** Creates the factory with optional runtime-loaded production credentials. */
    public StreamWriterRowAppenderFactory(
            DefaultStreamOptions options,
            @Nullable String serviceAccountKeyFile,
            @Nullable EmulatorEndpoint emulatorEndpoint) {
        this.options = Objects.requireNonNull(options, "options must not be null");
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.emulatorEndpoint = emulatorEndpoint;
    }

    @Override
    public RowAppender create(
            TableDestination destination, Descriptors.Descriptor rowDescriptor, String location)
            throws IOException {
        if (emulatorEndpoint != null) {
            return createAgainstEmulator(destination, rowDescriptor);
        }
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
        configureCredentials(builder::setCredentialsProvider);
        if (location != null) {
            builder.setLocation(location);
        }
        StreamWriter streamWriter = builder.build();
        return new StreamWriterRowAppender(streamWriter, null);
    }

    @VisibleForTesting
    void configureCredentials(Consumer<CredentialsProvider> setter) throws IOException {
        if (serviceAccountKeyFile == null) {
            return;
        }
        if (credentialsProvider == null) {
            credentialsProvider =
                    FixedCredentialsProvider.create(
                            BigQueryCredentials.load(serviceAccountKeyFile));
        }
        setter.accept(credentialsProvider);
    }

    /**
     * Opens an appender against the emulator: a dedicated client, the primed {@code
     * .../streams/_default} name, and no pool. The location is not forwarded — the emulator has no
     * regions, and the routing hint only picks a production endpoint.
     *
     * <p>The client is closed on every failure path here, since no appender is returned to own it —
     * in a {@code finally} rather than per catch clause, so an {@link Error} does not leak it
     * either.
     */
    private RowAppender createAgainstEmulator(
            TableDestination destination, Descriptors.Descriptor rowDescriptor) throws IOException {
        String streamName = destination.toTablePath() + "/streams/_default";
        BigQueryWriteClient client = BigQueryWriteClients.forEmulator(emulatorEndpoint);
        StreamWriter streamWriter = null;
        try {
            client.getWriteStream(GetWriteStreamRequest.newBuilder().setName(streamName).build());
            streamWriter =
                    StreamWriter.newBuilder(streamName, client)
                            .setWriterSchema(ProtoSchemaConverter.convert(rowDescriptor))
                            .setRetrySettings(toRetrySettings(options))
                            .setMaxRetryDuration(options.getMaxRetryDuration())
                            .setTraceId(TRACE_ID)
                            .build();
        } catch (ApiException e) {
            // Only the emulator's own mistranslation is rewritten. Anything else — an emulator that
            // is not listening, a request it rejects — must keep its status, or a create-if-needed
            // writer would answer it by creating a table that already exists and retrying forever.
            if (e.getStatusCode().getCode() != StatusCode.Code.UNKNOWN) {
                throw e;
            }
            throw new NotFoundException(e, GrpcStatusCode.of(Status.Code.NOT_FOUND), false);
        } finally {
            if (streamWriter == null) {
                client.close();
            }
        }
        return new StreamWriterRowAppender(streamWriter, client);
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
     * factory wins; see {@code APPLIED_POOL_BOUNDS}. Called before {@code StreamWriter.build()} so
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

        /**
         * The client the writer is bound to, when this appender owns one. Null on the production
         * path, where the writer draws its connection from the SDK's JVM-static pool and there is
         * nothing per-appender to release.
         */
        @Nullable private final BigQueryWriteClient client;

        StreamWriterRowAppender(StreamWriter streamWriter, @Nullable BigQueryWriteClient client) {
            this.streamWriter = streamWriter;
            this.client = client;
        }

        @Override
        public ApiFuture<AppendRowsResponse> append(ProtoRows rows) {
            return streamWriter.append(rows);
        }

        @Override
        public void close() {
            try {
                streamWriter.close();
            } finally {
                if (client != null) {
                    client.close();
                }
            }
        }
    }
}
