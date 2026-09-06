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

package io.github.flink.gcp.connector.bigtable.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.Preconditions;

import com.google.api.gax.core.CredentialsProvider;
import io.github.flink.gcp.connector.base.failure.DefaultFailureHandlerContext;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.bigtable.BigtableCredentials;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.mutaterows.writer.BigtableWriter;
import io.github.flink.gcp.connector.bigtable.sink.mutaterows.writer.DefaultMutationBatcherFactory;
import io.github.flink.gcp.connector.bigtable.sink.mutaterows.writer.MutationBatcherFactory;
import io.github.flink.gcp.connector.bigtable.sink.tables.BigtableTableAdmin;
import io.github.flink.gcp.connector.bigtable.sink.tables.TableAdmin;

import javax.annotation.Nullable;

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
    @Nullable private final TableDestination initialDestination;
    @Nullable private final TableCreateOptions expectedFamilies;

    /**
     * Creates the sink; called by {@link BigtableSinkBuilder}.
     *
     * @param config the sink configuration
     */
    public BigtableMutateRowsSink(BigtableSinkConfig<T> config) {
        this(config, null, null);
    }

    /**
     * Creates a Table sink that validates its fixed destination when its writer starts. The
     * destination and expected families must either both be supplied or both be absent.
     */
    public BigtableMutateRowsSink(
            BigtableSinkConfig<T> config,
            @Nullable TableDestination initialDestination,
            @Nullable TableCreateOptions expectedFamilies) {
        Preconditions.checkArgument(
                (initialDestination == null) == (expectedFamilies == null),
                "initialDestination and expectedFamilies must both be supplied or both be absent");
        this.config = config;
        this.initialDestination = initialDestination;
        this.expectedFamilies = expectedFamilies;
    }

    /** Returns the sink configuration. */
    public BigtableSinkConfig<T> getConfig() {
        return config;
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) throws IOException {
        CredentialsProvider credentials =
                BigtableCredentials.loadDataAndTableAdmin(config.getServiceAccountKeyFile());
        return createWriter(
                context,
                new DefaultMutationBatcherFactory(
                        config.getAppProfileId(),
                        config.getWriterOptions(),
                        config.getEmulatorEndpoint(),
                        credentials),
                credentials);
    }

    /**
     * The production path, against an injectable factory.
     *
     * <p>The seam exists for one assertion the sink cannot otherwise make observable: that a failed
     * creation releases the {@link MutationBatcherFactory} it had already built, and not only the
     * failure handler. The production overload above is what a job calls, and it is one call, so
     * the two cannot drift.
     */
    @VisibleForTesting
    SinkWriter<T> createWriter(WriterInitContext context, MutationBatcherFactory factory)
            throws IOException {
        return createWriter(context, factory, null);
    }

    private SinkWriter<T> createWriter(
            WriterInitContext context,
            MutationBatcherFactory factory,
            @Nullable CredentialsProvider credentials)
            throws IOException {
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
        // Both constructed unconditionally, and neither opens a connection: the factory builds a
        // client per instance on the first record routed there, and the admin's client is per-call,
        // built for schema validation or a creation repair.
        TableAdmin tableAdmin = new BigtableTableAdmin(config.getEmulatorEndpoint(), credentials);
        try {
            BigtableWriter<T> writer =
                    (BigtableWriter<T>)
                            createWriter(
                                    factory,
                                    tableAdmin,
                                    context.getMailboxExecutor(),
                                    context.metricGroup());
            if (initialDestination != null) {
                writer.prepareTable(initialDestination, expectedFamilies);
            }
            return writer;
        } catch (Throwable e) {
            // No writer is returned to Flink when construction or startup validation fails, so
            // this path releases its resources. The failure handler's contract promises a close
            // here too — a restart would otherwise open one more per attempt. This also covers
            // constructor preconditions on deserialized options.
            //
            // Throwable, not Exception: a client's first classload can fail with a
            // NoClassDefFoundError, which repeats on every attempt and would otherwise walk past
            // this guard. Precise rethrow keeps the declared throws clause honest, and it also
            // means a checked exception added to anything above stays covered.
            Closers.closeAllSuppressing(
                    e, factory, tableAdmin, config.getFailedMutationHandler()::close);
            throw e;
        }
    }

    /**
     * Creates the writer against an injected batcher factory and table admin. Deliberately does
     * <b>not</b> open the failure handler — that belongs to the production path above, so writer
     * tests injecting fakes need no {@link WriterInitContext}. Table-specific startup validation
     * also belongs to the production lifecycle above; writer tests may call {@link
     * BigtableWriter#prepareTable(TableDestination, TableCreateOptions)} explicitly.
     */
    @VisibleForTesting
    public SinkWriter<T> createWriter(
            MutationBatcherFactory factory,
            TableAdmin tableAdmin,
            MailboxExecutor mailboxExecutor,
            SinkWriterMetricGroup metricGroup) {
        return new BigtableWriter<>(config, factory, tableAdmin, mailboxExecutor, metricGroup);
    }
}
