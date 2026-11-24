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

package io.github.flink.gcp.connector.spanner.source.batch.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Options.QueryOption;
import com.google.cloud.spanner.Options.ReadOption;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.TimestampBound;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.SpannerClients;
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperation;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperationResolution;
import io.grpc.Context;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;

/**
 * Plans a read through a {@code google-cloud-spanner} {@link BatchClient}.
 *
 * <p>Named after the client it plans through, which is the connector's convention for the real
 * implementation of a seam.
 *
 * <p><b>The handle lives as long as one {@link #plan} call.</b> The enumerator plans a read once
 * and then has no further use for it, and on the JobManager a handle must not stay open until the
 * enumerator closes: Flink 2.2.1 closes a source's enumerator on a thread of its own, does not wait
 * for it, and in practice releases the job's class loader first, and a handle still open then can
 * exit the JobManager (docs/adr/0208, measured on BigQuery's source in #2237). So {@link #plan}
 * opens the handle, under a {@code GrpcEventLoops} lease, and closes it before it returns. Readers
 * open handles of their own and rejoin the snapshot by its id; the snapshot does not depend on this
 * handle staying open. A {@link #close()} that lands while a call is in flight cancels the call
 * through the gRPC {@link Context} it runs in (gRPC cancels a call when the context it was started
 * in is cancelled), closes the handle, and returns once the call has released it; that wait has no
 * bound of its own beyond the client's shutdown, and an interrupt ends it.
 *
 * <p>A deferred read operation (a Table API scan or FULL cache load that reads through {@code
 * scan.index}, or whose primary-key filter depends on the key's sort order) resolves through the
 * handle's database client, which creates a multiplexed session of its own in the background, out
 * of the context's reach; a close while that creation is in flight waits for the client's bounded
 * shutdown instead. Any other operation, every DataStream API read included, does not build the
 * database client.
 *
 * <p><b>{@code cleanup()} is not called.</b> It is the documented way to release the session a
 * batch read holds, and it would be the enumerator's to call rather than a reader's, since a reader
 * that released the session would end every other reader's read. But it can only be called while
 * the handle is open, which on the JobManager ends with {@link #plan}, before any reader has read.
 * Against this client version (6.127.0) the call would cost nothing anyway: {@code BatchClientImpl}
 * serves every batch transaction from a multiplexed session, which the client never deletes and
 * which expires on its own, and {@code SessionImpl.close()} returns immediately for one. Were a
 * later client to stop multiplexing, a session left behind here would idle out on the service,
 * which is the cost to prefer over ending the readers' reads.
 *
 * <p>The client's retry configuration is left alone. It retries the partition calls on the
 * transient codes under a total timeout of its own, so a failure that reaches the enumerator has
 * already exhausted the retry the client owns.
 *
 * <p>One planner belongs to one enumerator: {@link DefaultPartitionPlannerFactory} mints it, and
 * the source mints one per {@code createEnumerator} and {@code restoreEnumerator}. That is what
 * makes the one-way {@code closed} flag below correct — it ends this object rather than poisoning
 * one the next enumerator will also be handed ({@code docs/adr/0128}).
 */
@Internal
public final class BatchClientPartitionPlanner implements PartitionPlanner {

    private final DatabaseDestination database;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;
    private final boolean exportClientMetricsToCloudMonitoring;

    /**
     * The call {@link #plan} is making, and the handle it opened for it, or {@code null} between
     * calls.
     *
     * <p>Guarded by the monitor because {@link #plan} runs on the executor {@code
     * SplitEnumeratorContext#callAsync} hands the work to, while {@link #close()} runs on the
     * scheduler thread and may overtake it.
     */
    @GuardedBy("this")
    @Nullable
    private InFlight call;

    @GuardedBy("this")
    @Nullable
    private GrpcEventLoops.LeasedClient<Spanner> spanner;

    /**
     * What the enumerator loaded and handed over, or {@code null} for application default
     * credentials.
     *
     * <p>{@code volatile} because {@link #useCredentials} runs on the thread that creates or
     * restores the enumerator while {@link #plan} reads it on the {@code callAsync} executor.
     */
    @Nullable private volatile GoogleCredentials credentials;

    /**
     * The source's client metrics for its enumerators, or {@code null} when nothing is exported to
     * Flink. {@code volatile} for the reason {@code credentials} is; never closed here, because
     * they outlive this planner.
     */
    @Nullable private volatile ClientMetrics clientMetrics;

    @GuardedBy("this")
    private boolean closed;

    /**
     * Creates the planner.
     *
     * @param database the database to read
     * @param emulatorEndpoint the emulator endpoint (plaintext, no credentials), or {@code null}
     *     for the real service
     */
    public BatchClientPartitionPlanner(
            DatabaseDestination database, @Nullable EmulatorEndpoint emulatorEndpoint) {
        this(
                database,
                emulatorEndpoint,
                SpannerClients.DEFAULT_EXPORT_CLIENT_METRICS_TO_CLOUD_MONITORING);
    }

    /**
     * Creates the planner with the Cloud Monitoring choice.
     *
     * @param database the database to read
     * @param emulatorEndpoint the emulator endpoint (plaintext, no credentials), or {@code null}
     *     for the real service
     * @param exportClientMetricsToCloudMonitoring whether the client keeps its built-in Cloud
     *     Monitoring export
     */
    public BatchClientPartitionPlanner(
            DatabaseDestination database,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            boolean exportClientMetricsToCloudMonitoring) {
        this.database = database;
        this.emulatorEndpoint = emulatorEndpoint;
        this.exportClientMetricsToCloudMonitoring = exportClientMetricsToCloudMonitoring;
    }

    @Override
    public PartitionPlan plan(
            SpannerReadOperation operation,
            TimestampBound bound,
            PartitionOptions partitionOptions,
            boolean dataBoostEnabled,
            @Nullable SpannerRpcPriority rpcPriority)
            throws IOException {
        InFlight inFlight = new InFlight();
        synchronized (this) {
            checkOpen();
            Preconditions.checkState(call == null, "A plan is already being made.");
            call = inFlight;
        }
        Context previous = inFlight.context.attach();
        try {
            GrpcEventLoops.LeasedClient<Spanner> opened = openClient();
            PartitionPlan plan;
            try {
                plan =
                        planOn(
                                opened,
                                operation,
                                bound,
                                partitionOptions,
                                dataBoostEnabled,
                                rpcPriority);
            } catch (Throwable e) {
                // The planning failure is the one to report: a query the service will not
                // partition says why in its message, and a close failure must not replace it.
                Closers.closeAllSuppressing(e, () -> closeClient(opened));
                throw e;
            }
            closeClient(opened);
            return plan;
        } finally {
            inFlight.context.detach(previous);
            synchronized (this) {
                call = null;
            }
            // Releases what the context holds; the call is over either way.
            inFlight.context.close();
            inFlight.ended.countDown();
        }
    }

    private PartitionPlan planOn(
            GrpcEventLoops.LeasedClient<Spanner> opened,
            SpannerReadOperation operation,
            TimestampBound bound,
            PartitionOptions partitionOptions,
            boolean dataBoostEnabled,
            @Nullable SpannerRpcPriority rpcPriority)
            throws IOException {
        DatabaseId databaseId =
                DatabaseId.of(
                        database.getProject(), database.getInstance(), database.getDatabase());
        BatchClient batchClient = opened.client().getBatchClient(databaseId);
        // The handle is closed with the call; cleanup() is not called (see the class javadoc).
        try (BatchReadOnlyTransaction txn = batchClient.batchReadOnlyTransaction(bound)) {
            // Only a deferred operation needs the database client, whose creation starts a
            // multiplexed session of its own that the handle's close would then wait for.
            SpannerReadOperation resolved =
                    !SpannerReadOperationResolution.isDeferred(operation)
                            ? operation
                            : SpannerReadOperationResolution.resolve(
                                    operation,
                                    opened.client().getDatabaseClient(databaseId),
                                    txn.getReadTimestamp());
            List<Partition> partitions =
                    partition(txn, resolved, partitionOptions, dataBoostEnabled, rpcPriority);
            return new PartitionPlan(
                    txn.getBatchTransactionId(), txn.getReadTimestamp(), partitions);
        }
    }

    /**
     * Assembles the options a partition call carries.
     *
     * <p>They travel further than the call they are given to: the partition the service answers
     * with holds the options it was planned under, and {@code execute} replays them — so the
     * priority reaches the streaming read that actually moves the rows, which is the request whose
     * load on the instance a job cares about. It does <em>not</em> reach the {@code PartitionQuery}
     * call itself, which the client sends under the session's own options.
     *
     * <p>Assembled twice, once per option family, because the client library gives the two values
     * no common supertype: {@code dataBoostEnabled} answers with a {@code ReadAndQueryOption} and
     * {@code priority} with a {@code ReadQueryUpdateTransactionOption}, which are siblings. Both
     * are a {@code ReadOption} and a {@code QueryOption}, so each call site gets its own array —
     * and a cast to unify them compiles and then fails at run time, which is why there is not one.
     */
    @VisibleForTesting
    static QueryOption[] queryOptions(
            boolean dataBoostEnabled, @Nullable SpannerRpcPriority rpcPriority) {
        List<QueryOption> options = new ArrayList<>(2);
        if (dataBoostEnabled) {
            options.add(Options.dataBoostEnabled(true));
        }
        if (rpcPriority != null) {
            options.add(Options.priority(rpcPriority.toSpanner()));
        }
        return options.toArray(new QueryOption[0]);
    }

    /**
     * The read-call counterpart of {@link #queryOptions}; see its javadoc for why there are two.
     */
    @VisibleForTesting
    static ReadOption[] readOptions(
            boolean dataBoostEnabled, @Nullable SpannerRpcPriority rpcPriority) {
        List<ReadOption> options = new ArrayList<>(2);
        if (dataBoostEnabled) {
            options.add(Options.dataBoostEnabled(true));
        }
        if (rpcPriority != null) {
            options.add(Options.priority(rpcPriority.toSpanner()));
        }
        return options.toArray(new ReadOption[0]);
    }

    /**
     * Makes the partition call the read operation asks for.
     *
     * <p>A {@link SpannerException} is left to travel as it is. A query the service will not
     * partition comes back as {@code INVALID_ARGUMENT} with a message naming what about the query
     * it could not distribute, and that message is the only thing that tells the user which part to
     * change.
     */
    private static List<Partition> partition(
            BatchReadOnlyTransaction txn,
            SpannerReadOperation operation,
            PartitionOptions partitionOptions,
            boolean dataBoostEnabled,
            @Nullable SpannerRpcPriority rpcPriority) {
        if (operation.isQuery()) {
            return txn.partitionQuery(
                    partitionOptions,
                    operation.getStatement(),
                    queryOptions(dataBoostEnabled, rpcPriority));
        }
        ReadOption[] options = readOptions(dataBoostEnabled, rpcPriority);
        if (operation.getIndex() == null) {
            return txn.partitionRead(
                    partitionOptions,
                    operation.getTable(),
                    operation.getKeys(),
                    operation.getColumns(),
                    options);
        }
        return txn.partitionReadUsingIndex(
                partitionOptions,
                operation.getTable(),
                operation.getIndex(),
                operation.getKeys(),
                operation.getColumns(),
                options);
    }

    /**
     * Opens the handle for the call in flight, unless {@link #close()} came first.
     *
     * <p>Opened outside the monitor, because building the handle can make a round trip (the client
     * checks an emulator is answering) and {@link #close()} must not wait for it to cancel the
     * call. The monitor is this object rather than a lock field: one planner belongs to one
     * enumerator, so the only contention is between that enumerator's planning call and its
     * teardown, and a separate lock would name nothing the object does not already name.
     */
    private GrpcEventLoops.LeasedClient<Spanner> openClient() throws IOException {
        synchronized (this) {
            checkOpen();
        }
        GrpcEventLoops.LeasedClient<Spanner> opened = SpannerClients.open(database, settings());
        synchronized (this) {
            if (!closed) {
                spanner = opened;
                return opened;
            }
        }
        // Teardown overtook the open and could not see this handle.
        opened.close();
        throw closedBeforeUse();
    }

    /** Closes the handle {@link #openClient()} opened, or waits for {@link #close()} to. */
    private void closeClient(GrpcEventLoops.LeasedClient<Spanner> opened) {
        synchronized (this) {
            if (spanner == opened) {
                spanner = null;
            }
        }
        opened.close();
    }

    @Override
    public void useCredentials(@Nullable GoogleCredentials credentials) {
        this.credentials = credentials;
    }

    @Override
    public void useClientMetrics(@Nullable ClientMetrics clientMetrics) {
        this.clientMetrics = clientMetrics;
    }

    /**
     * Builds settings exposed for verifying that the enumerator injects its runtime credentials and
     * client-metrics choices.
     */
    @VisibleForTesting
    SpannerOptions settings() throws IOException {
        return SpannerClients.settings(
                database,
                emulatorEndpoint,
                credentials,
                clientMetrics,
                exportClientMetricsToCloudMonitoring);
    }

    @GuardedBy("this")
    private void checkOpen() throws IOException {
        if (closed) {
            throw closedBeforeUse();
        }
    }

    private IOException closedBeforeUse() {
        return new IOException(
                "The Spanner partition planner for "
                        + database
                        + " was closed before it was used.");
    }

    /**
     * Ends the planner: a call in flight is cancelled and its handle closed, and a later call
     * fails.
     *
     * <p>The cancellation comes first, so that the handle's close, which waits for its channels to
     * finish their calls, does not wait out one the planner no longer wants. The close then waits
     * for the call to end, so that the call's handle is closed and its lease released when it
     * returns, whichever thread got to the handle; an interrupt ends that wait early and is
     * restored.
     */
    @Override
    public void close() {
        InFlight inFlight;
        GrpcEventLoops.LeasedClient<Spanner> openSpanner;
        synchronized (this) {
            closed = true;
            inFlight = call;
            openSpanner = spanner;
            spanner = null;
        }
        if (inFlight != null) {
            inFlight.context.cancel(
                    new CancellationException(
                            "The Spanner partition planner for " + database + " was closed."));
        }
        if (openSpanner != null) {
            openSpanner.close();
        }
        if (inFlight != null) {
            try {
                inFlight.ended.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** One {@link #plan} call: the context it runs in, and whether it has ended. */
    private static final class InFlight {

        /**
         * Not the calling thread's context: Flink's threads carry none worth inheriting, and a
         * deadline set on one would end the plan for a reason this class does not know about.
         */
        private final Context.CancellableContext context = Context.ROOT.withCancellation();

        private final CountDownLatch ended = new CountDownLatch(1);
    }
}
