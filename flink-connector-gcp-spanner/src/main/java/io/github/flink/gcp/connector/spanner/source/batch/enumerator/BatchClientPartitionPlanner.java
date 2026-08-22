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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.DatabaseClient;
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
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.spanner.SpannerClients;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperation;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperationResolution;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Plans a read through a {@code google-cloud-spanner} {@link BatchClient}.
 *
 * <p>Named after the client its {@link #close()} releases, which is the connector's convention for
 * the real implementation of a seam.
 *
 * <p>The handle is built on first use and closed once, both on the coordinator's side of the job:
 * the enumerator plans a read once and then has no further use for it. Readers open handles of
 * their own and rejoin the snapshot by its id.
 *
 * <p><b>What {@link #close()} owes.</b> A batch read holds a session on the service, and {@code
 * cleanup()} is what releases it. This connector calls it, and it is the enumerator's to call
 * rather than a reader's — a reader that released the session would end every other reader's read.
 * Against this client version the call happens to cost nothing, because {@code BatchClientImpl}
 * serves batch transactions from a multiplexed session and {@code SessionImpl.close()} returns
 * immediately for one; the call stays because it is the documented contract, and because a client
 * that stopped multiplexing would need it.
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

    private final SpannerDatabase database;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;

    /**
     * The service handle and the batch transaction on it, both built by {@link #plan}.
     *
     * <p>{@code volatile} and guarded by the monitor below because {@link #plan} runs on the
     * executor {@code SplitEnumeratorContext#callAsync} hands the work to, while {@link #close()}
     * runs on the scheduler thread and may overtake it.
     */
    @Nullable private volatile Spanner spanner;

    /**
     * What the enumerator loaded and handed over, or {@code null} for application default
     * credentials.
     *
     * <p>{@code volatile} because {@link #useCredentials} runs on the thread that creates or
     * restores the enumerator while {@link #plan} reads it on the {@code callAsync} executor.
     */
    @Nullable private volatile GoogleCredentials credentials;

    @Nullable private volatile BatchReadOnlyTransaction transaction;

    private volatile boolean closed;

    /**
     * Creates the planner.
     *
     * @param database the database to read
     * @param emulatorEndpoint the emulator endpoint (plaintext, no credentials), or {@code null}
     *     for the real service
     */
    public BatchClientPartitionPlanner(
            SpannerDatabase database, @Nullable EmulatorEndpoint emulatorEndpoint) {
        this.database = database;
        this.emulatorEndpoint = emulatorEndpoint;
    }

    @Override
    public PartitionPlan plan(
            SpannerReadOperation operation,
            TimestampBound bound,
            PartitionOptions partitionOptions,
            boolean dataBoostEnabled,
            @Nullable SpannerRpcPriority rpcPriority)
            throws IOException {
        BatchReadOnlyTransaction txn = open(bound);
        SpannerReadOperation resolved =
                SpannerReadOperationResolution.resolve(
                        operation, databaseClient(), txn.getReadTimestamp());
        List<Partition> partitions =
                partition(txn, resolved, partitionOptions, dataBoostEnabled, rpcPriority);
        return new PartitionPlan(txn.getBatchTransactionId(), txn.getReadTimestamp(), partitions);
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
     * Opens the batch transaction, building the service handle on first use.
     *
     * <p>The monitor is this object rather than a lock field: one planner belongs to one
     * enumerator, so the only contention is between that enumerator's planning call and its
     * teardown, and a separate lock would name nothing the object does not already name.
     */
    private BatchReadOnlyTransaction open(TimestampBound bound) throws IOException {
        BatchClient batchClient;
        synchronized (this) {
            checkOpen();
            if (spanner == null) {
                spanner = SpannerClients.open(database, settings());
            }
            batchClient =
                    spanner.getBatchClient(
                            DatabaseId.of(
                                    database.getProject(),
                                    database.getInstance(),
                                    database.getDatabase()));
        }
        // Opened outside the monitor, because this call begins the transaction on the service and
        // close() runs on the scheduler thread: holding the monitor across it would make a job
        // being torn down wait for a round trip to Spanner.
        BatchReadOnlyTransaction opened = batchClient.batchReadOnlyTransaction(bound);
        synchronized (this) {
            if (closed) {
                // Teardown overtook this call and released what it could see, which did not include
                // this transaction. Releasing it here is what keeps the session it holds from
                // outliving the enumerator.
                release(opened, null);
                checkOpen();
            }
            transaction = opened;
        }
        return opened;
    }

    @Override
    public void useCredentials(@Nullable GoogleCredentials credentials) {
        this.credentials = credentials;
    }

    /** Builds settings exposed for verifying that the enumerator injects runtime credentials. */
    @VisibleForTesting
    SpannerOptions settings() throws IOException {
        return SpannerClients.settings(database, emulatorEndpoint, credentials);
    }

    private DatabaseClient databaseClient() throws IOException {
        synchronized (this) {
            checkOpen();
            if (spanner == null) {
                throw new IOException(
                        "The Spanner partition planner did not open its service handle.");
            }
            return spanner.getDatabaseClient(
                    DatabaseId.of(
                            database.getProject(), database.getInstance(), database.getDatabase()));
        }
    }

    private void checkOpen() throws IOException {
        if (closed) {
            throw new IOException(
                    "The Spanner partition planner for "
                            + database
                            + " was closed before it was used.");
        }
    }

    @Override
    public void close() throws IOException {
        BatchReadOnlyTransaction openTransaction;
        Spanner openSpanner;
        synchronized (this) {
            closed = true;
            openTransaction = transaction;
            transaction = null;
            openSpanner = spanner;
            spanner = null;
        }
        release(openTransaction, openSpanner);
    }

    /**
     * Releases a batch read, in three steps that are three different things: the transaction
     * handle, the session the read holds on the service, and the client.
     *
     * <p>{@code Closers} continues past a failure in any of them, so a session that cannot be
     * released still does not leak the channel behind it.
     */
    private void release(
            @Nullable BatchReadOnlyTransaction openTransaction, @Nullable Spanner openSpanner)
            throws IOException {
        try {
            Closers.closeAll(
                    openTransaction,
                    openTransaction == null ? null : openTransaction::cleanup,
                    openSpanner);
        } catch (Exception e) {
            throw new IOException(
                    "Failed to release the Spanner batch read of " + database + ".", e);
        }
    }
}
