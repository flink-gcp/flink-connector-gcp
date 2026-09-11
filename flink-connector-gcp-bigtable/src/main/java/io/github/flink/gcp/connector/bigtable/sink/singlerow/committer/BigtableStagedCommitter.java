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

package io.github.flink.gcp.connector.bigtable.sink.singlerow.committer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.metrics.DestinationMetrics;
import io.github.flink.gcp.connector.bigtable.BigtableMetricNames;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableStagedOptions;
import io.github.flink.gcp.connector.bigtable.sink.ColumnFamilyType;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableCommittable;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowClient;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowClientFactory;
import io.github.flink.gcp.connector.bigtable.sink.tables.StagedTableValidator;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.function.LongSupplier;

/**
 * Bounded conditional requests on Flink's committer thread. An SDK failure fails the commit;
 * checkpoint recovery replays the unchanged envelope and its marker absorbs ambiguous successes.
 */
@Internal
public final class BigtableStagedCommitter implements Committer<BigtableCommittable> {
    /** Creates runtime factories using each restored envelope's original application profile. */
    @FunctionalInterface
    public interface ClientFactories {
        /** Creates an instance-client owner for the supplied profile. */
        SingleRowClientFactory create(String profile) throws IOException;
    }

    private final BigtableStagedOptions options;
    private final StagedTableValidator validator;
    private final ClientFactories factories;
    private final Map<String, ColumnFamilyType> expectedFamilies;
    private final Map<String, ClientState> clients = new LinkedHashMap<>(16, 0.75f, true);
    private final ArrayDeque<Pending> pending = new ArrayDeque<>();
    private final Set<ApiFuture<Boolean>> active = new HashSet<>();
    private final Counter deduplicated;
    private final Counter completed;
    private final Counter failed;
    private final DestinationMetrics destinations;
    private final LongSupplier nanoClock;
    private volatile boolean closed;

    /** Creates a committer without obtaining a client or performing a target write. */
    public BigtableStagedCommitter(
            BigtableStagedOptions options,
            StagedTableValidator validator,
            ClientFactories factories,
            Map<String, ColumnFamilyType> expectedFamilies,
            MetricGroup metrics) {
        this(options, validator, factories, expectedFamilies, metrics, System::nanoTime);
    }

    BigtableStagedCommitter(
            BigtableStagedOptions options,
            StagedTableValidator validator,
            ClientFactories factories,
            Map<String, ColumnFamilyType> expectedFamilies,
            MetricGroup metrics,
            LongSupplier nanoClock) {
        this.nanoClock = nanoClock;
        options.validate();
        this.options = options;
        this.validator = validator;
        this.factories = factories;
        this.expectedFamilies = Map.copyOf(expectedFamilies);
        destinations =
                DestinationMetrics.of(
                        metrics, options.getRequestOptions().isPerDestinationMetrics());
        deduplicated = metrics.counter(BigtableMetricNames.REQUESTS_DEDUPLICATED);
        completed = metrics.counter(BigtableMetricNames.REQUESTS_COMPLETED);
        failed = metrics.counter(BigtableMetricNames.REQUESTS_FAILED);
    }

    @Override
    public void commit(Collection<CommitRequest<BigtableCommittable>> requests)
            throws IOException, InterruptedException {
        boolean success = false;
        try {
            if (closed) {
                throw new IOException("Bigtable staged committer is closed");
            }
            evictIdle();
            for (CommitRequest<BigtableCommittable> request : requests) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("Bigtable commit interrupted");
                }
                if (pending.size() >= options.getRequestOptions().getMaxInFlightRequests()) {
                    finishOne();
                }
                BigtableCommittable value = request.getCommittable();
                SingleRowClient client = client(value);
                synchronized (active) {
                    if (closed) {
                        throw new IOException("Bigtable staged committer is closed");
                    }
                    DestinationMetrics.Counters destinationMetrics =
                            destinations.forDestination(value.getDestination().toString());
                    destinationMetrics.recordSent();
                    ApiFuture<Boolean> future =
                            client.checkAndMutateRow(
                                    ConditionalRowMutation.fromProto(value.getRequest()));
                    active.add(future);
                    pending.addLast(new Pending(request, future, destinationMetrics));
                }
            }
            drain();
            success = true;
        } finally {
            if (!success) {
                failed.inc();
                cancelPending();
            }
        }
    }

    private SingleRowClient client(BigtableCommittable value)
            throws IOException, InterruptedException {
        TableDestination destination = value.getDestination();
        String profile = value.getRequest().getAppProfileId();
        String key = destination.getProject() + "/" + destination.getInstance() + "/" + profile;
        ClientState state = clients.get(key);
        if (state == null) {
            if (clients.size() >= options.getRequestOptions().getMaxActiveInstances()) {
                drain();
                String oldest = clients.keySet().iterator().next();
                closeClient(clients.remove(oldest));
            }
            state = new ClientState(factories.create(profile));
            clients.put(key, state);
        }
        TableState table = state.tables.get(destination);
        if (table == null || !table.validated.contains(value.getMarkerFamily())) {
            validator.validate(destination, profile, value.getMarkerFamily(), expectedFamilies);
            if (table == null) {
                table = new TableState(state.factory.create(destination));
                state.tables.put(destination, table);
            }
            table.validated.add(value.getMarkerFamily());
        }
        table.lastAccess = nanoClock.getAsLong();
        return table.client;
    }

    private void finishOne() throws IOException, InterruptedException {
        Pending current = pending.removeFirst();
        try {
            if (current.future.get()) {
                current.request.signalAlreadyCommitted();
                deduplicated.inc();
            }
            completed.inc();
        } catch (ExecutionException | CancellationException failure) {
            current.metrics.sendFailed();
            throw new IOException(
                    "Bigtable staged commit failed; restore its checkpoint to retry the same envelope",
                    failure);
        } finally {
            synchronized (active) {
                active.remove(current.future);
            }
            if (!current.future.isDone()) {
                current.future.cancel(true);
            }
        }
    }

    private void drain() throws IOException, InterruptedException {
        while (!pending.isEmpty()) {
            finishOne();
        }
    }

    private void cancelPending() {
        synchronized (active) {
            for (ApiFuture<Boolean> future : active) {
                future.cancel(true);
            }
            active.clear();
        }
        pending.clear();
    }

    private void evictIdle() throws IOException {
        long now = nanoClock.getAsLong();
        long idleNanos = options.getRequestOptions().getDestinationIdleTimeout().toNanos();
        var iterator = clients.entrySet().iterator();
        while (iterator.hasNext()) {
            ClientState state = iterator.next().getValue();
            var tables = state.tables.entrySet().iterator();
            while (tables.hasNext()) {
                var table = tables.next();
                if (now - table.getValue().lastAccess > idleNanos) {
                    try {
                        state.factory.release(table.getKey());
                    } catch (Exception failure) {
                        throw new IOException("Failed to release idle Bigtable table", failure);
                    }
                    tables.remove();
                }
            }
            if (state.tables.isEmpty()) {
                iterator.remove();
                closeClient(state);
            }
        }
    }

    private static void closeClient(ClientState state) throws IOException {
        try {
            state.factory.close();
        } catch (Exception failure) {
            throw new IOException("Failed to close Bigtable staged clients", failure);
        }
    }

    @Override
    public void close() throws Exception {
        synchronized (active) {
            closed = true;
            for (ApiFuture<Boolean> future : active) {
                future.cancel(true);
            }
            active.clear();
        }
        List<AutoCloseable> owners = new ArrayList<>();
        clients.values().forEach(state -> owners.add(state.factory));
        clients.clear();
        Closers.closeAll(owners);
    }

    private static final class ClientState {
        final SingleRowClientFactory factory;
        final Map<TableDestination, TableState> tables = new HashMap<>();

        ClientState(SingleRowClientFactory factory) {
            this.factory = factory;
        }
    }

    private static final class TableState {
        final SingleRowClient client;
        final Set<String> validated = new HashSet<>();
        long lastAccess;

        TableState(SingleRowClient client) {
            this.client = client;
        }
    }

    private static final class Pending {
        final CommitRequest<BigtableCommittable> request;
        final ApiFuture<Boolean> future;
        final DestinationMetrics.Counters metrics;

        Pending(
                CommitRequest<BigtableCommittable> request,
                ApiFuture<Boolean> future,
                DestinationMetrics.Counters metrics) {
            this.metrics = metrics;
            this.request = request;
            this.future = future;
        }
    }
}
