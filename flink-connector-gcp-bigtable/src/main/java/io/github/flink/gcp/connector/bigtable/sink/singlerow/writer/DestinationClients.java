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

import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.bigtable.BigtableDataClients;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * The tables a runtime holds a client lease for, grouped by instance in least-recently-used order.
 *
 * <p>Touched only on the task thread, by both surfaces: the sink writer's {@code write} and {@code
 * flush}, and the async function's {@code asyncInvoke} and {@code timeout}, which Flink runs on the
 * task mailbox. The one cross-thread field is {@code activeClients}, a volatile mirror of the
 * instance map's size for the metric reporter, which must not walk a concurrently mutating map. The
 * instance map is access-ordered, so looking a table up makes its instance recent — that is the LRU
 * an eviction reads.
 *
 * <p>The bookkeeping here is the part of {@code BigtableWriter}'s destination pool that a client
 * lease needs whether or not a batcher sits over it; what it does not carry is any wait, so the
 * surface that can block drains before it evicts, and the one that cannot evicts only an idle
 * instance.
 */
@Internal
final class DestinationClients {

    private static final Logger LOG = LoggerFactory.getLogger(DestinationClients.class);

    private final SingleRowClientFactory factory;
    private final SingleRowRequestMetrics metrics;
    private final LongSupplier nanoClock;

    /**
     * Insertion-ordered so a teardown reports the first failure of a deterministic sequence, and so
     * an eviction sweep visits tables in the order they were first written to.
     */
    private final Map<TableDestination, DestinationState> states = new LinkedHashMap<>();

    private final Map<String, Set<TableDestination>> instanceDestinations =
            new LinkedHashMap<>(16, 0.75f, true);

    private volatile int activeClients;

    DestinationClients(
            SingleRowClientFactory factory,
            SingleRowRequestMetrics metrics,
            LongSupplier nanoClock) {
        this.factory = factory;
        this.metrics = metrics;
        this.nanoClock = nanoClock;
    }

    /**
     * Returns the table's state if it is held, making its instance the most recently used.
     *
     * @param destination the table
     * @return the state, or {@code null} if the table is not held
     */
    @Nullable
    DestinationState find(TableDestination destination) {
        DestinationState state = states.get(destination);
        if (state != null) {
            // This access-ordered map owns the instance LRU. Touching any of its tables makes the
            // shared instance recent even though the destination state itself was already found.
            instanceDestinations.get(state.instanceKey);
        }
        return state;
    }

    /** Returns whether a client for the table's instance is held, for any of its tables. */
    boolean holdsInstance(TableDestination destination) {
        return instanceDestinations.containsKey(BigtableDataClients.instanceKey(destination));
    }

    /** Returns the number of instance clients held. */
    int instanceCount() {
        return instanceDestinations.size();
    }

    /**
     * Opens the table: leases its instance's client from the factory and records the state.
     *
     * <p>Leased and then recorded, so a failure leaves no half-populated entry and the next record
     * routed here retries. The failure is thrown at the caller rather than routed: a client that
     * cannot be built is a configuration or credentials failure, not a bad record.
     *
     * @param destination the table, not yet held
     * @return the new state
     * @throws IOException if the factory cannot lease a client
     * @throws InterruptedException if interrupted while the factory builds one
     */
    DestinationState open(TableDestination destination) throws IOException, InterruptedException {
        String instanceKey = BigtableDataClients.instanceKey(destination);
        SingleRowClient client;
        try {
            client = factory.create(destination);
        } catch (IOException | RuntimeException e) {
            throw new IOException(
                    "Failed to create a Bigtable client for table " + destination + ".", e);
        }
        DestinationState state =
                new DestinationState(
                        destination,
                        instanceKey,
                        client,
                        metrics.forTable(destination),
                        nanoClock.getAsLong());
        states.put(destination, state);
        Set<TableDestination> destinations = instanceDestinations.get(instanceKey);
        if (destinations == null) {
            destinations = new LinkedHashSet<>();
            instanceDestinations.put(instanceKey, destinations);
            activeClients = instanceDestinations.size();
        }
        destinations.add(destination);
        return state;
    }

    /**
     * Returns the least recently used instance.
     *
     * @return its key, or {@code null} if none is held
     */
    @Nullable
    String leastRecentlyUsedInstance() {
        Iterator<String> keys = instanceDestinations.keySet().iterator();
        return keys.hasNext() ? keys.next() : null;
    }

    /**
     * Returns the least recently used instance none of whose tables has a request in flight — what
     * a surface that cannot wait may evict.
     *
     * @return its key, or {@code null} if every held instance is busy
     */
    @Nullable
    String leastRecentlyUsedIdleInstance() {
        for (Map.Entry<String, Set<TableDestination>> entry : instanceDestinations.entrySet()) {
            if (isIdle(entry.getValue())) {
                return entry.getKey();
            }
        }
        return null;
    }

    private boolean isIdle(Set<TableDestination> destinations) {
        for (TableDestination destination : destinations) {
            if (states.get(destination).inFlight.get() > 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Drops an instance and every table held over it.
     *
     * @param instanceKey the instance
     * @return the dropped states, for {@link #release(List)}
     */
    List<DestinationState> removeInstance(String instanceKey) {
        Set<TableDestination> destinations = instanceDestinations.remove(instanceKey);
        if (destinations == null) {
            throw new IllegalStateException(
                    "Bigtable instance bookkeeping lost instance " + instanceKey + ".");
        }
        activeClients = instanceDestinations.size();
        List<DestinationState> removed = new ArrayList<>(destinations.size());
        for (TableDestination destination : destinations) {
            DestinationState state = states.remove(destination);
            if (state == null) {
                throw new IllegalStateException(
                        "Bigtable instance bookkeeping lost table " + destination + ".");
            }
            removed.add(state);
        }
        return removed;
    }

    /**
     * Drops every table idle beyond the timeout with nothing in flight, counting an idle eviction
     * for each instance emptied by it.
     *
     * <p>The in-flight check is what makes this safe on the surface that cannot drain first: a
     * table whose request is still outstanding keeps its lease, whatever its last access says.
     *
     * @param now the current {@code nanoClock} reading
     * @param idleTimeoutNanos the idle timeout
     * @return the dropped states, for {@link #release(List)}
     */
    List<DestinationState> removeIdle(long now, long idleTimeoutNanos) {
        List<DestinationState> evicted = new ArrayList<>();
        Iterator<Map.Entry<TableDestination, DestinationState>> iterator =
                states.entrySet().iterator();
        while (iterator.hasNext()) {
            DestinationState state = iterator.next().getValue();
            if (now - state.lastAccessNanos <= idleTimeoutNanos || state.inFlight.get() > 0) {
                continue;
            }
            iterator.remove();
            evicted.add(state);
            LOG.info(
                    "Evicted Bigtable table {} after {} without requests",
                    state.destination,
                    Duration.ofNanos(now - state.lastAccessNanos));
        }
        for (DestinationState state : evicted) {
            Set<TableDestination> destinations = instanceDestinations.get(state.instanceKey);
            if (destinations == null || !destinations.remove(state.destination)) {
                throw new IllegalStateException(
                        "Bigtable instance bookkeeping lost table " + state.destination + ".");
            }
            if (destinations.isEmpty()) {
                instanceDestinations.remove(state.instanceKey);
                metrics.idleEviction();
            }
        }
        activeClients = instanceDestinations.size();
        return evicted;
    }

    /** Returns the number of tables held. */
    int tableCount() {
        return states.size();
    }

    /** Returns the number of instance clients held, readable from any thread. */
    int activeClients() {
        return activeClients;
    }

    /**
     * Returns every dropped table's lease to the factory. Every release runs before any failure is
     * reported, so one failing cannot strand the rest.
     *
     * @param evicted the states {@link #removeInstance(String)} or {@link #removeIdle(long, long)}
     *     dropped
     * @throws Exception the first release failure, with the others suppressed
     */
    void release(List<DestinationState> evicted) throws Exception {
        List<AutoCloseable> releases = new ArrayList<>(evicted.size());
        for (DestinationState state : evicted) {
            releases.add(() -> factory.release(state.destination));
        }
        Closers.closeAll(releases);
    }

    /** Forgets every table and instance, without releasing anything: for a teardown. */
    void clear() {
        states.clear();
        instanceDestinations.clear();
        activeClients = 0;
    }
}
