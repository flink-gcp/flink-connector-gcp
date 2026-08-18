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

package io.github.flink.gcp.connector.bigquery.sink.serializer;

import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link LazyDerivedState}. */
class LazyDerivedStateTest {

    @Test
    void derivesOnceHoweverOftenTheStateIsRead() {
        Owner owner = new Owner();

        assertThat(owner.derivations).as("derivations before the first read").hasValue(0);

        String first = owner.state();

        assertThat(owner.state()).isSameAs(first);
        assertThat(owner.state()).isSameAs(first);
        assertThat(owner.derivations).as("derivations").hasValue(1);
    }

    @Test
    void neitherTheStateNorTheDerivationTravelsInTheJobGraph() throws Exception {
        Owner owner = new Owner();
        String derived = owner.state();

        String serialized =
                new String(InstantiationUtil.serializeObject(owner), StandardCharsets.ISO_8859_1);

        // The state is transient, so what was derived here stays here...
        assertThat(derived).isEqualTo("derived 1");
        assertThat(serialized).doesNotContain("derived 1");
        // ...and the derivation is an argument, so there is no field a serializable functional
        // interface could reach the job graph through, where a lambda would be bound back by a
        // synthetic-method name no connector version pins (ADR-0125).
        assertThat(serialized).doesNotContain("SerializedLambda");

        // Absence alone would also pass on a holder that had lost its owner, so read it back.
        Owner restored = InstantiationUtil.clone(owner);

        assertThat(restored.state()).isEqualTo("derived 2");
    }

    @Test
    void rejectsADerivationThatYieldsNothingAndStaysEmpty() {
        LazyDerivedState<String> holder = new LazyDerivedState<>();

        assertThatThrownBy(() -> holder.get("owner", owner -> null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("The derived state must not be null");

        // A failed derivation is not remembered: a serializer whose first attempt failed on a task
        // manager is not poisoned for the life of the job.
        assertThat(holder.get("owner", String::toUpperCase)).isEqualTo("OWNER");
    }

    @Test
    void concurrentFirstReadsDeriveOnceAndNeverOverlap() throws Exception {
        int readers = 8;
        WatchedOwner owner = new WatchedOwner();
        CyclicBarrier start = new CyclicBarrier(readers);
        ExecutorService pool = Executors.newFixedThreadPool(readers);
        try {
            List<Future<String>> reads = new ArrayList<>(readers);
            for (int i = 0; i < readers; i++) {
                reads.add(
                        pool.submit(
                                () -> {
                                    start.await(30, TimeUnit.SECONDS);
                                    return owner.state();
                                }));
            }

            String first = reads.get(0).get(30, TimeUnit.SECONDS);
            for (Future<String> read : reads) {
                assertThat(read.get(30, TimeUnit.SECONDS))
                        .as("every reader sees the one derived state")
                        .isSameAs(first);
            }
            assertThat(owner.derivations).as("derivations").hasValue(1);
            assertThat(owner.widestOverlap).as("readers inside the derivation at once").hasValue(1);
        } finally {
            pool.shutdownNow();
        }
    }

    /** An owner in the shape the serializers use: a holder read through an unbound reference. */
    private static final class Owner implements Serializable {

        private static final long serialVersionUID = 1L;

        private final AtomicInteger derivations = new AtomicInteger();
        private final LazyDerivedState<String> derivedState = new LazyDerivedState<>();

        String state() {
            return derivedState.get(this, Owner::derive);
        }

        private String derive() {
            return "derived " + derivations.incrementAndGet();
        }
    }

    /**
     * An owner whose derivation notices any reader deriving beside it, so that the exclusion is
     * asserted rather than inferred from the count.
     */
    private static final class WatchedOwner {

        private static final int SPINS = 1_000_000;

        private final AtomicInteger derivations = new AtomicInteger();
        private final AtomicInteger inside = new AtomicInteger();
        private final AtomicInteger widestOverlap = new AtomicInteger();
        private final LazyDerivedState<String> derivedState = new LazyDerivedState<>();

        String state() {
            return derivedState.get(this, WatchedOwner::derive);
        }

        private String derive() {
            widestOverlap.accumulateAndGet(inside.incrementAndGet(), Math::max);
            // A bounded spin, not a sleep: it holds the derivation open long enough for a second
            // reader to be seen in it, and gives up rather than waiting for one. Held under the
            // monitor no second reader can arrive, so the budget is paid once, in milliseconds.
            for (int spin = 0; spin < SPINS && inside.get() == 1; spin++) {
                Thread.onSpinWait();
            }
            inside.decrementAndGet();
            return "derived " + derivations.incrementAndGet();
        }
    }
}
