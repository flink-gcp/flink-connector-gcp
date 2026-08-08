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

package io.github.flink.gcp.connector.pubsub;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.atomic.LongAdder;

/**
 * A read-only {@link Counter} view of one {@link PubSubShutdownResidue} adder, so a residue
 * registers as the counter it is rather than as a gauge over a monotonic total.
 *
 * <p>Registering a caller-supplied {@link Counter} is what lets the instrument be right while the
 * storage stays process-wide: the count has to outlive the task (see {@link
 * PubSubShutdownResidue}), and a cumulative count of events is a counter by the naming convention.
 *
 * <p>The mutators throw. Nothing calls them: a metric group only registers the instance and
 * reporters only read {@link #getCount()} — incrementing is done by the teardowns, through the
 * adder {@link PubSubShutdownResidue} hands them. A silent no-op would hide a caller that believed
 * it was counting something.
 */
@Internal
public final class AbandonedShutdownsCounter implements Counter {

    private final LongAdder residue;

    /**
     * Creates the view.
     *
     * @param residue the adder the teardowns count into
     */
    public AbandonedShutdownsCounter(LongAdder residue) {
        this.residue = Preconditions.checkNotNull(residue, "residue must not be null");
    }

    @Override
    public void inc() {
        throw new UnsupportedOperationException(
                "An abandoned-shutdown residue is maintained by the teardowns, not here.");
    }

    @Override
    public void inc(long n) {
        inc();
    }

    @Override
    public void dec() {
        inc();
    }

    @Override
    public void dec(long n) {
        inc();
    }

    @Override
    public long getCount() {
        return residue.sum();
    }
}
