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

package io.github.flink.gcp.connector.bigtable.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/**
 * Tuning options for the sink's writer: the batch thresholds handed to the client, and the writer's
 * own bounds on unacknowledged mutations.
 *
 * <p>Set via {@link BigtableSinkBuilder#writerOptions(BigtableWriterOptions)}; optional — every
 * knob is defaulted, so {@link #defaults()} is equivalent to not setting options at all. An unset
 * batch threshold leaves the client's own default in place rather than restating it here, so a
 * client upgrade that retunes it is inherited.
 *
 * <p>There are deliberately <em>no</em> retry knobs. Unlike Cloud Tasks, the Bigtable client
 * retries {@code MutateRows} itself — per entry, for the transient codes, on a schedule of its own
 * — so the sink owns no retry loop and has nothing to expose.
 *
 * <h2>Why the in-flight bounds are the writer's own</h2>
 *
 * <p>The client also has a flow controller of its own, and it is the wrong instrument here: it
 * <em>blocks</em> the calling thread when its limits are reached, and the calling thread is Flink's
 * task thread, which must stay free to run mailbox mails. So the writer keeps its own bounds and
 * yields to the mailbox instead. That only works while the writer's bounds are reached first, and
 * the client's limits — 1000 entries per channel and 100 MB, blocking — cannot be raised through
 * its public API. Raising {@link Builder#maxInFlightMutations(int)} far above the default therefore
 * moves the effective bound into the client, where it stalls the task thread rather than
 * backpressuring the stream.
 *
 * <p>Instances are immutable and serializable.
 */
@PublicEvolving
public final class BigtableWriterOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final BigtableWriterOptions DEFAULTS = builder().build();

    @Nullable private final Long batchElementCount;
    @Nullable private final Long batchByteSize;
    private final int maxInFlightMutations;
    private final long maxInFlightBytes;

    private BigtableWriterOptions(Builder builder) {
        this.batchElementCount = builder.batchElementCount;
        this.batchByteSize = builder.batchByteSize;
        this.maxInFlightMutations = builder.maxInFlightMutations;
        this.maxInFlightBytes = builder.maxInFlightBytes;
    }

    /**
     * Creates a new {@link Builder}.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the default options: the client's own batch thresholds, at most 1000 unacknowledged
     * mutations and at most 64 MiB of them.
     *
     * @return the default options
     */
    public static BigtableWriterOptions defaults() {
        return DEFAULTS;
    }

    /** Returns the batch element-count threshold, or {@code null} to use the client's default. */
    @Nullable
    public Long getBatchElementCount() {
        return batchElementCount;
    }

    /** Returns the batch byte threshold, or {@code null} to use the client's default. */
    @Nullable
    public Long getBatchByteSize() {
        return batchByteSize;
    }

    /** Returns the writer's cap on unacknowledged mutations. */
    public int getMaxInFlightMutations() {
        return maxInFlightMutations;
    }

    /** Returns the writer's cap on the serialized size of unacknowledged mutations. */
    public long getMaxInFlightBytes() {
        return maxInFlightBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BigtableWriterOptions that = (BigtableWriterOptions) o;
        return maxInFlightMutations == that.maxInFlightMutations
                && maxInFlightBytes == that.maxInFlightBytes
                && Objects.equals(batchElementCount, that.batchElementCount)
                && Objects.equals(batchByteSize, that.batchByteSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                batchElementCount, batchByteSize, maxInFlightMutations, maxInFlightBytes);
    }

    @Override
    public String toString() {
        return "BigtableWriterOptions{batchElementCount="
                + batchElementCount
                + ", batchByteSize="
                + batchByteSize
                + ", maxInFlightMutations="
                + maxInFlightMutations
                + ", maxInFlightBytes="
                + maxInFlightBytes
                + "}";
    }

    /** Builder for {@link BigtableWriterOptions}. */
    @PublicEvolving
    public static final class Builder {

        @Nullable private Long batchElementCount;
        @Nullable private Long batchByteSize;
        private int maxInFlightMutations = 1000;
        private long maxInFlightBytes = 64L * 1024 * 1024;

        private Builder() {}

        /**
         * Sets how many mutations the client accumulates before sending a batch. Defaults to the
         * client's own threshold (100).
         *
         * @param batchElementCount the element-count threshold, positive
         * @return this builder
         */
        public Builder batchElementCount(long batchElementCount) {
            Preconditions.checkArgument(
                    batchElementCount > 0, "batchElementCount must be positive");
            this.batchElementCount = batchElementCount;
            return this;
        }

        /**
         * Sets how many bytes of mutations the client accumulates before sending a batch. Defaults
         * to the client's own threshold (20 MB). Element count alone bounds no memory: Bigtable
         * accepts up to 100 MB of mutations per request.
         *
         * @param batchByteSize the byte threshold, positive
         * @return this builder
         */
        public Builder batchByteSize(long batchByteSize) {
            Preconditions.checkArgument(batchByteSize > 0, "batchByteSize must be positive");
            this.batchByteSize = batchByteSize;
            return this;
        }

        /**
         * Caps the mutations the writer keeps unacknowledged. A write at the cap yields to the task
         * mailbox until completions bring the count down, bounding sink memory between checkpoints.
         * Defaults to 1000.
         *
         * <p>Raising this far above the default moves the effective bound into the client's own
         * flow controller, which blocks the task thread instead of yielding — see the class
         * documentation.
         *
         * @param maxInFlightMutations the in-flight cap, positive
         * @return this builder
         */
        public Builder maxInFlightMutations(int maxInFlightMutations) {
            Preconditions.checkArgument(
                    maxInFlightMutations > 0, "maxInFlightMutations must be positive");
            this.maxInFlightMutations = maxInFlightMutations;
            return this;
        }

        /**
         * Caps the serialized size of the mutations the writer keeps unacknowledged. Defaults to 64
         * MiB. This is the bound that actually bounds memory — a single row mutation may be
         * megabytes, so a count alone does not.
         *
         * @param maxInFlightBytes the in-flight byte cap, positive
         * @return this builder
         */
        public Builder maxInFlightBytes(long maxInFlightBytes) {
            Preconditions.checkArgument(maxInFlightBytes > 0, "maxInFlightBytes must be positive");
            this.maxInFlightBytes = maxInFlightBytes;
            return this;
        }

        /**
         * Builds the options.
         *
         * @return the options
         */
        public BigtableWriterOptions build() {
            return new BigtableWriterOptions(this);
        }
    }
}
