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

package io.github.flink.gcp.connector.bigtable.source.readrows.enumerator;

import com.google.api.gax.core.CredentialsProvider;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link RowKeySampler} that answers from a script.
 *
 * <p>The call count is the point of it as much as the answer is: a restore that sampled again would
 * renumber every split, and counting calls is the only way to see that it did not.
 *
 * <p>Refusing after {@link #close()} mirrors {@link DataClientRowKeySampler}: without it a sampler
 * shared between two enumerators behaves exactly like a fresh one, which is why nothing caught
 * issue #990. A test that hands one to a source goes through {@link Factory}, which is the
 * serializable half.
 */
public final class ScriptedRowKeySampler implements RowKeySampler {

    /**
     * The script, held as bytes and longs rather than as {@link RowKeySample}s.
     *
     * <p>{@code RowKeySample} is deliberately not serializable — nothing in production carries one
     * across the job graph, since the enumerator both takes the samples and consumes them on the
     * coordinator — and neither is this double. What travels is {@link Factory}, which holds the
     * same two arrays.
     */
    private final byte[][] keys;

    private final long[] offsets;
    @Nullable private final RuntimeException failure;

    private final AtomicInteger calls = new AtomicInteger();
    private final AtomicInteger closes = new AtomicInteger();

    private volatile boolean closed;

    private ScriptedRowKeySampler(
            byte[][] keys, long[] offsets, @Nullable RuntimeException failure) {
        this.keys = keys;
        this.offsets = offsets;
        this.failure = failure;
    }

    /** Returns a sampler that answers with the given samples. */
    public static ScriptedRowKeySampler answering(RowKeySample... samples) {
        byte[][] keys = new byte[samples.length][];
        long[] offsets = new long[samples.length];
        for (int i = 0; i < samples.length; i++) {
            keys[i] = samples[i].getKey().toByteArray();
            offsets[i] = samples[i].getOffsetBytes();
        }
        return new ScriptedRowKeySampler(keys, offsets, null);
    }

    /** Returns a sampler that fails every call. */
    public static ScriptedRowKeySampler failingWith(RuntimeException failure) {
        return new ScriptedRowKeySampler(new byte[0][], new long[0], failure);
    }

    @Override
    public List<RowKeySample> sample(TableDestination table) throws IOException {
        if (closed) {
            throw new IOException(
                    "The Bigtable row key sampler for "
                            + table
                            + " was closed before it was used.");
        }
        calls.incrementAndGet();
        if (failure != null) {
            throw failure;
        }
        List<RowKeySample> samples = new ArrayList<>(keys.length);
        for (int i = 0; i < keys.length; i++) {
            samples.add(RowKeySample.of(ByteString.copyFrom(keys[i]), offsets[i]));
        }
        return samples;
    }

    /** Answers from a script rather than a client, so there is nothing to authenticate. */
    @Override
    public void useCredentials(@Nullable CredentialsProvider credentials) {}

    @Override
    public void close() {
        closed = true;
        closes.incrementAndGet();
    }

    /** Returns whether this sampler refuses further sampling. */
    public boolean isClosed() {
        return closed;
    }

    /** Returns how many times the table was sampled. */
    public int sampleCalls() {
        return calls.get();
    }

    /** Returns how many times this sampler was closed. */
    public int closeCalls() {
        return closes.get();
    }

    /**
     * Mints scripted samplers, and is what a test hands to a source builder.
     *
     * <p>Serializable, as the seam on the configuration now is; the samplers it mints are not, and
     * the list of them is {@code transient} so a copy deserialized inside a MiniCluster job records
     * its own rather than pretending to share the test's.
     */
    public static final class Factory implements RowKeySamplerFactory {

        private static final long serialVersionUID = 1L;

        private final byte[][] keys;
        private final long[] offsets;

        /**
         * The seams minted here.
         *
         * <p>{@code transient} because a copy of this factory deserialized inside a MiniCluster job
         * records its own; concurrent because {@code create()} may run on a coordinator worker
         * thread while a test reads the list on its own.
         */
        @Nullable private transient volatile List<ScriptedRowKeySampler> minted;

        private Factory(byte[][] keys, long[] offsets) {
            this.keys = keys;
            this.offsets = offsets;
        }

        /** Returns a factory minting samplers that answer with the given samples. */
        public static Factory answering(RowKeySample... samples) {
            ScriptedRowKeySampler scripted = ScriptedRowKeySampler.answering(samples);
            return new Factory(scripted.keys, scripted.offsets);
        }

        @Override
        public RowKeySampler create() {
            ScriptedRowKeySampler sampler = new ScriptedRowKeySampler(keys, offsets, null);
            recorded().add(sampler);
            return sampler;
        }

        /** Returns the samplers minted here, in the order they were minted. */
        public List<ScriptedRowKeySampler> minted() {
            return new ArrayList<>(recorded());
        }

        private synchronized List<ScriptedRowKeySampler> recorded() {
            if (minted == null) {
                minted = new CopyOnWriteArrayList<>();
            }
            return minted;
        }

        /**
         * Returns the one sampler minted, failing when there was not exactly one.
         *
         * <p>The count is half of what a caller asserts: one enumerator mints one sampler, and a
         * source that minted two would otherwise pass every count assertion.
         */
        public ScriptedRowKeySampler only() {
            if (recorded().size() != 1) {
                throw new AssertionError(
                        "expected exactly one minted sampler but was " + recorded().size());
            }
            return recorded().get(0);
        }
    }
}
