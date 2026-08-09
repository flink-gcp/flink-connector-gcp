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

package io.github.flink.gcp.connector.bigtable.source.readrows.enumerator;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link RowKeySampler} that answers from a script.
 *
 * <p>The call count is the point of it as much as the answer is: a restore that sampled again would
 * renumber every split, and counting calls is the only way to see that it did not.
 */
public final class ScriptedRowKeySampler implements RowKeySampler {

    private static final long serialVersionUID = 1L;

    /**
     * The script, held as bytes and longs rather than as {@link RowKeySample}s.
     *
     * <p>{@code RowKeySample} is deliberately not serializable — nothing in production carries one
     * across the job graph, since the enumerator both takes the samples and consumes them on the
     * coordinator — while this double <em>is</em> serialized, because the configuration it sits in
     * travels to a MiniCluster job.
     */
    private final byte[][] keys;

    private final long[] offsets;
    @Nullable private final RuntimeException failure;

    private final AtomicInteger calls = new AtomicInteger();
    private final AtomicInteger closes = new AtomicInteger();

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

    @Override
    public void close() {
        closes.incrementAndGet();
    }

    /** Returns how many times the table was sampled. */
    public int sampleCalls() {
        return calls.get();
    }

    /** Returns how many times this sampler was closed. */
    public int closeCalls() {
        return closes.get();
    }
}
