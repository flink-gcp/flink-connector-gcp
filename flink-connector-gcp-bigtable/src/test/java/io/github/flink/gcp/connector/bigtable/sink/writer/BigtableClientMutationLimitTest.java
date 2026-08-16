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

package io.github.flink.gcp.connector.bigtable.sink.writer;

import com.google.api.gax.batching.BatchResource;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.data.v2.stub.mutaterows.MutateRowsBatchingDescriptor;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the client-side facts this connector's batch knobs rest on ({@code docs/adr/0082}): what
 * holds a {@code MutateRows} request to the 100,000 mutations Bigtable documents per batch is the
 * <em>client</em>, in two places, neither of them reachable from a threshold this connector sets.
 *
 * <p>An SDK fact rather than this connector's own behaviour, in the shape {@code docs/adr/0041}
 * uses for the four the writer is built on: a client upgrade that moves either one fails a test
 * rather than a job. Both are asserted through types the client library declares public — {@code
 * MutateRowsBatchingDescriptor} and gax's {@code BatchResource} carry the internal-API annotation
 * this module already accepts for {@code RowMutationEntry.toProto()}, and a release that removes
 * either breaks this file at compile time.
 *
 * <p>What is deliberately <em>not</em> claimed here: a second, independent guard. {@code
 * BulkMutation} does carry a running mutation count with a precondition refusal, but only on its
 * {@code add(ByteString, Mutation)} overload; the batcher's request builder calls {@code
 * add(RowMutationEntry)}, which counts nothing. So the batch-level invariant rests on the flush
 * below alone, and {@code BigtableWriteITCase} is what drives it end to end.
 */
class BigtableClientMutationLimitTest {

    /** What Bigtable documents for a batch of ordinary mutations, and what the client encodes. */
    private static final long DOCUMENTED_MUTATIONS_PER_BATCH = 100_000;

    /**
     * The thresholds a caller configures, set out of the way: this test is about the guard that
     * fires <em>regardless</em> of them, so both are handed their largest possible value.
     */
    private static final long NO_CONFIGURED_THRESHOLD = Long.MAX_VALUE;

    private final MutateRowsBatchingDescriptor descriptor = new MutateRowsBatchingDescriptor();

    @Test
    void theClientsBatchFlushesOnceOneMoreEntryWouldPassTheMutationLimit() {
        BatchResource atTheLimit = accumulate(50_000, 50_000);

        // The control the assertion below needs: at exactly the documented limit, and with no
        // configured threshold that could fire instead, the batch is left to keep accumulating.
        assertThat(atTheLimit.shouldFlush(NO_CONFIGURED_THRESHOLD, NO_CONFIGURED_THRESHOLD))
                .isFalse();

        BatchResource pastTheLimit = accumulate(50_000, 50_001);

        assertThat(pastTheLimit.shouldFlush(NO_CONFIGURED_THRESHOLD, NO_CONFIGURED_THRESHOLD))
                .isTrue();
    }

    @Test
    void oneEntryCannotCarryMoreMutationsThanABatchMayHold() {
        // The other half of the invariant: the flush above bounds what accumulates, and this
        // bounds what a single entry can bring, so no batch reaches the service over the limit.
        assertThatCode(() -> entryOf("row", (int) DOCUMENTED_MUTATIONS_PER_BATCH))
                .doesNotThrowAnyException();

        assertThatThrownBy(() -> entryOf("row", (int) DOCUMENTED_MUTATIONS_PER_BATCH + 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Too many mutations per row");
    }

    /** The resource the batcher would hold after accumulating one entry of each size. */
    private BatchResource accumulate(int firstMutations, int secondMutations) {
        return descriptor
                .createResource(entryOf("row-0", firstMutations))
                .add(descriptor.createResource(entryOf("row-1", secondMutations)));
    }

    private static RowMutationEntry entryOf(String key, int mutations) {
        RowMutationEntry entry = RowMutationEntry.create(key);
        for (int i = 0; i < mutations; i++) {
            entry.setCell("cf", "q" + i, 1_000L, "v");
        }
        return entry;
    }
}
