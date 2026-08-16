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

package io.github.flink.gcp.connector.bigtable.source.changestream.enumerator;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.bigtable.source.changestream.PendingMerge;
import io.github.flink.gcp.connector.bigtable.source.changestream.TestChangeStreamTokens;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRanges;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

class PendingMergeAccumulatorTest {

    private static final int PARENT_COUNT = 1024;
    private static final Instant WATERMARK = Instant.parse("2026-08-12T00:00:00Z");

    @Test
    void accumulatesManyParentsWithBoundedNeighborChecksAndOneFinalMaterialization() {
        PendingMergeAccumulator accumulator =
                new PendingMergeAccumulator(
                        ByteStringRange.create(key(0), key(PARENT_COUNT)), WATERMARK);
        List<ChangeStreamContinuationToken> tokens = new ArrayList<>();
        for (int i = 0; i < PARENT_COUNT; i++) {
            tokens.add(
                    TestChangeStreamTokens.token(
                            ByteStringRange.create(key(i), key(i + 1)), "token-" + i));
        }
        Collections.reverse(tokens);

        for (ChangeStreamContinuationToken token : tokens) {
            accumulator.add(token, WATERMARK);
        }

        assertThat(accumulator.isComplete()).isTrue();
        assertThat(accumulator.getAdjacencyEvaluations()).isLessThanOrEqualTo(3L * PARENT_COUNT);
        assertThat(accumulator.getMaterializations()).isZero();

        PendingMerge checkpoint = accumulator.toPendingMerge();

        assertThat(accumulator.getMaterializations()).isEqualTo(1);
        assertThat(checkpoint.getContinuationTokens()).hasSize(PARENT_COUNT);
        assertThat(checkpoint.getContinuationTokens().get(0).getToken()).isEqualTo("token-0");
        assertThat(checkpoint.getContinuationTokens().get(PARENT_COUNT - 1).getToken())
                .isEqualTo("token-1023");
    }

    @Test
    void arbitraryArrivalOrderStillProducesRangeOrderedCheckpointTokens() {
        ChangeStreamContinuationToken first = token(0, "first");
        ChangeStreamContinuationToken second = token(1, "second");
        ChangeStreamContinuationToken third = token(2, "third");
        PendingMergeAccumulator accumulator =
                new PendingMergeAccumulator(ByteStringRange.create(key(0), key(3)), WATERMARK);

        accumulator.add(third, WATERMARK);
        accumulator.add(first, WATERMARK);
        accumulator.add(second, WATERMARK);

        assertThat(accumulator.toPendingMerge().getContinuationTokens())
                .containsExactly(first, second, third);
    }

    @Test
    void duplicateTokenDoesNotChangeCoverageButStillAdvancesTheLowWatermark() {
        ByteStringRange partition = ByteStringRange.create("a", "z");
        ChangeStreamContinuationToken token = TestChangeStreamTokens.token(partition, "parent");
        PendingMergeAccumulator accumulator = new PendingMergeAccumulator(partition, WATERMARK);

        accumulator.add(token, WATERMARK);
        accumulator.add(token, WATERMARK.minusSeconds(2));
        accumulator.add(token, WATERMARK.minusSeconds(1));

        PendingMerge checkpoint = accumulator.toPendingMerge();
        assertThat(accumulator.isComplete()).isTrue();
        assertThat(checkpoint.getContinuationTokens()).containsExactly(token);
        assertThat(checkpoint.getLowWatermark()).isEqualTo(WATERMARK.minusSeconds(2));
    }

    @Test
    void distinctTokensForTheSameParentRangeAreNotDeduplicated() {
        ByteStringRange partition = ByteStringRange.create("a", "z");
        ChangeStreamContinuationToken first = TestChangeStreamTokens.token(partition, "first");
        ChangeStreamContinuationToken second = TestChangeStreamTokens.token(partition, "second");
        PendingMergeAccumulator accumulator = new PendingMergeAccumulator(partition, WATERMARK);

        accumulator.add(first, WATERMARK);
        accumulator.add(second, WATERMARK);

        assertThat(accumulator.toPendingMerge().getContinuationTokens())
                .containsExactly(first, second);
    }

    @Test
    void restoreCanonicalizesTokenOrderWithoutChangingTheCheckpointModel() {
        ChangeStreamContinuationToken right =
                TestChangeStreamTokens.token(ByteStringRange.create("m", "z"), "right");
        ChangeStreamContinuationToken left =
                TestChangeStreamTokens.token(ByteStringRange.create("a", "m"), "left");
        PendingMerge restored =
                new PendingMerge(ByteStringRange.create("a", "z"), List.of(right, left), WATERMARK);

        PendingMerge checkpoint = PendingMergeAccumulator.restore(restored).toPendingMerge();

        assertThat(checkpoint.getContinuationTokens()).containsExactly(left, right);
        assertThat(checkpoint.getPartition()).isEqualTo(restored.getPartition());
        assertThat(checkpoint.getLowWatermark()).isEqualTo(restored.getLowWatermark());
    }

    @Test
    void partitionKeysDoNotAliasLogRenderings() {
        ByteStringRange commaInStart = ByteStringRange.create("a, b", "c");
        ByteStringRange commaInEnd = ByteStringRange.create("a", "b, c");
        PendingMergeAccumulator first = new PendingMergeAccumulator(commaInStart, WATERMARK);
        PendingMergeAccumulator second = new PendingMergeAccumulator(commaInEnd, WATERMARK);

        assertThat(RowRanges.format(commaInStart)).isEqualTo(RowRanges.format(commaInEnd));
        assertThat(first.partitionKey()).isNotEqualTo(second.partitionKey());
    }

    private static String key(int index) {
        return String.format(Locale.ROOT, "key-%04d", index);
    }

    private static ChangeStreamContinuationToken token(int index, String token) {
        return TestChangeStreamTokens.token(
                ByteStringRange.create(key(index), key(index + 1)), token);
    }
}
