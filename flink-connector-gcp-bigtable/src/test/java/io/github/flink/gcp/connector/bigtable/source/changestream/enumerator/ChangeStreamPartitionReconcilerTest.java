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

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.bigtable.source.changestream.MissingPartition;
import io.github.flink.gcp.connector.bigtable.source.changestream.PendingMerge;
import io.github.flink.gcp.connector.bigtable.source.changestream.TestChangeStreamTokens;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRanges;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class ChangeStreamPartitionReconcilerTest {

    private static final ByteStringRange WHOLE = ByteStringRange.unbounded();
    private static final ByteStringRange LEFT = ByteStringRange.unbounded().endOpen("m");
    private static final ByteStringRange RIGHT = ByteStringRange.unbounded().startClosed("m");
    private static final ByteStringRange GAP = ByteStringRange.create("a", "z");
    private static final Instant NOW = Instant.parse("2026-08-11T00:30:00Z");
    private final ChangeStreamPartitionReconciler reconciler =
            new ChangeStreamPartitionReconciler();

    @Test
    void tiledLedgerCoversMovingServiceBoundaries() {
        ChangeStreamPartitionReconciler.Result result =
                reconciler.reconcile(
                        Collections.singletonList(WHOLE),
                        Arrays.asList(split("left", LEFT), split("right", RIGHT)),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        NOW,
                        NOW.minusSeconds(60));

        assertThat(result.missing).isEmpty();
        assertThat(result.recoveries).isEmpty();
    }

    @Test
    void reconstructsPersistentGapFromCompatibleParentTokens() {
        Instant observed = NOW.minus(ChangeStreamPartitionReconciler.TOKEN_GRACE);
        PendingMerge merge =
                new PendingMerge(
                        WHOLE,
                        Arrays.asList(
                                TestChangeStreamTokens.token(LEFT, "left"),
                                TestChangeStreamTokens.token(RIGHT, "right")),
                        NOW.minusSeconds(20));
        java.util.List<com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken>
                compatible =
                        ChangeStreamPartitionReconciler.compatibleTokens(
                                WHOLE, Collections.singletonList(merge));
        assertThat(compatible).hasSize(2);
        assertThat(ChangeStreamPartitionReconciler.tokensCover(WHOLE, compatible)).isTrue();

        ChangeStreamPartitionReconciler.Result result =
                reconciler.reconcile(
                        Collections.singletonList(WHOLE),
                        Collections.emptyList(),
                        Collections.singletonList(merge),
                        Collections.singletonList(
                                new MissingPartition(WHOLE, observed, NOW.minusSeconds(30))),
                        NOW,
                        NOW.minusSeconds(30));

        assertThat(result.missing).isEmpty();
        assertThat(result.recoveries)
                .singleElement()
                .satisfies(
                        recovery -> {
                            assertThat(recovery.tokens).hasSize(2);
                            assertThat(recovery.tokenless).isFalse();
                            assertThat(recovery.lowWatermark).isEqualTo(NOW.minusSeconds(30));
                        });
    }

    @Test
    void restartsPersistentTokenlessGapAtTrackedLowWatermark() {
        Instant lowWatermark = NOW.minusSeconds(90);
        MissingPartition missing =
                new MissingPartition(
                        WHOLE,
                        NOW.minus(ChangeStreamPartitionReconciler.TOKENLESS_GRACE),
                        lowWatermark);

        ChangeStreamPartitionReconciler.Result result =
                reconciler.reconcile(
                        Collections.singletonList(WHOLE),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonList(missing),
                        NOW,
                        NOW);

        assertThat(result.recoveries)
                .singleElement()
                .satisfies(
                        recovery -> {
                            assertThat(recovery.tokens).isEmpty();
                            assertThat(recovery.tokenless).isTrue();
                            assertThat(recovery.lowWatermark).isEqualTo(lowWatermark);
                        });
    }

    @Test
    void preservesTimersImmediatelyBeforeEachRecoveryThreshold() {
        PendingMerge tokens =
                new PendingMerge(
                        WHOLE,
                        Arrays.asList(
                                TestChangeStreamTokens.token(LEFT, "left"),
                                TestChangeStreamTokens.token(RIGHT, "right")),
                        NOW.minusSeconds(30));
        MissingPartition beforeTokenGrace =
                new MissingPartition(
                        WHOLE,
                        NOW.minus(ChangeStreamPartitionReconciler.TOKEN_GRACE).plusNanos(1),
                        NOW.minusSeconds(60));
        ChangeStreamPartitionReconciler.Result tokenResult =
                reconciler.reconcile(
                        Collections.singletonList(WHOLE),
                        Collections.emptyList(),
                        Collections.singletonList(tokens),
                        Collections.singletonList(beforeTokenGrace),
                        NOW,
                        NOW);
        assertThat(tokenResult.recoveries).isEmpty();
        assertThat(tokenResult.missing).containsExactly(beforeTokenGrace);

        MissingPartition beforeTokenlessGrace =
                new MissingPartition(
                        WHOLE,
                        NOW.minus(ChangeStreamPartitionReconciler.TOKENLESS_GRACE).plusNanos(1),
                        NOW.minusSeconds(60));
        ChangeStreamPartitionReconciler.Result tokenlessResult =
                reconciler.reconcile(
                        Collections.singletonList(WHOLE),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonList(beforeTokenlessGrace),
                        NOW,
                        NOW);
        assertThat(tokenlessResult.recoveries).isEmpty();
        assertThat(tokenlessResult.missing).containsExactly(beforeTokenlessGrace);
    }

    @Test
    void recoversOnlyTheUncoveredRemainderOfAPartialLiveOverlap() {
        ByteStringRange service = ByteStringRange.create("a", "n");
        ByteStringRange live = ByteStringRange.create("a", "m");
        MissingPartition old =
                new MissingPartition(
                        service,
                        NOW.minus(ChangeStreamPartitionReconciler.TOKENLESS_GRACE),
                        NOW.minusSeconds(30));

        ChangeStreamPartitionReconciler.Result result =
                reconciler.reconcile(
                        Collections.singletonList(service),
                        Collections.singletonList(split("live", live)),
                        Collections.emptyList(),
                        Collections.singletonList(old),
                        NOW,
                        NOW);

        assertThat(result.missing).isEmpty();
        assertThat(result.recoveries)
                .singleElement()
                .satisfies(
                        recovery -> {
                            assertThat(RowRanges.format(recovery.partition))
                                    .isEqualTo(RowRanges.format(ByteStringRange.create("m", "n")));
                            assertThat(recovery.tokenless).isTrue();
                        });
    }

    @Test
    void recoversAGapBetweenTwoCoveredStretchesOfOnePartition() {
        ByteStringRange service = ByteStringRange.create("a", "z");
        MissingPartition old =
                new MissingPartition(
                        service,
                        NOW.minus(ChangeStreamPartitionReconciler.TOKENLESS_GRACE),
                        NOW.minusSeconds(30));

        ChangeStreamPartitionReconciler.Result result =
                reconciler.reconcile(
                        Collections.singletonList(service),
                        Arrays.asList(
                                split("head", ByteStringRange.create("a", "m")),
                                split("tail", ByteStringRange.create("t", "z"))),
                        Collections.emptyList(),
                        Collections.singletonList(old),
                        NOW,
                        NOW);

        assertThat(result.recoveries)
                .singleElement()
                .satisfies(
                        recovery ->
                                assertThat(RowRanges.format(recovery.partition))
                                        .isEqualTo(
                                                RowRanges.format(
                                                        ByteStringRange.create("m", "t"))));
    }

    @Test
    void coversAPartitionWhoseLedgerRangesArriveOutOfOrder() {
        ByteStringRange service = ByteStringRange.create("a", "z");

        ChangeStreamPartitionReconciler.Result result =
                reconciler.reconcile(
                        Collections.singletonList(service),
                        Arrays.asList(
                                split("tail", ByteStringRange.create("m", "z")),
                                split("head", ByteStringRange.create("a", "m"))),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        NOW,
                        NOW.minusSeconds(60));

        assertThat(result.missing).isEmpty();
        assertThat(result.recoveries).isEmpty();
    }

    @Test
    void aFirstObservedGapStartsItsTimerAtTheFallbackLowWatermark() {
        Instant fallback = NOW.minusSeconds(120);

        ChangeStreamPartitionReconciler.Result result =
                reconciler.reconcile(
                        Collections.singletonList(WHOLE),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        NOW,
                        fallback);

        assertThat(result.recoveries).isEmpty();
        assertThat(result.missing)
                .singleElement()
                .satisfies(
                        missing -> {
                            assertThat(missing.getFirstObserved()).isEqualTo(NOW);
                            assertThat(missing.getLowWatermark()).isEqualTo(fallback);
                        });
    }

    @Test
    void tokensStartingInsideTheGapDoNotRecoverIt() {
        assertThat(
                        ChangeStreamPartitionReconciler.tokensCover(
                                GAP,
                                Collections.singletonList(
                                        TestChangeStreamTokens.token(
                                                ByteStringRange.create("b", "z"), "late"))))
                .isFalse();
    }

    @Test
    void tokensLeavingAHoleInTheMiddleDoNotRecoverTheGap() {
        assertThat(
                        ChangeStreamPartitionReconciler.tokensCover(
                                GAP,
                                Arrays.asList(
                                        TestChangeStreamTokens.token(
                                                ByteStringRange.create("a", "m"), "head"),
                                        TestChangeStreamTokens.token(
                                                ByteStringRange.create("t", "z"), "tail"))))
                .isFalse();
    }

    @Test
    void tokensEndingInsideTheGapDoNotRecoverIt() {
        assertThat(
                        ChangeStreamPartitionReconciler.tokensCover(
                                GAP,
                                Collections.singletonList(
                                        TestChangeStreamTokens.token(
                                                ByteStringRange.create("a", "t"), "short"))))
                .isFalse();
    }

    @Test
    void aTokenStartingBeforeTheGapIsNotAdopted() {
        PendingMerge merge =
                new PendingMerge(
                        WHOLE,
                        Collections.singletonList(
                                TestChangeStreamTokens.token(
                                        ByteStringRange.create("a", "m"), "wide")),
                        NOW.minusSeconds(20));

        assertThat(
                        ChangeStreamPartitionReconciler.compatibleTokens(
                                ByteStringRange.create("b", "m"), Collections.singletonList(merge)))
                .isEmpty();
    }

    @Test
    void aTokenReachingPastTheGapIsNotAdopted() {
        PendingMerge merge =
                new PendingMerge(
                        WHOLE,
                        Collections.singletonList(
                                TestChangeStreamTokens.token(
                                        ByteStringRange.create("c", "z"), "wide")),
                        NOW.minusSeconds(20));

        assertThat(
                        ChangeStreamPartitionReconciler.compatibleTokens(
                                ByteStringRange.create("b", "m"), Collections.singletonList(merge)))
                .isEmpty();
    }

    @Test
    void aTokenReportedByTwoMergesIsAdoptedOnce() {
        PendingMerge first =
                new PendingMerge(
                        WHOLE,
                        Collections.singletonList(TestChangeStreamTokens.token(LEFT, "left")),
                        NOW.minusSeconds(20));
        PendingMerge second =
                new PendingMerge(
                        WHOLE,
                        Arrays.asList(
                                TestChangeStreamTokens.token(LEFT, "left"),
                                TestChangeStreamTokens.token(RIGHT, "right")),
                        NOW.minusSeconds(20));

        assertThat(
                        ChangeStreamPartitionReconciler.compatibleTokens(
                                WHOLE, Arrays.asList(first, second)))
                .containsExactly(
                        TestChangeStreamTokens.token(LEFT, "left"),
                        TestChangeStreamTokens.token(RIGHT, "right"));
    }

    @Test
    void adoptsTheEarliestLowWatermarkOfEveryContributingMerge() {
        Instant earliest = NOW.minusSeconds(300);
        PendingMerge left =
                new PendingMerge(
                        WHOLE,
                        Collections.singletonList(TestChangeStreamTokens.token(LEFT, "left")),
                        earliest);
        // Listed second and later, so a version keeping the last contributor rather than the
        // earliest resumes 290 s of this keyspace past changes the left parent had not seen.
        PendingMerge right =
                new PendingMerge(
                        WHOLE,
                        Collections.singletonList(TestChangeStreamTokens.token(RIGHT, "right")),
                        NOW.minusSeconds(10));

        ChangeStreamPartitionReconciler.Result result =
                reconciler.reconcile(
                        Collections.singletonList(WHOLE),
                        Collections.emptyList(),
                        Arrays.asList(left, right),
                        Collections.singletonList(
                                new MissingPartition(
                                        WHOLE,
                                        NOW.minus(ChangeStreamPartitionReconciler.TOKEN_GRACE),
                                        NOW.minusSeconds(30))),
                        NOW,
                        NOW);

        assertThat(result.recoveries)
                .singleElement()
                .satisfies(recovery -> assertThat(recovery.lowWatermark).isEqualTo(earliest));
    }

    private static ChangeStreamPartitionSplit split(String id, ByteStringRange partition) {
        return new ChangeStreamPartitionSplit(id, partition, Collections.emptyList(), NOW);
    }
}
