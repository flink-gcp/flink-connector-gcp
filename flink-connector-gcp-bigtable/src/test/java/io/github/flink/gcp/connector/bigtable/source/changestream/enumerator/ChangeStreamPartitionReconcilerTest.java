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
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.bigtable.source.changestream.MissingPartition;
import io.github.flink.gcp.connector.bigtable.source.changestream.PendingMerge;
import io.github.flink.gcp.connector.bigtable.source.changestream.TestChangeStreamTokens;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRanges;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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

    /**
     * A bounded run finishes a range once and the service reports it forever, so a completed range
     * has to count as covered or the run's own success reads as a gap (#951). The empty ledger here
     * is what a drained bounded run holds.
     */
    @Test
    void aCompletedRangeCoversItsServicePartition() {
        ChangeStreamPartitionReconciler.Result result =
                reconciler.reconcile(
                        Arrays.asList(LEFT, RIGHT),
                        Collections.emptyList(),
                        Arrays.asList(LEFT, RIGHT),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        NOW,
                        NOW.minusSeconds(60));

        assertThat(result.missing).isEmpty();
        assertThat(result.recoveries).isEmpty();
    }

    /**
     * Completion is not all-or-nothing while a bounded run drains, and the two collections tile
     * together: a range already read to the end time is covered by the completed list, one still
     * being read by the ledger, and only what neither holds may be reported.
     */
    @Test
    void completedRangesTileWithTheLiveLedgerAndLeaveOnlyTheRemainder() {
        ByteStringRange service = ByteStringRange.create("a", "z");
        ChangeStreamPartitionReconciler.Result covered =
                reconciler.reconcile(
                        Collections.singletonList(service),
                        Collections.singletonList(split("live", ByteStringRange.create("a", "m"))),
                        Collections.singletonList(ByteStringRange.create("m", "z")),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        NOW,
                        NOW.minusSeconds(60));

        assertThat(covered.missing).isEmpty();
        assertThat(covered.recoveries).isEmpty();

        ChangeStreamPartitionReconciler.Result shortOfTheEnd =
                reconciler.reconcile(
                        Collections.singletonList(service),
                        Collections.singletonList(split("live", ByteStringRange.create("a", "m"))),
                        Collections.singletonList(ByteStringRange.create("m", "t")),
                        Collections.emptyList(),
                        Collections.singletonList(
                                new MissingPartition(
                                        service,
                                        NOW.minus(ChangeStreamPartitionReconciler.TOKENLESS_GRACE),
                                        NOW.minusSeconds(30))),
                        NOW,
                        NOW);

        assertThat(shortOfTheEnd.recoveries)
                .singleElement()
                .satisfies(
                        recovery ->
                                assertThat(RowRanges.format(recovery.partition))
                                        .isEqualTo(
                                                RowRanges.format(
                                                        ByteStringRange.create("t", "z"))));
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

    @Test
    void keepsTwoPartitionsApartWhenOneEndsAtTheRowKeyTheRendererUsesAsItsSentinel() {
        // A partition ending at the row key "*" and one running to the end of the table are
        // different partitions, and matching a scanned partition to a remembered one must keep them
        // apart — otherwise the second inherits the first's grace timer and low watermark. This
        // used to open by asserting the two rendered alike, which is how a rendering-based match
        // confused them; the renderer escapes 0x2A now, so the two are told apart twice over. The
        // match is on the range regardless, which is what this still guards.
        ByteStringRange endsAtStar = ByteStringRange.unbounded().startClosed("a").endOpen("*");
        ByteStringRange runsToTheEnd = ByteStringRange.unbounded().startClosed("a");
        assertThat(endsAtStar).isNotEqualTo(runsToTheEnd);

        Instant longAgo = NOW.minus(ChangeStreamPartitionReconciler.TOKENLESS_GRACE);
        ChangeStreamPartitionReconciler.Result result =
                reconciler.reconcile(
                        Collections.singletonList(runsToTheEnd),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonList(
                                new MissingPartition(endsAtStar, longAgo, NOW.minusSeconds(30))),
                        NOW,
                        NOW);

        // Not recognised, so it is observed for the first time now and no grace has elapsed.
        assertThat(result.recoveries).isEmpty();
        assertThat(result.missing)
                .singleElement()
                .satisfies(
                        missing -> {
                            assertThat(missing.getPartition()).isEqualTo(runsToTheEnd);
                            assertThat(missing.getFirstObserved()).isEqualTo(NOW);
                        });
    }

    @Test
    void requiresNormalisedPartitionsToRecogniseOneAcrossScans() {
        // GenerateInitialChangeStreamPartitionsUserCallable builds every partition with
        // ByteStringRange.create(start_key_closed, end_key_open), so the first partition of a table
        // arrives as a CLOSED bound at the empty key rather than as UNBOUNDED. This reconciler
        // reads bound types, so it requires that spelling to be folded first — which is
        // DefaultChangeStreamCoordinatorClient's job, pinned by its own test. Both halves are
        // asserted here because the failure is silent: the partition is simply never recovered.
        ByteStringRange asTheServiceSendsIt = ByteStringRange.create(ByteString.EMPTY, key("m"));

        assertThat(recoveriesAfterTwoScansSeparatedByTheTokenlessGrace(asTheServiceSendsIt))
                .as("raw service spelling: firstObserved resets, so no grace can elapse")
                .isEmpty();
        assertThat(
                        recoveriesAfterTwoScansSeparatedByTheTokenlessGrace(
                                RowRanges.copyOf(asTheServiceSendsIt)))
                .as("normalised: recognised across scans, so the grace elapses")
                .singleElement()
                .satisfies(recovery -> assertThat(recovery.tokenless).isTrue());
    }

    @Test
    void requiresNormalisedPartitionsToSeeTheLastPartitionOfATableAtAll() {
        // The mirror case: the last partition arrives with an empty end_key_open. Read literally
        // that is a range ending at the empty key — the smallest key there is — so it contains
        // nothing and the reconciler reports no gap however long the ledger has been missing it.
        ByteStringRange asTheServiceSendsIt = ByteStringRange.create(key("m"), ByteString.EMPTY);

        assertThat(missingAfterOneScanOfAnEmptyLedger(asTheServiceSendsIt))
                .as("raw service spelling: the partition looks empty, so nothing is missing")
                .isEmpty();
        assertThat(missingAfterOneScanOfAnEmptyLedger(RowRanges.copyOf(asTheServiceSendsIt)))
                .as("normalised: the whole partition is uncovered")
                .hasSize(1);
    }

    private List<ChangeStreamPartitionReconciler.Recovery>
            recoveriesAfterTwoScansSeparatedByTheTokenlessGrace(ByteStringRange partition) {
        ChangeStreamPartitionReconciler scanner = new ChangeStreamPartitionReconciler();
        List<ByteStringRange> service = Collections.singletonList(partition);
        ChangeStreamPartitionReconciler.Result first =
                scanner.reconcile(
                        service,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        NOW,
                        NOW.minusSeconds(60));
        Instant later = NOW.plus(ChangeStreamPartitionReconciler.TOKENLESS_GRACE).plusSeconds(60);
        return scanner.reconcile(
                        service,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        first.missing,
                        later,
                        later.minusSeconds(60))
                .recoveries;
    }

    private List<MissingPartition> missingAfterOneScanOfAnEmptyLedger(ByteStringRange partition) {
        return new ChangeStreamPartitionReconciler()
                .reconcile(
                        Collections.singletonList(partition),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        NOW,
                        NOW.minusSeconds(60))
                .missing;
    }

    private static ByteString key(String text) {
        return ByteString.copyFromUtf8(text);
    }

    private static ChangeStreamPartitionSplit split(String id, ByteStringRange partition) {
        return new ChangeStreamPartitionSplit(id, partition, Collections.emptyList(), NOW);
    }
}
