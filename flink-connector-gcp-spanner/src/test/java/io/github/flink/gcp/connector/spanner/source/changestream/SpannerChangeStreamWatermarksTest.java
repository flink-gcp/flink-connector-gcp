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

package io.github.flink.gcp.connector.spanner.source.changestream;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class SpannerChangeStreamWatermarksTest {

    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");

    @Test
    void minimumIncludesCreatedScheduledAndRunningButNotFinishedPartitions() {
        SpannerChangeStreamPartitionSplit initial =
                SpannerChangeStreamPartitionSplit.initial(START, null, 2_000)
                        .withLifecycleState(PartitionLifecycleState.FINISHED);
        SpannerChangeStreamPartitionSplit created =
                child("created", initial, START.plusMillis(2), PartitionLifecycleState.CREATED);
        SpannerChangeStreamPartitionSplit scheduled =
                child("scheduled", initial, START.plusMillis(1), PartitionLifecycleState.SCHEDULED);
        SpannerChangeStreamPartitionSplit running =
                child("running", initial, START.plusMillis(3), PartitionLifecycleState.RUNNING);
        SpannerChangeStreamPartitionSplit finished =
                child("finished", initial, START, PartitionLifecycleState.FINISHED);

        assertThat(
                        SpannerChangeStreamWatermarks.sourceWatermark(
                                        Arrays.asList(
                                                initial, created, scheduled, running, finished))
                                .getAsLong())
                .isEqualTo(START.plusMillis(1).toEpochMilli() - 1);
    }

    @Test
    void watermarkPrecedesTheEntireMillisecondContainingTheSpannerFrontier() {
        Instant withinMillisecond = Instant.parse("2026-01-01T00:00:00.123456789Z");

        assertThat(SpannerChangeStreamWatermarks.beforeInstant(withinMillisecond))
                .isEqualTo(Instant.parse("2026-01-01T00:00:00.123Z").toEpochMilli() - 1);
        assertThat(
                        SpannerChangeStreamWatermarks.beforeInstant(
                                Instant.parse("2026-01-01T00:00:00.123Z")))
                .isEqualTo(Instant.parse("2026-01-01T00:00:00.123Z").toEpochMilli() - 1);
    }

    @Test
    void minimumInstantSaturatesInsteadOfOverflowing() {
        assertThat(
                        SpannerChangeStreamWatermarks.beforeInstant(
                                Instant.ofEpochMilli(Long.MIN_VALUE)))
                .isEqualTo(Long.MIN_VALUE);
    }

    @Test
    void finishedLedgerHasNoProgressiveWatermark() {
        SpannerChangeStreamPartitionSplit finished =
                SpannerChangeStreamPartitionSplit.initial(START, START, 2_000)
                        .withLifecycleState(PartitionLifecycleState.FINISHED);

        assertThat(
                        SpannerChangeStreamWatermarks.sourceWatermark(
                                Collections.singletonList(finished)))
                .isEmpty();
    }

    private static SpannerChangeStreamPartitionSplit child(
            String token,
            SpannerChangeStreamPartitionSplit parent,
            Instant watermark,
            PartitionLifecycleState state) {
        return new SpannerChangeStreamPartitionSplit(
                token,
                Collections.singletonList(parent.splitId()),
                START,
                null,
                2_000,
                watermark,
                state,
                watermark);
    }
}
