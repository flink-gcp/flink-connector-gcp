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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import io.github.flink.gcp.connector.pubsub.PubSubMetricNames;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RestoredLedger}. */
// A broken table that never grows loops forever in its probe, ignoring interrupts; a separate
// thread lets the timeout still report the failure.
@Timeout(value = 60, threadMode = ThreadMode.SEPARATE_THREAD)
class RestoredLedgerTest {

    private static final SubscriptionDestination SUB_A =
            SubscriptionDestination.of("project", "subscription-a");
    private static final SubscriptionDestination SUB_B =
            SubscriptionDestination.of("project", "subscription-b");
    private static final Duration TTL = Duration.ofMinutes(10);

    private final AtomicLong clock = new AtomicLong();
    private final TestReaderMetrics testMetrics = new TestReaderMetrics();
    private final RestoredLedger ledger =
            bound(new RestoredLedger(TTL, clock::get, testMetrics.metrics()));

    private RestoredLedger bound(RestoredLedger restoredLedger) {
        testMetrics.metrics().bindRestoredLedger(restoredLedger);
        return restoredLedger;
    }

    @Test
    void suppressesEveryCopyOfARestoredMessage() {
        ledger.add(SUB_A, List.of("m1"));
        ledger.startClock();

        assertThat(ledger.suppresses(SUB_A, "m1")).isTrue();
        // A second copy, for example after a second lease expiry, is covered too.
        assertThat(ledger.suppresses(SUB_A, "m1")).isTrue();
        assertThat(ledger.suppresses(SUB_A, "m2")).isFalse();
        assertThat(testMetrics.counter(PubSubMetricNames.REDELIVERIES_SUPPRESSED)).isEqualTo(2);
    }

    @Test
    void keysByTheSubscriptionTheMessageWasReceivedOn() {
        // Two subscriptions of one topic deliver the same message id; each is its own input.
        ledger.add(SUB_A, List.of("m1"));

        assertThat(ledger.suppresses(SUB_B, "m1")).isFalse();
        assertThat(ledger.suppresses(SUB_A, "m1")).isTrue();
    }

    @Test
    void expiresOnceTheTimeToLiveHasPassedSinceTheClockStarted() {
        ledger.add(SUB_A, List.of("m1", "m2", "m3"));
        assertThat(ledger.suppresses(SUB_A, "m1")).isTrue();
        ledger.startClock();

        clock.addAndGet(TTL.toNanos() - 1);
        assertThat(ledger.suppresses(SUB_A, "m2")).isTrue();

        clock.addAndGet(1);
        assertThat(ledger.suppresses(SUB_A, "m3")).isFalse();
        assertThat(ledger.suppresses(SUB_A, "m1")).isFalse();
        assertThat(ledger.size()).isZero();
        // m3 was never redelivered before the expiry; m1 and m2 were.
        assertThat(testMetrics.counter(PubSubMetricNames.LEDGER_ENTRIES_EXPIRED)).isEqualTo(1);
    }

    @Test
    void doesNotExpireBeforeTheReaderStartsConsuming() {
        ledger.add(SUB_A, List.of("m1"));

        clock.addAndGet(TTL.toNanos() * 2);

        assertThat(ledger.suppresses(SUB_A, "m1")).isTrue();
    }

    @Test
    void theClockStartsOnce() {
        ledger.add(SUB_A, List.of("m1"));
        ledger.startClock();
        clock.addAndGet(TTL.toNanos() - 1);

        // A later assignment, for example after the reader is handed another split, must not
        // extend the window.
        ledger.startClock();
        clock.addAndGet(1);

        assertThat(ledger.suppresses(SUB_A, "m1")).isFalse();
    }

    @Test
    void ignoresEntriesAddedAfterExpiry() {
        ledger.add(SUB_A, List.of("m1"));
        ledger.startClock();
        clock.addAndGet(TTL.toNanos());
        assertThat(ledger.suppresses(SUB_A, "m1")).isFalse();

        ledger.add(SUB_A, List.of("m2"));

        assertThat(ledger.suppresses(SUB_A, "m2")).isFalse();
        assertThat(ledger.size()).isZero();
    }

    @Test
    void reportsItsSize() {
        ledger.add(SUB_A, List.of("m1", "m2"));
        ledger.add(SUB_B, List.of("m1"));
        ledger.suppresses(SUB_A, "m1");

        assertThat(testMetrics.gauge(PubSubMetricNames.LEDGER_ENTRIES)).isEqualTo(3);
    }

    @Test
    void aCheckpointExpiresALedgerThatNoDeliveryReaches() {
        ledger.add(SUB_A, List.of("m1", "m2"));
        ledger.startClock();
        clock.addAndGet(TTL.toNanos());

        // A subscription that went quiet after the restore delivers nothing to look up.
        assertThat(ledger.expireIfDue()).isTrue();

        assertThat(ledger.size()).isZero();
        assertThat(testMetrics.counter(PubSubMetricNames.LEDGER_ENTRIES_EXPIRED)).isEqualTo(2);
        assertThat(ledger.expireIfDue()).isTrue();
        assertThat(testMetrics.counter(PubSubMetricNames.LEDGER_ENTRIES_EXPIRED)).isEqualTo(2);
    }

    @Test
    void holdsTheIdsTheServiceAssignsAndAnyOtherShapeSideBySide() {
        // Digit-only ids are held packed, everything else as strings; a lookup must not confuse
        // the two forms or ids that differ only in leading or trailing zeros.
        ledger.add(SUB_A, List.of("17045123456789012", "0070", "m1", "170451234567890123"));
        ledger.startClock();

        // A second chunk repeating ids, as two subtasks' chunks of one subscription can.
        ledger.add(SUB_A, List.of("17045123456789012", "m1"));
        assertThat(ledger.size()).isEqualTo(4);
        assertThat(ledger.suppresses(SUB_A, "17045123456789012")).isTrue();
        assertThat(ledger.suppresses(SUB_A, "0070")).isTrue();
        assertThat(ledger.suppresses(SUB_A, "m1")).isTrue();
        assertThat(ledger.suppresses(SUB_A, "170451234567890123")).isTrue();
        assertThat(ledger.suppresses(SUB_A, "070")).isFalse();
        assertThat(ledger.suppresses(SUB_A, "00700")).isFalse();
        assertThat(ledger.suppresses(SUB_A, "1704512345678901")).isFalse();
        assertThat(ledger.suppresses(SUB_B, "17045123456789012")).isFalse();
    }

    @Test
    void findsEveryEntryOfALargeLedgerAndNothingBetweenThem() {
        // Enough entries to grow the table several times, with gaps so that misses fall inside the
        // entries' range as well as outside it.
        List<String> ids = new ArrayList<>();
        for (long i = 0; i < 50_000; i++) {
            ids.add(Long.toString(17_045_123_456_000_000L + 3 * i));
        }
        ledger.add(SUB_A, ids);
        ledger.startClock();

        assertThat(ledger.size()).isEqualTo(ids.size());
        for (long i = 0; i < 50_000; i++) {
            long id = 17_045_123_456_000_000L + 3 * i;
            assertThat(ledger.suppresses(SUB_A, Long.toString(id))).as("%d", id).isTrue();
            assertThat(ledger.suppresses(SUB_A, Long.toString(id + 1))).as("%d", id + 1).isFalse();
        }
        assertThat(ledger.suppresses(SUB_A, "17045123455999999")).isFalse();
    }

    @Test
    void countsUnmatchedPackedEntriesAtExpiryAcrossGrowth() {
        // Matches recorded before the table grows must survive the rehash, or the expiry would
        // count matched entries as never redelivered.
        ledger.add(SUB_A, List.of("10000000000000001", "10000000000000002", "m1"));
        assertThat(ledger.suppresses(SUB_A, "10000000000000001")).isTrue();
        List<String> more = new ArrayList<>();
        for (long i = 0; i < 100; i++) {
            more.add(Long.toString(20_000_000_000_000_000L + i));
        }
        ledger.add(SUB_A, more);
        // Matched again after the rehash, and twice: each entry counts once whatever its copies.
        assertThat(ledger.suppresses(SUB_A, "10000000000000001")).isTrue();
        assertThat(ledger.suppresses(SUB_A, "20000000000000000")).isTrue();
        assertThat(ledger.suppresses(SUB_A, "20000000000000000")).isTrue();
        ledger.startClock();

        clock.addAndGet(TTL.toNanos());
        assertThat(ledger.expireIfDue()).isTrue();
        // 103 entries, two of them matched.
        assertThat(testMetrics.counter(PubSubMetricNames.LEDGER_ENTRIES_EXPIRED)).isEqualTo(101);
    }

    @Test
    void reAddingAMatchedStringIdKeepsItCountedOnceAndMatched() {
        ledger.add(SUB_A, List.of("m1", "m2"));
        assertThat(ledger.suppresses(SUB_A, "m1")).isTrue();
        // A later chunk of the same subscription repeats it.
        ledger.add(SUB_A, List.of("m1"));
        ledger.startClock();

        assertThat(ledger.size()).isEqualTo(2);
        clock.addAndGet(TTL.toNanos());
        assertThat(ledger.expireIfDue()).isTrue();
        assertThat(testMetrics.counter(PubSubMetricNames.LEDGER_ENTRIES_EXPIRED)).isEqualTo(1);
    }

    @Test
    void rejectsANonPositiveTimeToLive() {
        assertThatThrownBy(
                        () -> new RestoredLedger(Duration.ZERO, clock::get, testMetrics.metrics()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
