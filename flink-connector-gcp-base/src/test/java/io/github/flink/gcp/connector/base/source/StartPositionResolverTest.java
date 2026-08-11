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

package io.github.flink.gcp.connector.base.source;

import org.apache.flink.util.FlinkRuntimeException;

import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link StartPositionResolver}. */
class StartPositionResolverTest {

    private static final class LogOwner {}

    private static final Instant NOW = Instant.parse("2026-08-11T12:00:00Z");
    private static final Duration RETENTION = Duration.ofDays(7);
    private static final Instant EARLIEST = NOW.minus(RETENTION).plus(Duration.ofMinutes(1));

    @Test
    void resolvesEveryFreshPositionAgainstOneStartupInstant() throws Exception {
        CountingClock clock = new CountingClock(NOW);
        AtomicInteger retentionLookups = new AtomicInteger();
        StartPositionResolver resolver = resolver(clock, retentionLookups, () -> RETENTION);

        assertThat(resolver.resolve(StartPosition.latest())).isEqualTo(NOW);
        assertThat(resolver.resolve(StartPosition.at(NOW.minus(Duration.ofHours(1)))))
                .isEqualTo(NOW.minus(Duration.ofHours(1)));
        assertThat(resolver.resolve(StartPosition.ago(Duration.ofHours(2))))
                .isEqualTo(NOW.minus(Duration.ofHours(2)));
        assertThat(resolver.resolve(StartPosition.earliest())).isEqualTo(EARLIEST);
        assertThat(clock.calls).hasValue(1);
        assertThat(retentionLookups).hasValue(1);
    }

    @Test
    void latestNeedsNoRetentionLookup() throws Exception {
        AtomicInteger retentionLookups = new AtomicInteger();
        StartPositionResolver resolver =
                resolver(
                        Clock.fixed(NOW, ZoneOffset.UTC),
                        retentionLookups,
                        () -> {
                            throw new AssertionError("latest must not discover retention");
                        });

        assertThat(resolver.resolve(StartPosition.latest())).isEqualTo(NOW);
        assertThat(retentionLookups).hasValue(0);
    }

    @Test
    void rejectsAFuturePositionBeforeDiscoveringRetention() {
        AtomicInteger retentionLookups = new AtomicInteger();
        StartPositionResolver resolver =
                resolver(Clock.fixed(NOW, ZoneOffset.UTC), retentionLookups, () -> RETENTION);
        Instant future = NOW.plusNanos(1);

        assertThatThrownBy(() -> resolver.resolve(StartPosition.at(future)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(future.toString())
                .hasMessageContaining(NOW.toString())
                .hasMessageContaining("latest()");
        assertThat(retentionLookups).hasValue(0);
    }

    @Test
    void clampsAnOlderPositionAndWarnsWithTheLostRange() throws Exception {
        Instant requested = EARLIEST.minus(Duration.ofHours(4));

        try (LogCapture capture = LogCapture.of(LogOwner.class)) {
            StartPositionResolver resolver = resolver(() -> RETENTION);

            assertThat(resolver.resolve(StartPosition.at(requested))).isEqualTo(EARLIEST);
            assertThat(capture.getMessages())
                    .singleElement()
                    .satisfies(
                            message ->
                                    assertThat(message)
                                            .contains(requested.toString())
                                            .contains(EARLIEST.toString())
                                            .contains("PT4H"));
        }
    }

    @Test
    void anExactEarliestPositionIsNotClampedOrWarned() throws Exception {
        try (LogCapture capture = LogCapture.of(LogOwner.class)) {
            StartPositionResolver resolver = resolver(() -> RETENTION);

            assertThat(resolver.resolve(StartPosition.at(EARLIEST))).isEqualTo(EARLIEST);
            assertThat(capture.getMessages()).isEmpty();
        }
    }

    @Test
    void anUnrepresentableRelativePositionHasAConfigurationError() {
        StartPositionResolver resolver = resolver(() -> RETENTION);
        StartPosition requested = StartPosition.ago(Duration.ofSeconds(Long.MAX_VALUE));

        assertThatThrownBy(() -> resolver.resolve(requested))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(requested.toString())
                .hasMessageContaining(NOW.toString());
    }

    @Test
    void rejectsAnInvalidRetentionResult() {
        StartPositionResolver tooShort =
                resolver(() -> StartPositionResolver.RETENTION_SAFETY_MARGIN);
        StartPositionResolver absent = resolver(() -> null);

        assertThatThrownBy(() -> tooShort.resolve(StartPosition.earliest()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("longer than")
                .hasMessageContaining("PT1M");
        assertThatThrownBy(() -> absent.resolve(StartPosition.earliest()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("retentionLookup must not return null");
    }

    @Test
    void validRestoredStateWinsWithoutResolvingAFallback() throws Exception {
        StartPositionResolver resolver = resolver(() -> RETENTION);

        assertThat(
                        resolver.resolveRestored(
                                "partition-a", EARLIEST, Optional.of(StartPosition.latest())))
                .isEmpty();
        assertThat(
                        resolver.resolveRestored(
                                "partition-b",
                                EARLIEST.plusSeconds(1),
                                Optional.of(StartPosition.earliest())))
                .isEmpty();
    }

    @Test
    void expiredRestoredStateFailsByDefaultWithRecoveryGuidance() {
        StartPositionResolver resolver = resolver(() -> RETENTION);
        Instant restored = EARLIEST.minus(Duration.ofHours(6));

        assertThatThrownBy(
                        () -> resolver.resolveRestored("partition-a", restored, Optional.empty()))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("partition-a")
                .hasMessageContaining(restored.toString())
                .hasMessageContaining(EARLIEST.toString())
                .hasMessageContaining("PT6H")
                .hasMessageContaining("without restored state")
                .hasMessageContaining("StartPosition");
    }

    @Test
    void fallbackRestartsOnlyTheExpiredPartitionAndWarnsOnce() throws Exception {
        Instant restored = EARLIEST.minus(Duration.ofHours(6));

        try (LogCapture capture = LogCapture.of(LogOwner.class)) {
            StartPositionResolver resolver = resolver(() -> RETENTION);

            assertThat(
                            resolver.resolveRestored(
                                    "partition-a", restored, Optional.of(StartPosition.latest())))
                    .contains(NOW);
            assertThat(capture.getMessages())
                    .singleElement()
                    .satisfies(
                            message ->
                                    assertThat(message)
                                            .contains("partition-a")
                                            .contains(restored.toString())
                                            .contains(EARLIEST.toString())
                                            .contains("PT6H")
                                            .contains(StartPosition.latest().toString())
                                            .contains(NOW.toString()));
        }
    }

    @Test
    void anOldFallbackIsClampedInsideThePartitionWarning() throws Exception {
        Instant restored = EARLIEST.minus(Duration.ofHours(6));
        Instant fallback = EARLIEST.minus(Duration.ofHours(2));

        try (LogCapture capture = LogCapture.of(LogOwner.class)) {
            StartPositionResolver resolver = resolver(() -> RETENTION);

            assertThat(
                            resolver.resolveRestored(
                                    "partition-a",
                                    restored,
                                    Optional.of(StartPosition.at(fallback))))
                    .contains(EARLIEST);
            assertThat(capture.getMessages())
                    .singleElement()
                    .satisfies(
                            message ->
                                    assertThat(message)
                                            .contains(fallback.toString())
                                            .contains(EARLIEST.toString()));
        }
    }

    @Test
    void aFutureFallbackIsRejectedRatherThanHidingTheMisconfiguration() {
        StartPositionResolver resolver = resolver(() -> RETENTION);

        assertThatThrownBy(
                        () ->
                                resolver.resolveRestored(
                                        "partition-a",
                                        EARLIEST.minusSeconds(1),
                                        Optional.of(StartPosition.at(NOW.plusSeconds(1)))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("after the enumerator startup instant");
    }

    @Test
    void restoredPartitionsShareOneRetentionLookupButReportTheirOwnLostWindows() throws Exception {
        AtomicInteger retentionLookups = new AtomicInteger();

        try (LogCapture capture = LogCapture.of(LogOwner.class)) {
            StartPositionResolver resolver =
                    resolver(Clock.fixed(NOW, ZoneOffset.UTC), retentionLookups, () -> RETENTION);

            resolver.resolveRestored(
                    "partition-a",
                    EARLIEST.minus(Duration.ofHours(1)),
                    Optional.of(StartPosition.latest()));
            resolver.resolveRestored(
                    "partition-b",
                    EARLIEST.minus(Duration.ofHours(2)),
                    Optional.of(StartPosition.latest()));

            assertThat(retentionLookups).hasValue(1);
            assertThat(capture.getMessages())
                    .anySatisfy(message -> assertThat(message).contains("partition-a", "PT1H"))
                    .anySatisfy(message -> assertThat(message).contains("partition-b", "PT2H"));
        }
    }

    private static StartPositionResolver resolver(
            StartPositionResolver.RetentionLookup retentionLookup) {
        return StartPositionResolver.create(
                LogOwner.class, retentionLookup, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    private static StartPositionResolver resolver(
            Clock clock,
            AtomicInteger calls,
            StartPositionResolver.RetentionLookup retentionLookup) {
        return StartPositionResolver.create(
                LogOwner.class,
                () -> {
                    calls.incrementAndGet();
                    return retentionLookup.get();
                },
                clock);
    }

    private static final class CountingClock extends Clock {

        private final Instant instant;
        private final AtomicInteger calls = new AtomicInteger();

        private CountingClock(Instant instant) {
            this.instant = instant;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            calls.incrementAndGet();
            return instant;
        }
    }
}
