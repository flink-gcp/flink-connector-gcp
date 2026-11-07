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

package io.github.flink.gcp.connector.firestore.common.table;

import io.github.flink.gcp.connector.firestore.common.table.PredicateMatcher.Operator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.function.LongPredicate;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the bounds a stored timestamp must satisfy for a comparison of its truncated value. */
class TimestampBoundTest {

    private static final Instant T = Instant.parse("2026-11-07T00:00:01.123456Z");

    private static final int MILLIS = 1_000_000;

    private static final int MICROS = 1_000;

    @Test
    void anEqualityBelowTheStoredPrecisionIsTheRangeOfOneUnit() {
        Instant aligned = Instant.parse("2026-11-07T00:00:01.123Z");

        assertThat(TimestampBound.of(Operator.EQUAL, aligned, MILLIS))
                .extracting(Object::toString)
                .containsExactly(
                        "GREATER_THAN_OR_EQUAL 2026-11-07T00:00:01.123Z",
                        "LESS_THAN 2026-11-07T00:00:01.124Z");
        assertThat(TimestampBound.of(Operator.EQUAL, T, MICROS))
                .extracting(Object::toString)
                .containsExactly("EQUAL 2026-11-07T00:00:01.123456Z");
        // No value read at the unit equals a literal off it.
        assertThat(TimestampBound.of(Operator.EQUAL, T, MILLIS)).isEmpty();
        assertThat(TimestampBound.of(Operator.EQUAL, T.plusNanos(1), MICROS)).isEmpty();
    }

    @Test
    void aNotEqualTranslatesOnlyAtTheStoredPrecision() {
        assertThat(TimestampBound.of(Operator.NOT_EQUAL, T, MICROS)).hasSize(1);
        assertThat(TimestampBound.of(Operator.NOT_EQUAL, T.plusNanos(1), MICROS)).isEmpty();
        assertThat(TimestampBound.of(Operator.NOT_EQUAL, T.minusNanos(456_000), MILLIS)).isEmpty();
    }

    @Test
    void aBoundPastTheRangeTheServicesStoreTranslatesToNothing() {
        Instant last = Instant.parse("9999-12-31T23:59:59.999Z");

        assertThat(TimestampBound.of(Operator.LESS_THAN_OR_EQUAL, last, MILLIS)).isEmpty();
        assertThat(TimestampBound.of(Operator.GREATER_THAN_OR_EQUAL, last, MILLIS)).hasSize(1);
    }

    /**
     * For random stored microseconds and literals, a stored value satisfies the bounds exactly when
     * its value truncated to the unit satisfies the comparison: the bounds neither drop nor admit a
     * value.
     */
    @ParameterizedTest
    @EnumSource(
            value = Operator.class,
            names = {
                "EQUAL",
                "NOT_EQUAL",
                "LESS_THAN",
                "LESS_THAN_OR_EQUAL",
                "GREATER_THAN",
                "GREATER_THAN_OR_EQUAL"
            })
    void theBoundsHoldExactlyWhenTheTruncatedValueSatisfiesTheComparison(Operator operator) {
        Random random = new Random(operator.ordinal());
        int[] units = {MICROS, 10 * MICROS, MILLIS, 1_000_000_000};
        long base = T.getEpochSecond() * 1_000_000_000L;
        for (int unit : units) {
            for (int i = 0; i < 2_000; i++) {
                // Within a few units either side, the stored value on whole microseconds.
                long stored =
                        base + ((long) (random.nextDouble() * 8 * unit) / MICROS) * (long) MICROS;
                // A literal anywhere, on the unit or off it.
                long literal = base + (long) (random.nextDouble() * 8 * unit);
                if (i % 4 == 0) {
                    literal -= literal % unit;
                }
                List<TimestampBound> bounds = TimestampBound.of(operator, instant(literal), unit);
                long read = stored - Math.floorMod(stored, unit);
                boolean flink = compare(operator).test(Long.compare(read, literal));
                if (bounds.isEmpty()) {
                    // Declined: nothing to check but that the predicate is not always false.
                    continue;
                }
                boolean service = true;
                for (TimestampBound bound : bounds) {
                    long value = nanos(bound.value());
                    assertThat(value % MICROS).isZero();
                    service &= compare(bound.operator()).test(Long.compare(stored, value));
                }
                assertThat(service)
                        .as("%s stored %s literal %s unit %s", operator, stored, literal, unit)
                        .isEqualTo(flink);
            }
        }
    }

    private static LongPredicate compare(Operator operator) {
        switch (operator) {
            case EQUAL:
                return c -> c == 0;
            case NOT_EQUAL:
                return c -> c != 0;
            case LESS_THAN:
                return c -> c < 0;
            case LESS_THAN_OR_EQUAL:
                return c -> c <= 0;
            case GREATER_THAN:
                return c -> c > 0;
            case GREATER_THAN_OR_EQUAL:
                return c -> c >= 0;
            default:
                throw new IllegalArgumentException(operator.toString());
        }
    }

    private static Instant instant(long nanos) {
        return Instant.ofEpochSecond(0, nanos);
    }

    private static long nanos(Instant instant) {
        return instant.getEpochSecond() * 1_000_000_000L + instant.getNano();
    }
}
