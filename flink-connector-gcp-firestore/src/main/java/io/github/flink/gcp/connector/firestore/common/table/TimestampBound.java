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

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.firestore.common.table.PredicateMatcher.Operator;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * One bound a stored timestamp must satisfy: an operator and a value of whole microseconds, which
 * both modes store and compare exactly.
 *
 * <p>The converter reads a stored timestamp truncated to the column's unit, so a comparison of the
 * read value with a literal is one of the stored value with a bound rounded to that unit: {@code
 * read < L} holds exactly when {@code stored < ceil(L)}, {@code read <= L} when {@code stored <
 * floor(L) + unit}, {@code read > L} when {@code stored >= floor(L) + unit}, {@code read >= L} when
 * {@code stored >= ceil(L)}, and {@code read = L}, for a literal on the unit, when {@code L <=
 * stored < L + unit}. Every bound is then a whole number of microseconds, so the service never
 * rounds one; Datastore mode floors a finer literal, which would turn {@code < T + 500ns} into
 * {@code < T} and drop {@code T}.
 */
@Internal
public final class TimestampBound {

    /** The precision both modes store: whole microseconds. */
    private static final int STORED_UNIT_NANOS = 1_000;

    private static final Instant MIN = Instant.parse("0001-01-01T00:00:00Z");

    private static final Instant MAX = Instant.parse("9999-12-31T23:59:59.999999999Z");

    private final Operator operator;
    private final Instant value;

    private TimestampBound(Operator operator, Instant value) {
        this.operator = operator;
        this.value = value;
    }

    /**
     * Returns the unit a converter truncates a stored timestamp to at a precision, never finer than
     * the microseconds both modes store.
     *
     * @param precision the column's precision, from 0 to 9
     * @return the unit in nanoseconds
     */
    static int unitNanos(int precision) {
        int unit = 1;
        for (int digits = precision; digits < 9; digits++) {
            unit *= 10;
        }
        return Math.max(unit, STORED_UNIT_NANOS);
    }

    /**
     * Returns the bounds for a comparison of a value read at {@code unitNanos} with a literal.
     *
     * @param operator the comparison's operator
     * @param literal the literal
     * @param unitNanos the column's truncation unit in nanoseconds, a multiple of 1,000 that
     *     divides a second
     * @return the bounds, all of which the stored value must satisfy; empty when the comparison has
     *     none, or one falls outside the range the services store
     */
    static List<TimestampBound> of(Operator operator, Instant literal, int unitNanos) {
        int nano = literal.getNano();
        Instant floor = literal.minusNanos(nano % unitNanos);
        boolean aligned = floor.equals(literal);
        Instant ceil = aligned ? literal : floor.plusNanos(unitNanos);
        List<TimestampBound> bounds;
        switch (operator) {
            case EQUAL:
                if (!aligned) {
                    // No value read at the unit equals the literal.
                    return List.of();
                }
                bounds =
                        unitNanos == STORED_UNIT_NANOS
                                ? List.of(new TimestampBound(Operator.EQUAL, literal))
                                : List.of(
                                        new TimestampBound(Operator.GREATER_THAN_OR_EQUAL, literal),
                                        new TimestampBound(
                                                Operator.LESS_THAN, literal.plusNanos(unitNanos)));
                break;
            case NOT_EQUAL:
                // Below the stored precision, values read as unequal to the literal lie on both
                // sides of a range, which no single not-equal filter states.
                if (!aligned || unitNanos != STORED_UNIT_NANOS) {
                    return List.of();
                }
                bounds = List.of(new TimestampBound(Operator.NOT_EQUAL, literal));
                break;
            case LESS_THAN:
                bounds = List.of(new TimestampBound(Operator.LESS_THAN, ceil));
                break;
            case LESS_THAN_OR_EQUAL:
                bounds =
                        List.of(new TimestampBound(Operator.LESS_THAN, floor.plusNanos(unitNanos)));
                break;
            case GREATER_THAN:
                bounds =
                        List.of(
                                new TimestampBound(
                                        Operator.GREATER_THAN_OR_EQUAL,
                                        floor.plusNanos(unitNanos)));
                break;
            case GREATER_THAN_OR_EQUAL:
                bounds = List.of(new TimestampBound(Operator.GREATER_THAN_OR_EQUAL, ceil));
                break;
            default:
                return List.of();
        }
        for (TimestampBound bound : bounds) {
            if (bound.value.isBefore(MIN) || bound.value.isAfter(MAX)) {
                return List.of();
            }
        }
        return bounds;
    }

    /**
     * Returns the operator the stored value is compared with.
     *
     * @return the operator, never {@link Operator#IS_NOT_NULL}
     */
    public Operator operator() {
        return operator;
    }

    /**
     * Returns the value the stored value is compared with, a whole number of microseconds.
     *
     * @return the value
     */
    public Instant value() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TimestampBound that = (TimestampBound) o;
        return operator == that.operator && value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operator, value);
    }

    @Override
    public String toString() {
        return operator + " " + value;
    }
}
