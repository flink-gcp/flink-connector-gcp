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
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.expressions.ResolvedExpression;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The planner's result of a filter push-down and the filters a scan's query adds: every predicate
 * stays a residual, and those that translated are also reported as accepted.
 *
 * @param <T> the mode's filter type
 */
@Internal
public final class PushedFilters<T> {

    private static final PushedFilters<?> EMPTY =
            new PushedFilters<>(
                    Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

    private final List<ResolvedExpression> accepted;
    private final List<ResolvedExpression> remaining;
    private final List<T> filters;

    PushedFilters(
            List<ResolvedExpression> accepted,
            List<ResolvedExpression> remaining,
            List<T> filters) {
        this.accepted = List.copyOf(accepted);
        this.remaining = List.copyOf(remaining);
        this.filters = List.copyOf(filters);
    }

    /**
     * Returns the state of a scan no predicate has reached.
     *
     * @param <T> the mode's filter type
     * @return the empty state
     */
    @SuppressWarnings("unchecked")
    public static <T> PushedFilters<T> empty() {
        return (PushedFilters<T>) EMPTY;
    }

    /**
     * Returns a state that pushes nothing and keeps every predicate.
     *
     * @param filters the predicates
     * @param <T> the mode's filter type
     * @return the state
     */
    public static <T> PushedFilters<T> declined(List<ResolvedExpression> filters) {
        return new PushedFilters<>(Collections.emptyList(), filters, Collections.emptyList());
    }

    /**
     * Returns the result the planner takes.
     *
     * @return the accepted and remaining predicates
     */
    public SupportsFilterPushDown.Result result() {
        return SupportsFilterPushDown.Result.of(accepted, remaining);
    }

    /**
     * Returns the filters the scan's query adds, all of which it must satisfy.
     *
     * @return the filters, empty when none translated
     */
    public List<T> filters() {
        return filters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PushedFilters<?> that = (PushedFilters<?>) o;
        return accepted.equals(that.accepted)
                && remaining.equals(that.remaining)
                && filters.equals(that.filters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accepted, remaining, filters);
    }
}
