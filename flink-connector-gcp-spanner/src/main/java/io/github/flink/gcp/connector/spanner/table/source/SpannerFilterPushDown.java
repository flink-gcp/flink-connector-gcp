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

package io.github.flink.gcp.connector.spanner.table.source;

import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import io.github.flink.gcp.connector.spanner.table.SpannerTableSchemaConverter;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Translates the exact subset of Flink predicates that Spanner key reads can preserve.
 *
 * <h2>What "the exact subset" is, and why it is that small</h2>
 *
 * <p>A Spanner read is not a query: it takes a {@code KeySet} — points and ranges over the key, in
 * key order. Everything below follows from that one shape.
 *
 * <p>{@link #compile} walks the key columns <b>in key order</b> and stops at the first one an
 * equality does not pin. Equalities extend the prefix; the first column carrying only inequalities
 * contributes the range and ends the walk; a column carrying nothing ends it too. So {@code a = 1
 * AND b > 2} is expressible and {@code b > 2} alone is not — not because the second is harder, but
 * because a {@code KeySet} has no way to say "any {@code a}, with {@code b} above 2".
 *
 * <p>Only {@code AND} is descended. An {@code OR} is left untranslated rather than turned into a
 * union of ranges: a {@code KeySet} can hold several ranges, but proving that a particular union is
 * exactly the predicate — rather than a superset that silently drops the leftover — is a different
 * problem from the prefix walk, and this class does not attempt it.
 *
 * <p>Comparisons are normalized so that the column is always on the left; {@code 10 < id} is parsed
 * as {@code id > 10}. Two types are carved out because a range needs a total order the key agrees
 * with: a {@code Double} is refused outside equality and refused entirely when it is {@code NaN},
 * and a {@code UUID} is accepted only for equality.
 *
 * <h2>The rule that keeps this safe</h2>
 *
 * <p><b>{@code accepted} and {@code remaining} are not complements, and a filter is routinely in
 * both.</b> A filter joins {@code accepted} when the key constraint used <em>any</em> of its
 * predicates, which is what tells the planner the source did something with it. It stays in {@code
 * remaining} unless it is <em>exact</em> — every one of its predicates used, no part of it
 * unsupported, and no {@code IS NOT NULL} among them.
 *
 * <p>That asymmetry is the correctness property of this whole class. The key read is allowed to be
 * a superset of the predicate; what is not allowed is for a partially expressible filter to be
 * reported as fully handled, because the rows the range over-selects would then never be filtered
 * out. An {@code IS NOT NULL} is never exact for the same reason: a Spanner key range does not
 * exclude nulls the way the SQL predicate does, so the filter has to run as well.
 *
 * <p>Contradictions are resolved here rather than sent: {@code a = 1 AND a = 2}, or a lower bound
 * above its upper, compile to an empty key set, so Spanner is never asked to read a range that
 * cannot match.
 *
 * <p>A read through a secondary index accepts nothing. These predicates were compiled against the
 * <em>primary</em> key, and an index has a key of its own; the state carries them all the same, so
 * that {@code RuntimeState.keySet} can recompile against whichever key the read actually uses.
 */
final class SpannerFilterPushDown {

    private SpannerFilterPushDown() {}

    static State translate(
            SpannerTableSchemaConverter schema,
            List<ResolvedExpression> filters,
            boolean secondaryIndex) {
        List<ParsedFilter> parsedFilters = new ArrayList<>(filters.size());
        List<Predicate> predicates = new ArrayList<>();
        Set<Integer> nonNullFields = new LinkedHashSet<>();
        int nextId = 0;
        for (ResolvedExpression filter : filters) {
            ParsedFilter parsed = new ParsedFilter(filter);
            nextId = parse(schema, filter, parsed, predicates, nonNullFields, nextId);
            parsedFilters.add(parsed);
        }

        List<KeyColumn> primaryKey = new ArrayList<>();
        for (int index : schema.getPrimaryKeyIndexes()) {
            primaryKey.add(
                    new KeyColumn(schema.getColumns().get(index).getName(), index, false, false));
        }
        Compiled primary = compile(primaryKey, predicates);
        @Nullable KeyConstraint primaryConstraint = primary.constraint;
        Set<Integer> used = secondaryIndex ? Collections.emptySet() : primary.usedPredicateIds;

        List<ResolvedExpression> accepted = new ArrayList<>();
        List<ResolvedExpression> remaining = new ArrayList<>();
        for (ParsedFilter parsed : parsedFilters) {
            boolean contributes = !secondaryIndex && parsed.contributesAny(used);
            if (contributes) {
                accepted.add(parsed.expression);
            }
            if (secondaryIndex || !parsed.isExact(used)) {
                remaining.add(parsed.expression);
            }
        }
        return new State(
                accepted, remaining, predicates, nonNullFields, secondaryIndex, primaryConstraint);
    }

    private static int parse(
            SpannerTableSchemaConverter schema,
            ResolvedExpression expression,
            ParsedFilter parent,
            List<Predicate> predicates,
            Set<Integer> nonNullFields,
            int nextId) {
        if (!(expression instanceof CallExpression)) {
            parent.unsupported = true;
            return nextId;
        }
        CallExpression call = (CallExpression) expression;
        FunctionDefinition function = call.getFunctionDefinition();
        List<ResolvedExpression> children = call.getResolvedChildren();
        if (function.equals(BuiltInFunctionDefinitions.AND)) {
            for (ResolvedExpression child : children) {
                nextId = parse(schema, child, parent, predicates, nonNullFields, nextId);
            }
            return nextId;
        }
        if (function.equals(BuiltInFunctionDefinitions.IS_NOT_NULL)
                && children.size() == 1
                && children.get(0) instanceof FieldReferenceExpression) {
            int field = ((FieldReferenceExpression) children.get(0)).getFieldIndex();
            nonNullFields.add(field);
            parent.nonNullFields.add(field);
            return nextId;
        }
        Comparison comparison = Comparison.of(function);
        if (comparison == null || children.size() != 2) {
            parent.unsupported = true;
            return nextId;
        }

        FieldReferenceExpression field;
        ValueLiteralExpression literal;
        Comparison normalized;
        if (children.get(0) instanceof FieldReferenceExpression
                && children.get(1) instanceof ValueLiteralExpression) {
            field = (FieldReferenceExpression) children.get(0);
            literal = (ValueLiteralExpression) children.get(1);
            normalized = comparison;
        } else if (children.get(1) instanceof FieldReferenceExpression
                && children.get(0) instanceof ValueLiteralExpression) {
            field = (FieldReferenceExpression) children.get(1);
            literal = (ValueLiteralExpression) children.get(0);
            normalized = comparison.reverse();
        } else {
            parent.unsupported = true;
            return nextId;
        }

        int fieldIndex = field.getFieldIndex();
        if (fieldIndex < 0 || fieldIndex >= schema.getColumns().size()) {
            parent.unsupported = true;
            return nextId;
        }
        SpannerTableSchemaConverter.Column column = schema.getColumns().get(fieldIndex);
        Optional<Object> encoded = SpannerKeyValueEncoder.literalValue(column, literal);
        if (!encoded.isPresent()
                || (encoded.get() instanceof Double
                        && (Double.isNaN((Double) encoded.get())
                                || normalized != Comparison.EQUALS))
                || (encoded.get() instanceof UUID && normalized != Comparison.EQUALS)) {
            parent.unsupported = true;
            return nextId;
        }
        Predicate predicate = new Predicate(nextId, fieldIndex, normalized, encoded.get());
        predicates.add(predicate);
        parent.predicateIds.add(nextId);
        return nextId + 1;
    }

    static Compiled compile(List<KeyColumn> keyColumns, List<Predicate> predicates) {
        Set<Integer> used = new HashSet<>();
        List<Object> prefix = new ArrayList<>();
        @Nullable Bound lower = null;
        @Nullable Bound upper = null;
        boolean empty = false;

        for (KeyColumn keyColumn : keyColumns) {
            List<Predicate> onColumn = predicatesFor(predicates, keyColumn.physicalIndex());
            List<Predicate> equalities = withComparison(onColumn, Comparison.EQUALS);
            if (!equalities.isEmpty()) {
                Object equality = equalities.get(0).value;
                for (Predicate predicate : onColumn) {
                    used.add(predicate.id);
                    if (!predicate.matches(equality)) {
                        empty = true;
                    }
                }
                prefix.add(equality);
                if (empty) {
                    break;
                }
                continue;
            }

            for (Predicate predicate : onColumn) {
                if (predicate.comparison.isLower()) {
                    lower = strongerLower(lower, predicate);
                    used.add(predicate.id);
                } else if (predicate.comparison.isUpper()) {
                    upper = strongerUpper(upper, predicate);
                    used.add(predicate.id);
                }
            }
            if (lower != null || upper != null) {
                if (lower != null && upper != null) {
                    int comparison = SpannerKeyValueEncoder.compare(lower.value, upper.value);
                    empty = comparison > 0 || (comparison == 0 && (!lower.closed || !upper.closed));
                }
                break;
            }
            break;
        }

        @Nullable KeyConstraint constraint = null;
        if (!used.isEmpty()) {
            constraint =
                    new KeyConstraint(
                            prefix, prefix.size() == keyColumns.size(), lower, upper, empty);
        }
        return new Compiled(constraint, used);
    }

    private static List<Predicate> predicatesFor(List<Predicate> predicates, int field) {
        List<Predicate> result = new ArrayList<>();
        if (field < 0) {
            return result;
        }
        for (Predicate predicate : predicates) {
            if (predicate.physicalField == field) {
                result.add(predicate);
            }
        }
        return result;
    }

    private static List<Predicate> withComparison(
            List<Predicate> predicates, Comparison comparison) {
        List<Predicate> result = new ArrayList<>();
        for (Predicate predicate : predicates) {
            if (predicate.comparison == comparison) {
                result.add(predicate);
            }
        }
        return result;
    }

    private static Bound strongerLower(@Nullable Bound current, Predicate predicate) {
        Bound candidate =
                new Bound(
                        predicate.value, predicate.comparison == Comparison.GREATER_THAN_OR_EQUAL);
        if (current == null) {
            return candidate;
        }
        int comparison = SpannerKeyValueEncoder.compare(candidate.value, current.value);
        if (comparison > 0 || (comparison == 0 && !candidate.closed)) {
            return candidate;
        }
        return current;
    }

    private static Bound strongerUpper(@Nullable Bound current, Predicate predicate) {
        Bound candidate =
                new Bound(predicate.value, predicate.comparison == Comparison.LESS_THAN_OR_EQUAL);
        if (current == null) {
            return candidate;
        }
        int comparison = SpannerKeyValueEncoder.compare(candidate.value, current.value);
        if (comparison < 0 || (comparison == 0 && !candidate.closed)) {
            return candidate;
        }
        return current;
    }

    static final class State {

        private static final State EMPTY =
                new State(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptySet(),
                        false,
                        null);

        private final List<ResolvedExpression> accepted;
        private final List<ResolvedExpression> remaining;
        private final List<Predicate> predicates;
        private final Set<Integer> nonNullFields;
        private final boolean secondaryIndex;
        @Nullable private final KeyConstraint primaryConstraint;

        private State(
                List<ResolvedExpression> accepted,
                List<ResolvedExpression> remaining,
                List<Predicate> predicates,
                Set<Integer> nonNullFields,
                boolean secondaryIndex,
                @Nullable KeyConstraint primaryConstraint) {
            this.accepted = Collections.unmodifiableList(new ArrayList<>(accepted));
            this.remaining = Collections.unmodifiableList(new ArrayList<>(remaining));
            this.predicates = Collections.unmodifiableList(new ArrayList<>(predicates));
            this.nonNullFields = Collections.unmodifiableSet(new LinkedHashSet<>(nonNullFields));
            this.secondaryIndex = secondaryIndex;
            this.primaryConstraint = primaryConstraint;
        }

        static State empty() {
            return EMPTY;
        }

        SupportsFilterPushDown.Result result() {
            return SupportsFilterPushDown.Result.of(accepted, remaining);
        }

        @Nullable
        KeySet keySet(List<KeyColumn> keyColumns) {
            return runtime().keySet(keyColumns);
        }

        boolean hasPrimaryKeyConstraint() {
            return primaryConstraint != null;
        }

        @Nullable
        KeySet directionIndependentPrimaryKeySet(List<KeyColumn> primaryKey) {
            return primaryConstraint == null || primaryConstraint.hasRange()
                    ? null
                    : primaryConstraint.toKeySet(primaryKey);
        }

        RuntimeState runtime() {
            return new RuntimeState(predicates, nonNullFields, primaryConstraint);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            State state = (State) o;
            return secondaryIndex == state.secondaryIndex
                    && accepted.equals(state.accepted)
                    && remaining.equals(state.remaining)
                    && predicates.equals(state.predicates)
                    && nonNullFields.equals(state.nonNullFields)
                    && Objects.equals(primaryConstraint, state.primaryConstraint);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    accepted,
                    remaining,
                    predicates,
                    nonNullFields,
                    secondaryIndex,
                    primaryConstraint);
        }
    }

    static final class RuntimeState implements Serializable {
        private static final long serialVersionUID = 1L;

        private final List<Predicate> predicates;
        private final Set<Integer> nonNullFields;
        @Nullable private final KeyConstraint primaryConstraint;

        private RuntimeState(
                List<Predicate> predicates,
                Set<Integer> nonNullFields,
                @Nullable KeyConstraint primaryConstraint) {
            this.predicates = Collections.unmodifiableList(new ArrayList<>(predicates));
            this.nonNullFields = Collections.unmodifiableSet(new LinkedHashSet<>(nonNullFields));
            this.primaryConstraint = primaryConstraint;
        }

        @Nullable
        KeySet keySet(List<KeyColumn> keyColumns) {
            Compiled compiled = compile(keyColumns, predicates);
            return compiled.constraint == null ? null : compiled.constraint.toKeySet(keyColumns);
        }

        boolean provesNonNull(int physicalField) {
            if (nonNullFields.contains(physicalField)) {
                return true;
            }
            for (Predicate predicate : predicates) {
                if (predicate.physicalField == physicalField) {
                    return true;
                }
            }
            return false;
        }

        boolean matchesPrimaryKey(Key key) {
            return primaryConstraint == null || primaryConstraint.matches(key);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RuntimeState that = (RuntimeState) o;
            return predicates.equals(that.predicates)
                    && nonNullFields.equals(that.nonNullFields)
                    && Objects.equals(primaryConstraint, that.primaryConstraint);
        }

        @Override
        public int hashCode() {
            return Objects.hash(predicates, nonNullFields, primaryConstraint);
        }
    }

    static final class Compiled {
        @Nullable private final KeyConstraint constraint;
        private final Set<Integer> usedPredicateIds;

        private Compiled(@Nullable KeyConstraint constraint, Set<Integer> usedPredicateIds) {
            this.constraint = constraint;
            this.usedPredicateIds = Collections.unmodifiableSet(new HashSet<>(usedPredicateIds));
        }
    }

    private static final class ParsedFilter {
        private final ResolvedExpression expression;
        private final Set<Integer> predicateIds = new LinkedHashSet<>();
        private final Set<Integer> nonNullFields = new LinkedHashSet<>();
        private boolean unsupported;

        private ParsedFilter(ResolvedExpression expression) {
            this.expression = expression;
        }

        private boolean contributesAny(Set<Integer> used) {
            for (int id : predicateIds) {
                if (used.contains(id)) {
                    return true;
                }
            }
            return false;
        }

        private boolean isExact(Set<Integer> used) {
            return !unsupported
                    && nonNullFields.isEmpty()
                    && !predicateIds.isEmpty()
                    && used.containsAll(predicateIds);
        }
    }

    private static final class Predicate implements Serializable {
        private static final long serialVersionUID = 1L;

        private final int id;
        private final int physicalField;
        private final Comparison comparison;
        private final Object value;

        private Predicate(int id, int physicalField, Comparison comparison, Object value) {
            this.id = id;
            this.physicalField = physicalField;
            this.comparison = comparison;
            this.value = value;
        }

        private boolean matches(Object candidate) {
            int result = SpannerKeyValueEncoder.compare(candidate, value);
            switch (comparison) {
                case EQUALS:
                    return result == 0;
                case LESS_THAN:
                    return result < 0;
                case LESS_THAN_OR_EQUAL:
                    return result <= 0;
                case GREATER_THAN:
                    return result > 0;
                case GREATER_THAN_OR_EQUAL:
                    return result >= 0;
                default:
                    throw new IllegalStateException("Unknown comparison " + comparison);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Predicate predicate = (Predicate) o;
            return id == predicate.id
                    && physicalField == predicate.physicalField
                    && comparison == predicate.comparison
                    && value.equals(predicate.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, physicalField, comparison, value);
        }
    }

    private static final class KeyConstraint implements Serializable {
        private static final long serialVersionUID = 1L;

        private final List<Object> prefix;
        private final boolean completeKey;
        @Nullable private final Bound lower;
        @Nullable private final Bound upper;
        private final boolean empty;

        private KeyConstraint(
                List<Object> prefix,
                boolean completeKey,
                @Nullable Bound lower,
                @Nullable Bound upper,
                boolean empty) {
            this.prefix = Collections.unmodifiableList(new ArrayList<>(prefix));
            this.completeKey = completeKey;
            this.lower = lower;
            this.upper = upper;
            this.empty = empty;
        }

        private boolean hasRange() {
            return lower != null || upper != null;
        }

        private KeySet toKeySet(List<KeyColumn> keyColumns) {
            if (empty) {
                return KeySet.newBuilder().build();
            }
            if (completeKey && lower == null && upper == null) {
                return KeySet.singleKey(key(prefix, null));
            }
            if (lower == null && upper == null) {
                Key prefixKey = key(prefix, null);
                return KeySet.range(KeyRange.closedClosed(prefixKey, prefixKey));
            }

            KeyColumn rangedPart = keyColumns.get(prefix.size());
            Bound start = rangedPart.isDescending() ? upper : lower;
            Bound end = rangedPart.isDescending() ? lower : upper;
            KeyRange.Builder range = KeyRange.newBuilder();
            range.setStart(start == null ? key(prefix, null) : key(prefix, start.value));
            range.setStartType(
                    start == null || start.closed
                            ? KeyRange.Endpoint.CLOSED
                            : KeyRange.Endpoint.OPEN);
            range.setEnd(end == null ? key(prefix, null) : key(prefix, end.value));
            range.setEndType(
                    end == null || end.closed ? KeyRange.Endpoint.CLOSED : KeyRange.Endpoint.OPEN);
            return KeySet.range(range.build());
        }

        private boolean matches(Key key) {
            if (empty) {
                return false;
            }
            Iterator<Object> parts = key.getParts().iterator();
            for (Object expected : prefix) {
                if (!parts.hasNext()
                        || SpannerKeyValueEncoder.compare(parts.next(), expected) != 0) {
                    return false;
                }
            }
            if (completeKey && parts.hasNext()) {
                return false;
            }
            if (lower == null && upper == null) {
                return true;
            }
            if (!parts.hasNext()) {
                return false;
            }
            Object ranged = parts.next();
            if (lower != null) {
                int comparison = SpannerKeyValueEncoder.compare(ranged, lower.value);
                if (comparison < 0 || (comparison == 0 && !lower.closed)) {
                    return false;
                }
            }
            if (upper != null) {
                int comparison = SpannerKeyValueEncoder.compare(ranged, upper.value);
                if (comparison > 0 || (comparison == 0 && !upper.closed)) {
                    return false;
                }
            }
            return true;
        }

        private static Key key(List<Object> prefix, @Nullable Object last) {
            Key.Builder key = Key.newBuilder();
            for (Object part : prefix) {
                key.appendObject(part);
            }
            if (last != null) {
                key.appendObject(last);
            }
            return key.build();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            KeyConstraint that = (KeyConstraint) o;
            return completeKey == that.completeKey
                    && empty == that.empty
                    && prefix.equals(that.prefix)
                    && Objects.equals(lower, that.lower)
                    && Objects.equals(upper, that.upper);
        }

        @Override
        public int hashCode() {
            return Objects.hash(prefix, completeKey, lower, upper, empty);
        }
    }

    private static final class Bound implements Serializable {
        private static final long serialVersionUID = 1L;

        private final Object value;
        private final boolean closed;

        private Bound(Object value, boolean closed) {
            this.value = value;
            this.closed = closed;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Bound bound = (Bound) o;
            return closed == bound.closed && value.equals(bound.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value, closed);
        }
    }

    private enum Comparison {
        EQUALS,
        LESS_THAN,
        LESS_THAN_OR_EQUAL,
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL;

        @Nullable
        private static Comparison of(FunctionDefinition function) {
            if (function.equals(BuiltInFunctionDefinitions.EQUALS)) {
                return EQUALS;
            }
            if (function.equals(BuiltInFunctionDefinitions.LESS_THAN)) {
                return LESS_THAN;
            }
            if (function.equals(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL)) {
                return LESS_THAN_OR_EQUAL;
            }
            if (function.equals(BuiltInFunctionDefinitions.GREATER_THAN)) {
                return GREATER_THAN;
            }
            if (function.equals(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL)) {
                return GREATER_THAN_OR_EQUAL;
            }
            return null;
        }

        private Comparison reverse() {
            switch (this) {
                case LESS_THAN:
                    return GREATER_THAN;
                case LESS_THAN_OR_EQUAL:
                    return GREATER_THAN_OR_EQUAL;
                case GREATER_THAN:
                    return LESS_THAN;
                case GREATER_THAN_OR_EQUAL:
                    return LESS_THAN_OR_EQUAL;
                default:
                    return this;
            }
        }

        private boolean isLower() {
            return this == GREATER_THAN || this == GREATER_THAN_OR_EQUAL;
        }

        private boolean isUpper() {
            return this == LESS_THAN || this == LESS_THAN_OR_EQUAL;
        }
    }
}
