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

package io.github.flink.gcp.connector.bigquery.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Translates a conservative subset of Flink SQL predicates into BigQuery row restrictions. */
@Internal
final class BigQueryFilterPushDown {

    static final int MAX_ROW_RESTRICTION_BYTES = 1_000_000;
    private static final int BIGQUERY_TIMESTAMP_PRECISION = 6;
    private static final DateTimeFormatter DATETIME_LITERAL =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS");
    private static final DateTimeFormatter TIMESTAMP_LITERAL =
            DATETIME_LITERAL.withZone(ZoneOffset.UTC);

    private BigQueryFilterPushDown() {}

    static State translate(
            RowType physicalRowType,
            List<ResolvedExpression> filters,
            @Nullable String explicitRowRestriction) {
        List<ResolvedExpression> accepted = new ArrayList<>();
        List<String> restrictions = new ArrayList<>();
        long generatedRestrictionBytes = 0;
        long combinedWrapperBytes =
                explicitRowRestriction == null
                        ? 0
                        : combinedRowRestriction(explicitRowRestriction, "")
                                .getBytes(StandardCharsets.UTF_8)
                                .length;
        for (ResolvedExpression filter : filters) {
            Optional<String> translated = translate(physicalRowType, filter);
            if (translated.isPresent()) {
                long translatedBytes = translated.get().getBytes(StandardCharsets.UTF_8).length;
                long candidateRestrictionBytes =
                        restrictions.isEmpty()
                                ? translatedBytes + 2
                                : generatedRestrictionBytes + translatedBytes + " AND ".length();
                if (combinedWrapperBytes + candidateRestrictionBytes <= MAX_ROW_RESTRICTION_BYTES) {
                    accepted.add(filter);
                    restrictions.add(translated.get());
                    generatedRestrictionBytes = candidateRestrictionBytes;
                }
            }
        }
        @Nullable String restriction = restrictions.isEmpty() ? null : join("AND", restrictions);
        // A generated restriction must be a necessary condition for the Flink predicate: a remote
        // false negative cannot be recovered after the read. Flink evaluates every original
        // predicate again so a row that BigQuery admits but Flink rejects cannot reach the result.
        return new State(accepted, filters, restriction);
    }

    private static Optional<String> translate(
            RowType physicalRowType, ResolvedExpression expression) {
        if (!(expression instanceof CallExpression)) {
            return Optional.empty();
        }
        CallExpression call = (CallExpression) expression;
        FunctionDefinition function = call.getFunctionDefinition();
        List<ResolvedExpression> children = call.getResolvedChildren();
        if (function.equals(BuiltInFunctionDefinitions.AND)) {
            List<String> translated = new ArrayList<>();
            for (ResolvedExpression child : children) {
                translate(physicalRowType, child).ifPresent(translated::add);
            }
            return translated.isEmpty() ? Optional.empty() : Optional.of(join("AND", translated));
        }
        if (function.equals(BuiltInFunctionDefinitions.OR)) {
            List<String> translated = new ArrayList<>();
            for (ResolvedExpression child : children) {
                Optional<String> branch = translate(physicalRowType, child);
                if (!branch.isPresent()) {
                    return Optional.empty();
                }
                translated.add(branch.get());
            }
            return translated.isEmpty() ? Optional.empty() : Optional.of(join("OR", translated));
        }
        if (function.equals(BuiltInFunctionDefinitions.IS_NULL)
                || function.equals(BuiltInFunctionDefinitions.IS_NOT_NULL)) {
            if (children.size() != 1 || !(children.get(0) instanceof FieldReferenceExpression)) {
                return Optional.empty();
            }
            FieldReferenceExpression field = (FieldReferenceExpression) children.get(0);
            return field(physicalRowType, field)
                    .filter(value -> supportsNullCheck(value.type))
                    .map(
                            value ->
                                    "("
                                            + value.name
                                            + (function.equals(BuiltInFunctionDefinitions.IS_NULL)
                                                    ? " IS NULL"
                                                    : " IS NOT NULL")
                                            + ")");
        }

        Comparison comparison = Comparison.of(function);
        if (comparison == null || children.size() != 2) {
            return Optional.empty();
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
            return Optional.empty();
        }
        if (literal.isNull()) {
            return Optional.empty();
        }
        Optional<Field> resolved = field(physicalRowType, field);
        if (!resolved.isPresent()) {
            return Optional.empty();
        }
        return comparison(resolved.get(), literal, normalized);
    }

    private static Optional<Field> field(RowType physicalRowType, FieldReferenceExpression field) {
        int index = physicalRowType.getFieldNames().indexOf(field.getName());
        if (index < 0) {
            return Optional.empty();
        }
        return Optional.of(
                new Field(
                        quoteIdentifier(physicalRowType.getFieldNames().get(index)),
                        physicalRowType.getTypeAt(index)));
    }

    private static Optional<String> comparison(
            Field field, ValueLiteralExpression literal, Comparison comparison) {
        LogicalType type = field.type;
        switch (type.getTypeRoot()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DATE:
                return directLiteral(type, literal)
                        .map(value -> binary(field.name, comparison, value));
            case BOOLEAN:
                if (comparison != Comparison.EQUALS && comparison != Comparison.NOT_EQUALS) {
                    return Optional.empty();
                }
                return directLiteral(type, literal)
                        .map(value -> binary(field.name, comparison, value));
            case VARCHAR:
                if (comparison != Comparison.EQUALS) {
                    return Optional.empty();
                }
                return literal.getValueAs(String.class)
                        .flatMap(BigQueryFilterPushDown::stringLiteral)
                        .map(value -> binary(field.name, comparison, value));
            case DECIMAL:
                return decimalComparison(field, literal, comparison);
            case FLOAT:
                return floatComparison(field, literal, comparison);
            case DOUBLE:
                return doubleLiteral(literal)
                        .map(Object::toString)
                        .map(value -> binary(field.name, comparison, value));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                if (((TimestampType) type).getPrecision() != BIGQUERY_TIMESTAMP_PRECISION) {
                    return Optional.empty();
                }
                return literal.getValueAs(LocalDateTime.class)
                        .filter(BigQueryFilterPushDown::isBigQueryDatetime)
                        .map(value -> "DATETIME '" + DATETIME_LITERAL.format(value) + "'")
                        .map(value -> binary(field.name, comparison, value));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (((LocalZonedTimestampType) type).getPrecision()
                        != BIGQUERY_TIMESTAMP_PRECISION) {
                    return Optional.empty();
                }
                return literal.getValueAs(Instant.class)
                        .filter(BigQueryFilterPushDown::isBigQueryTimestamp)
                        .map(value -> "TIMESTAMP '" + TIMESTAMP_LITERAL.format(value) + "Z'")
                        .map(value -> binary(field.name, comparison, value));
            default:
                return Optional.empty();
        }
    }

    private static boolean supportsNullCheck(LogicalType type) {
        switch (type.getTypeRoot()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DATE:
            case BOOLEAN:
            case VARCHAR:
            case DECIMAL:
            case FLOAT:
            case DOUBLE:
                return true;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return ((TimestampType) type).getPrecision() == BIGQUERY_TIMESTAMP_PRECISION;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return ((LocalZonedTimestampType) type).getPrecision()
                        == BIGQUERY_TIMESTAMP_PRECISION;
            default:
                return false;
        }
    }

    private static Optional<String> directLiteral(
            LogicalType type, ValueLiteralExpression literal) {
        try {
            switch (type.getTypeRoot()) {
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                    return literal.getValueAs(BigDecimal.class)
                            .map(BigDecimal::toBigIntegerExact)
                            .map(Object::toString);
                case DATE:
                    return literal.getValueAs(LocalDate.class)
                            .filter(BigQueryFilterPushDown::isBigQueryDate)
                            .map(value -> "DATE '" + value + "'");
                case BOOLEAN:
                    return literal.getValueAs(Boolean.class).map(value -> value ? "TRUE" : "FALSE");
                default:
                    return Optional.empty();
            }
        } catch (ArithmeticException | IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    private static Optional<String> decimalComparison(
            Field field, ValueLiteralExpression literal, Comparison comparison) {
        DecimalType type = (DecimalType) field.type;
        Optional<BigDecimal> converted = literal.getValueAs(BigDecimal.class);
        if (!converted.isPresent()) {
            return Optional.empty();
        }
        BigDecimal value;
        try {
            value = converted.get().setScale(type.getScale());
        } catch (ArithmeticException e) {
            return Optional.empty();
        }
        BigDecimal unit = BigDecimal.ONE.scaleByPowerOfTen(-type.getScale());
        // The source rounds a BigQuery decimal to the declared Flink scale. These bounds are
        // intentionally wider than the exact rounding interval; the Flink residual removes the
        // extra rows.
        switch (comparison) {
            case EQUALS:
                List<String> bounds = new ArrayList<>();
                bounds.add(
                        binary(
                                field.name,
                                Comparison.GREATER_THAN_OR_EQUAL,
                                bignumeric(value.subtract(unit))));
                bounds.add(
                        binary(
                                field.name,
                                Comparison.LESS_THAN_OR_EQUAL,
                                bignumeric(value.add(unit))));
                return Optional.of(join("AND", bounds));
            case NOT_EQUALS:
                return Optional.of(binary(field.name, comparison, bignumeric(value)));
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                return Optional.of(
                        binary(field.name, Comparison.LESS_THAN, bignumeric(value.add(unit))));
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return Optional.of(
                        binary(
                                field.name,
                                Comparison.GREATER_THAN,
                                bignumeric(value.subtract(unit))));
            default:
                return Optional.empty();
        }
    }

    private static Optional<String> floatComparison(
            Field field, ValueLiteralExpression literal, Comparison comparison) {
        Optional<Float> converted = literal.getValueAs(Float.class);
        if (!converted.isPresent() || !Float.isFinite(converted.get())) {
            return Optional.empty();
        }
        float value = converted.get();
        double previous = Math.nextDown(value);
        double next = Math.nextUp(value);
        // BigQuery stores FLOAT64 while the source narrows this column to a Java float. Adjacent
        // float values form conservative bounds around every raw double that can narrow to value.
        switch (comparison) {
            case EQUALS:
                List<String> bounds = new ArrayList<>();
                if (Double.isFinite(previous)) {
                    bounds.add(
                            binary(field.name, Comparison.GREATER_THAN, Double.toString(previous)));
                }
                if (Double.isFinite(next)) {
                    bounds.add(binary(field.name, Comparison.LESS_THAN, Double.toString(next)));
                }
                return bounds.isEmpty() ? Optional.empty() : Optional.of(join("AND", bounds));
            case NOT_EQUALS:
                return Optional.of(binary(field.name, comparison, Double.toString((double) value)));
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                return Double.isFinite(next)
                        ? Optional.of(
                                binary(field.name, Comparison.LESS_THAN, Double.toString(next)))
                        : Optional.empty();
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return Double.isFinite(previous)
                        ? Optional.of(
                                binary(
                                        field.name,
                                        Comparison.GREATER_THAN,
                                        Double.toString(previous)))
                        : Optional.empty();
            default:
                return Optional.empty();
        }
    }

    private static Optional<Double> doubleLiteral(ValueLiteralExpression literal) {
        Optional<Double> value = literal.getValueAs(Double.class);
        if (!value.isPresent()) {
            value = literal.getValueAs(BigDecimal.class).map(BigDecimal::doubleValue);
        }
        return value.filter(Double::isFinite);
    }

    private static String binary(String field, Comparison comparison, String literal) {
        return "(" + field + " " + comparison.sql + " " + literal + ")";
    }

    private static String bignumeric(BigDecimal value) {
        return "BIGNUMERIC '" + value.toPlainString() + "'";
    }

    private static Optional<String> stringLiteral(String value) {
        StringBuilder escaped = new StringBuilder(value.length() + 2).append('\'');
        for (int i = 0; i < value.length(); i++) {
            char character = value.charAt(i);
            if (Character.isHighSurrogate(character)) {
                if (i + 1 >= value.length() || !Character.isLowSurrogate(value.charAt(i + 1))) {
                    return Optional.empty();
                }
                escaped.append(character).append(value.charAt(++i));
                continue;
            }
            if (Character.isLowSurrogate(character)) {
                return Optional.empty();
            }
            switch (character) {
                case '\'':
                    escaped.append("\\'");
                    break;
                case '\\':
                    escaped.append("\\\\");
                    break;
                case '\b':
                    escaped.append("\\b");
                    break;
                case '\f':
                    escaped.append("\\f");
                    break;
                case '\n':
                    escaped.append("\\n");
                    break;
                case '\r':
                    escaped.append("\\r");
                    break;
                case '\t':
                    escaped.append("\\t");
                    break;
                default:
                    if (character < 0x20 || character == 0x7f) {
                        appendUnicodeEscape(escaped, character);
                    } else {
                        escaped.append(character);
                    }
            }
        }
        return Optional.of(escaped.append('\'').toString());
    }

    private static void appendUnicodeEscape(StringBuilder escaped, char character) {
        String hex = Integer.toHexString(character);
        escaped.append("\\u");
        for (int i = hex.length(); i < 4; i++) {
            escaped.append('0');
        }
        escaped.append(hex);
    }

    private static boolean isBigQueryDatetime(LocalDateTime value) {
        return value.getYear() >= 1 && value.getYear() <= 9999 && value.getNano() % 1_000 == 0;
    }

    private static boolean isBigQueryDate(LocalDate value) {
        return value.getYear() >= 1 && value.getYear() <= 9999;
    }

    private static boolean isBigQueryTimestamp(Instant value) {
        try {
            LocalDateTime utc = LocalDateTime.ofInstant(value, ZoneOffset.UTC);
            return utc.getYear() >= 1 && utc.getYear() <= 9999 && value.getNano() % 1_000 == 0;
        } catch (DateTimeException e) {
            return false;
        }
    }

    private static String join(String operator, List<String> expressions) {
        return "(" + String.join(" " + operator + " ", expressions) + ")";
    }

    private static String quoteIdentifier(String identifier) {
        return "`" + identifier.replace("\\", "\\\\").replace("`", "\\`") + "`";
    }

    static String combinedRowRestriction(
            @Nullable String explicitRowRestriction, String generatedRowRestriction) {
        if (explicitRowRestriction == null) {
            return generatedRowRestriction;
        }
        return "(\n" + explicitRowRestriction + "\n)\nAND\n(\n" + generatedRowRestriction + "\n)";
    }

    static final class State {
        private static final State EMPTY =
                new State(Collections.emptyList(), Collections.emptyList(), null);

        private final List<ResolvedExpression> accepted;
        private final List<ResolvedExpression> remaining;
        @Nullable private final String rowRestriction;

        private State(
                List<ResolvedExpression> accepted,
                List<ResolvedExpression> remaining,
                @Nullable String rowRestriction) {
            this.accepted = Collections.unmodifiableList(new ArrayList<>(accepted));
            this.remaining = Collections.unmodifiableList(new ArrayList<>(remaining));
            this.rowRestriction = rowRestriction;
        }

        static State empty() {
            return EMPTY;
        }

        SupportsFilterPushDown.Result result() {
            return SupportsFilterPushDown.Result.of(accepted, remaining);
        }

        @Nullable
        String rowRestriction() {
            return rowRestriction;
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
            return accepted.equals(state.accepted)
                    && remaining.equals(state.remaining)
                    && Objects.equals(rowRestriction, state.rowRestriction);
        }

        @Override
        public int hashCode() {
            return Objects.hash(accepted, remaining, rowRestriction);
        }
    }

    private static final class Field {
        private final String name;
        private final LogicalType type;

        private Field(String name, LogicalType type) {
            this.name = name;
            this.type = type;
        }
    }

    private enum Comparison {
        EQUALS("="),
        NOT_EQUALS("<>"),
        LESS_THAN("<"),
        LESS_THAN_OR_EQUAL("<="),
        GREATER_THAN(">"),
        GREATER_THAN_OR_EQUAL(">=");

        private final String sql;

        Comparison(String sql) {
            this.sql = sql;
        }

        @Nullable
        private static Comparison of(FunctionDefinition function) {
            if (function.equals(BuiltInFunctionDefinitions.EQUALS)) {
                return EQUALS;
            }
            if (function.equals(BuiltInFunctionDefinitions.NOT_EQUALS)) {
                return NOT_EQUALS;
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
    }
}
