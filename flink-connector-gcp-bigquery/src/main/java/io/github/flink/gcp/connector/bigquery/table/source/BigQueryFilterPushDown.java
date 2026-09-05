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
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/** Translates a conservative subset of Flink SQL predicates into BigQuery row restrictions. */
@Internal
final class BigQueryFilterPushDown {

    static final int MAX_ROW_RESTRICTION_BYTES = 1_000_000;
    private static final int BIGQUERY_TIMESTAMP_PRECISION = 6;
    private static final DateTimeFormatter TIME_LITERAL =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS");
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
        List<Sql> restrictions = new ArrayList<>();
        long generatedRestrictionBytes = 2;
        long combinedWrapperBytes =
                explicitRowRestriction == null
                        ? 0
                        : utf8Bytes(explicitRowRestriction, false)
                                + combinedRowRestriction("", "").length();
        for (ResolvedExpression filter : filters) {
            int separatorBytes = restrictions.isEmpty() ? 0 : " AND ".length();
            long remaining =
                    MAX_ROW_RESTRICTION_BYTES
                            - combinedWrapperBytes
                            - generatedRestrictionBytes
                            - separatorBytes;
            Optional<Sql> translated = translate(physicalRowType, filter, remaining);
            if (translated.isPresent()) {
                accepted.add(filter);
                restrictions.add(translated.get());
                generatedRestrictionBytes += separatorBytes + translated.get().bytes;
            }
        }
        @Nullable
        String restriction = restrictions.isEmpty() ? null : join("AND", restrictions).render();
        // A generated restriction must be a necessary condition for the Flink predicate: a remote
        // false negative cannot be recovered after the read. Flink evaluates every original
        // predicate again so a row that BigQuery admits but Flink rejects cannot reach the result.
        return new State(accepted, filters, restriction);
    }

    private static Optional<Sql> translate(
            RowType physicalRowType, ResolvedExpression expression, long remaining) {
        if (remaining <= 0 || !(expression instanceof CallExpression)) {
            return Optional.empty();
        }
        CallExpression call = (CallExpression) expression;
        FunctionDefinition function = call.getFunctionDefinition();
        List<ResolvedExpression> children = call.getResolvedChildren();
        boolean conjunction = function.equals(BuiltInFunctionDefinitions.AND);
        if (conjunction || function.equals(BuiltInFunctionDefinitions.OR)) {
            String operator = conjunction ? "AND" : "OR";
            List<Sql> translated = new ArrayList<>();
            long used = 2;
            for (ResolvedExpression child : children) {
                int separatorBytes = translated.isEmpty() ? 0 : operator.length() + 2;
                Optional<Sql> branch =
                        translate(physicalRowType, child, remaining - used - separatorBytes);
                if (!branch.isPresent()) {
                    if (!conjunction) {
                        return Optional.empty();
                    }
                    continue;
                }
                translated.add(branch.get());
                used += separatorBytes + branch.get().bytes;
            }
            return translated.isEmpty()
                    ? Optional.empty()
                    : Optional.of(join(operator, translated));
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
                                    concat(
                                            text("("),
                                            value.name,
                                            text(
                                                    function.equals(
                                                                    BuiltInFunctionDefinitions
                                                                            .IS_NULL)
                                                            ? " IS NULL)"
                                                            : " IS NOT NULL)")))
                    .filter(value -> value.bytes <= remaining);
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
        return comparison(resolved.get(), literal, normalized, remaining)
                .filter(value -> value.bytes <= remaining);
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

    private static Optional<Sql> comparison(
            Field field, ValueLiteralExpression literal, Comparison comparison, long remaining) {
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
                        .flatMap(value -> stringLiteral(value, remaining))
                        .map(value -> binary(field.name, comparison, value));
            case DECIMAL:
                return decimalComparison(field, literal, comparison);
            case BINARY:
            case VARBINARY:
                // Only direct field/literal expressions reach here. A remaining cast may change
                // the bytes and is rejected by translate; the converter itself preserves length.
                return literal.getValueAs(byte[].class)
                        .flatMap(value -> bytesLiteral(value, remaining))
                        .map(value -> binary(field.name, comparison, value));
            case FLOAT:
                return floatComparison(field, literal, comparison);
            case DOUBLE:
                return doubleLiteral(literal)
                        .map(Object::toString)
                        .map(value -> binary(field.name, comparison, value));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                int datetimePrecision = ((TimestampType) type).getPrecision();
                if (datetimePrecision > BIGQUERY_TIMESTAMP_PRECISION) {
                    return Optional.empty();
                }
                return literal.getValueAs(LocalDateTime.class)
                        .filter(BigQueryFilterPushDown::isBigQueryDatetime)
                        .filter(value -> isPrecisionAligned(value.getNano(), datetimePrecision))
                        .map(
                                value ->
                                        temporalComparison(
                                                field.name,
                                                comparison,
                                                datetimeLiteral(value),
                                                datetimeLiteral(
                                                        value.plusNanos(
                                                                bucketTailNanos(
                                                                        datetimePrecision)))));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                int timestampPrecision = ((LocalZonedTimestampType) type).getPrecision();
                if (timestampPrecision > BIGQUERY_TIMESTAMP_PRECISION) {
                    return Optional.empty();
                }
                return literal.getValueAs(Instant.class)
                        .filter(BigQueryFilterPushDown::isBigQueryTimestamp)
                        .filter(value -> isPrecisionAligned(value.getNano(), timestampPrecision))
                        .map(
                                value ->
                                        temporalComparison(
                                                field.name,
                                                comparison,
                                                timestampLiteral(value),
                                                timestampLiteral(
                                                        value.plusNanos(
                                                                bucketTailNanos(
                                                                        timestampPrecision)))));
            case TIME_WITHOUT_TIME_ZONE:
                int timePrecision = ((TimeType) type).getPrecision();
                if (timePrecision > GenericRecordToRowDataConverter.MAX_TIME_PRECISION) {
                    return Optional.empty();
                }
                return literal.getValueAs(LocalTime.class)
                        .filter(value -> isPrecisionAligned(value.getNano(), timePrecision))
                        .map(
                                value ->
                                        temporalComparison(
                                                field.name,
                                                comparison,
                                                timeLiteral(value),
                                                timeLiteral(
                                                        value.plusNanos(
                                                                bucketTailNanos(timePrecision)))));
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
            case BINARY:
            case VARBINARY:
                return true;
            case TIME_WITHOUT_TIME_ZONE:
                return ((TimeType) type).getPrecision()
                        <= GenericRecordToRowDataConverter.MAX_TIME_PRECISION;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return ((TimestampType) type).getPrecision() <= BIGQUERY_TIMESTAMP_PRECISION;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return ((LocalZonedTimestampType) type).getPrecision()
                        <= BIGQUERY_TIMESTAMP_PRECISION;
            default:
                return false;
        }
    }

    private static Optional<Sql> bytesLiteral(byte[] value, long remaining) {
        long size = 4L * value.length + 3;
        if (size > remaining) {
            return Optional.empty();
        }
        return Optional.of(
                new Sql(
                        size,
                        escaped -> {
                            escaped.append("b'");
                            for (byte element : value) {
                                escaped.append("\\x")
                                        .append(Character.forDigit((element & 0xff) >>> 4, 16))
                                        .append(Character.forDigit(element & 0xf, 16));
                            }
                            escaped.append('\'');
                        }));
    }

    private static long bucketTailNanos(int precision) {
        long unit = 1;
        for (int i = precision; i < BIGQUERY_TIMESTAMP_PRECISION; i++) {
            unit *= 10;
        }
        return (unit - 1) * 1_000;
    }

    private static boolean isPrecisionAligned(int nanos, int precision) {
        return nanos % (bucketTailNanos(precision) + 1_000) == 0;
    }

    private static String timeLiteral(LocalTime value) {
        return "TIME '" + TIME_LITERAL.format(value) + "'";
    }

    private static String datetimeLiteral(LocalDateTime value) {
        return "DATETIME '" + DATETIME_LITERAL.format(value) + "'";
    }

    private static String timestampLiteral(Instant value) {
        return "TIMESTAMP '" + TIMESTAMP_LITERAL.format(value) + "Z'";
    }

    private static Sql temporalComparison(
            Sql field, Comparison comparison, String lower, String upper) {
        // The converter truncates fractional seconds. Every source microsecond from lower to
        // upper therefore becomes the same Flink value. An inclusive upper bound stays within
        // that second, including at the end of the BigQuery date range or the TIME day.
        if (lower.equals(upper)) {
            return binary(field, comparison, lower);
        }
        switch (comparison) {
            case EQUALS:
                List<Sql> bounds = new ArrayList<>();
                bounds.add(binary(field, Comparison.GREATER_THAN_OR_EQUAL, lower));
                bounds.add(binary(field, Comparison.LESS_THAN_OR_EQUAL, upper));
                return join("AND", bounds);
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
                return binary(field, comparison, upper);
            default:
                // In particular, <> lower is necessary but may retain other values in the
                // same truncation bucket; the Flink residual removes those extra rows.
                return binary(field, comparison, lower);
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

    private static Optional<Sql> decimalComparison(
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
                List<Sql> bounds = new ArrayList<>();
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

    private static Optional<Sql> floatComparison(
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
                List<Sql> bounds = new ArrayList<>();
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

    private static Sql binary(Sql field, Comparison comparison, String literal) {
        return binary(field, comparison, text(literal));
    }

    private static Sql binary(Sql field, Comparison comparison, Sql literal) {
        return concat(text("("), field, text(" " + comparison.sql + " "), literal, text(")"));
    }

    private static String bignumeric(BigDecimal value) {
        return "BIGNUMERIC '" + value.toPlainString() + "'";
    }

    private static Optional<Sql> stringLiteral(String value, long remaining) {
        long size = stringLiteralBytes(value, remaining);
        if (size > remaining) {
            return Optional.empty();
        }
        return Optional.of(new Sql(size, escaped -> appendStringLiteral(escaped, value)));
    }

    private static void appendStringLiteral(StringBuilder escaped, String value) {
        escaped.append('\'');
        for (int i = 0; i < value.length(); i++) {
            char character = value.charAt(i);
            if (Character.isHighSurrogate(character)) {
                escaped.append(character).append(value.charAt(++i));
                continue;
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
        escaped.append('\'');
    }

    private static long stringLiteralBytes(String value, long remaining) {
        if (value.length() > remaining - 2) {
            return remaining + 1;
        }
        long bytes = 2;
        for (int i = 0; i < value.length(); i++) {
            char character = value.charAt(i);
            if (Character.isHighSurrogate(character)) {
                if (i + 1 >= value.length() || !Character.isLowSurrogate(value.charAt(++i))) {
                    return remaining + 1;
                }
                bytes += 4;
            } else if (Character.isLowSurrogate(character)) {
                return remaining + 1;
            } else if (character == '\''
                    || character == '\\'
                    || character == '\b'
                    || character == '\f'
                    || character == '\n'
                    || character == '\r'
                    || character == '\t') {
                bytes += 2;
            } else if (character < 0x20 || character == 0x7f) {
                bytes += 6;
            } else if (character < 0x80) {
                bytes++;
            } else if (character < 0x800) {
                bytes += 2;
            } else {
                bytes += 3;
            }
            if (bytes > remaining) {
                return remaining + 1;
            }
        }
        return bytes;
    }

    private static void appendUnicodeEscape(StringBuilder escaped, char character) {
        escaped.append("\\u");
        for (int shift = 12; shift >= 0; shift -= 4) {
            escaped.append(Character.forDigit((character >>> shift) & 0xf, 16));
        }
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

    private static Sql join(String operator, List<Sql> expressions) {
        String separator = " " + operator + " ";
        long bytes = 2L + (long) separator.length() * (expressions.size() - 1);
        for (Sql expression : expressions) {
            bytes += expression.bytes;
        }
        return new Sql(
                bytes,
                output -> {
                    output.append('(');
                    for (int i = 0; i < expressions.size(); i++) {
                        if (i > 0) {
                            output.append(separator);
                        }
                        expressions.get(i).appendTo(output);
                    }
                    output.append(')');
                });
    }

    private static Sql quoteIdentifier(String identifier) {
        return new Sql(
                2 + utf8Bytes(identifier, true),
                output -> {
                    output.append('`');
                    for (int i = 0; i < identifier.length(); i++) {
                        char character = identifier.charAt(i);
                        if (character == '\\' || character == '`') {
                            output.append('\\');
                        }
                        output.append(character);
                    }
                    output.append('`');
                });
    }

    // Count the UTF-8 representation without allocating a byte array. Like String.getBytes,
    // unpaired surrogates in raw SQL/identifiers encode as one replacement byte; string literals
    // instead reject them in stringLiteralBytes. An over-limit count need not be exact.
    private static long utf8Bytes(String value, boolean identifier) {
        long bytes = 0;
        for (int i = 0; i < value.length(); i++) {
            char character = value.charAt(i);
            if (identifier && (character == '\\' || character == '`')) {
                bytes += 2;
            } else if (character < 0x80) {
                bytes++;
            } else if (character < 0x800) {
                bytes += 2;
            } else if (Character.isHighSurrogate(character)
                    && i + 1 < value.length()
                    && Character.isLowSurrogate(value.charAt(i + 1))) {
                bytes += 4;
                i++;
            } else if (Character.isSurrogate(character)) {
                bytes++;
            } else {
                bytes += 3;
            }
            if (bytes > MAX_ROW_RESTRICTION_BYTES) {
                return bytes;
            }
        }
        return bytes;
    }

    private static Sql text(String value) {
        return new Sql(utf8Bytes(value, false), output -> output.append(value));
    }

    private static Sql concat(Sql... parts) {
        long bytes = 0;
        for (Sql part : parts) {
            bytes += part.bytes;
        }
        return new Sql(
                bytes,
                output -> {
                    for (Sql part : parts) {
                        part.appendTo(output);
                    }
                });
    }

    /** A measured SQL fragment whose potentially large text is emitted only after selection. */
    private static final class Sql {
        private final long bytes;
        private final Consumer<StringBuilder> writer;

        private Sql(long bytes, Consumer<StringBuilder> writer) {
            this.bytes = bytes;
            this.writer = writer;
        }

        private void appendTo(StringBuilder output) {
            writer.accept(output);
        }

        private String render() {
            StringBuilder output = new StringBuilder((int) bytes);
            appendTo(output);
            return output.toString();
        }
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
        private final Sql name;
        private final LogicalType type;

        private Field(Sql name, LogicalType type) {
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
