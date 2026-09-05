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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Covers scalar literal rendering and the declared precision boundary. */
class BigQueryScalarFilterPushDownTest {

    private static final BuiltInFunctionDefinition[] OPERATORS = {
        BuiltInFunctionDefinitions.EQUALS,
        BuiltInFunctionDefinitions.NOT_EQUALS,
        BuiltInFunctionDefinitions.LESS_THAN,
        BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
        BuiltInFunctionDefinitions.GREATER_THAN,
        BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL
    };
    private static final String[] SQL = {"=", "<>", "<", "<=", ">", ">="};
    private static final int[] REVERSED = {0, 1, 4, 5, 2, 3};
    private static final DateTimeFormatter CLOCK = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS");
    private static final DateTimeFormatter CIVIL =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS");

    @Test
    void bytesComparisonsPreserveEveryByteAndNormalizeEveryOperator() {
        List<byte[]> values =
                Arrays.asList(
                        new byte[0],
                        new byte[] {0},
                        new byte[] {0, 0},
                        new byte[] {'\'', '"', '\\'},
                        new byte[] {0x7f, (byte) 0x80, (byte) 0xff, (byte) 0xc3, 0x28});
        String[] literals = {
            "b''", "b'\\x00'", "b'\\x00\\x00'", "b'\\x27\\x22\\x5c'", "b'\\x7f\\x80\\xff\\xc3\\x28'"
        };
        for (DataType fieldType :
                Arrays.asList(
                        DataTypes.BYTES(),
                        DataTypes.BINARY(1),
                        DataTypes.BINARY(2),
                        DataTypes.BINARY(4))) {
            for (int v = 0; v < values.size(); v++) {
                byte[] value = values.get(v);
                List<DataType> literalTypes =
                        value.length == 0
                                ? Collections.singletonList(DataTypes.BYTES())
                                : Arrays.asList(
                                        DataTypes.BYTES(),
                                        DataTypes.BINARY(value.length),
                                        DataTypes.VARBINARY(value.length));
                for (DataType literalType : literalTypes) {
                    for (int op = 0; op < OPERATORS.length; op++) {
                        assertTranslation(
                                fieldType,
                                value,
                                literalType,
                                op,
                                false,
                                "((`value` " + SQL[op] + " " + literals[v] + "))");
                        assertTranslation(
                                fieldType,
                                value,
                                literalType,
                                op,
                                true,
                                "((`value` " + SQL[REVERSED[op]] + " " + literals[v] + "))");
                    }
                }
            }
            assertNullChecks(fieldType);
        }
    }

    @Test
    void remainingBinaryCastsComposeConservativelyWithDirectFilters() {
        DataType type = DataTypes.BINARY(2);
        ResolvedExpression literal = new ValueLiteralExpression(new byte[] {0});
        ResolvedExpression cast =
                CallExpression.permanent(
                        BuiltInFunctionDefinitions.CAST,
                        Arrays.asList(field(type), new TypeLiteralExpression(DataTypes.BINARY(1))),
                        DataTypes.BINARY(1));
        ResolvedExpression unsupported = call(BuiltInFunctionDefinitions.EQUALS, cast, literal);
        ResolvedExpression supported =
                comparison(type, new byte[] {(byte) 0x80}, DataTypes.BINARY(1), 2, false);
        BigQueryFilterPushDown.State castOnly = translate(type, unsupported, null);
        assertThat(castOnly.result().getAcceptedFilters()).isEmpty();
        assertThat(castOnly.result().getRemainingFilters()).containsExactly(unsupported);
        assertThat(castOnly.rowRestriction()).isNull();
        ResolvedExpression conjunction =
                call(BuiltInFunctionDefinitions.AND, unsupported, supported);
        BigQueryFilterPushDown.State and = translate(type, conjunction, null);
        assertThat(and.rowRestriction()).isEqualTo("(((`value` < b'\\x80')))");
        assertThat(and.result().getAcceptedFilters()).containsExactly(conjunction);
        assertThat(and.result().getRemainingFilters()).containsExactly(conjunction);
        ResolvedExpression disjunction =
                call(BuiltInFunctionDefinitions.OR, unsupported, supported);
        BigQueryFilterPushDown.State or = translate(type, disjunction, null);
        assertThat(or.rowRestriction()).isNull();
        assertThat(or.result().getAcceptedFilters()).isEmpty();
        assertThat(or.result().getRemainingFilters()).containsExactly(disjunction);
        ResolvedExpression castLiteral =
                CallExpression.permanent(
                        BuiltInFunctionDefinitions.CAST,
                        Arrays.asList(literal, new TypeLiteralExpression(type)),
                        type);
        assertThat(
                        translate(
                                        type,
                                        call(
                                                BuiltInFunctionDefinitions.EQUALS,
                                                field(type),
                                                castLiteral),
                                        null)
                                .rowRestriction())
                .isNull();
        assertThat(
                        translate(type, call(BuiltInFunctionDefinitions.IS_NULL, cast), null)
                                .rowRestriction())
                .isNull();
    }

    @Test
    void everyTemporalPrecisionUsesTheWholeTruncationBucket() {
        for (int p = 0; p <= 6; p++) {
            int unit = (int) Math.pow(10, 9 - p);
            int nanos = 123_456_000 / unit * unit;
            int lastNanos = nanos + unit - 1_000;
            for (LocalDateTime second :
                    Arrays.asList(
                            LocalDateTime.of(1, 1, 1, 0, 0),
                            LocalDateTime.of(1969, 12, 31, 23, 59, 59),
                            LocalDateTime.of(2026, 9, 5, 12, 34, 56),
                            LocalDateTime.of(9999, 12, 31, 23, 59, 59))) {
                LocalDateTime lower = second.withNano(nanos);
                LocalDateTime upper = second.withNano(lastNanos);
                assertTemporalMatrix(
                        DataTypes.TIMESTAMP(p),
                        lower,
                        "DATETIME '" + CIVIL.format(lower) + "'",
                        "DATETIME '" + CIVIL.format(upper) + "'");
                assertTemporalMatrix(
                        DataTypes.TIMESTAMP_LTZ(p),
                        lower.toInstant(ZoneOffset.UTC),
                        "TIMESTAMP '" + CIVIL.format(lower) + "Z'",
                        "TIMESTAMP '" + CIVIL.format(upper) + "Z'");
                if (p <= 3) {
                    assertTemporalMatrix(
                            DataTypes.TIME(p),
                            lower.toLocalTime(),
                            "TIME '" + CLOCK.format(lower) + "'",
                            "TIME '" + CLOCK.format(upper) + "'");
                }
            }
            assertNullChecks(DataTypes.TIMESTAMP(p));
            assertNullChecks(DataTypes.TIMESTAMP_LTZ(p));
            if (p <= 3) {
                assertNullChecks(DataTypes.TIME(p));
            }
        }
    }

    @Test
    void theFinalBucketDoesNotWrapAtMidnightOrTheMaximumYear() {
        for (int p = 0; p <= 5; p++) {
            int unit = (int) Math.pow(10, 9 - p);
            LocalDateTime lower = LocalDateTime.of(9999, 12, 31, 23, 59, 59, 1_000_000_000 - unit);
            assertTemporalMatrix(
                    DataTypes.TIMESTAMP(p),
                    lower,
                    "DATETIME '" + CIVIL.format(lower) + "'",
                    "DATETIME '9999-12-31 23:59:59.999999'");
            assertTemporalMatrix(
                    DataTypes.TIMESTAMP_LTZ(p),
                    lower.toInstant(ZoneOffset.UTC),
                    "TIMESTAMP '" + CIVIL.format(lower) + "Z'",
                    "TIMESTAMP '9999-12-31 23:59:59.999999Z'");
            if (p <= 3) {
                assertTemporalMatrix(
                        DataTypes.TIME(p),
                        lower.toLocalTime(),
                        "TIME '" + CLOCK.format(lower) + "'",
                        "TIME '23:59:59.999999'");
            }
        }
    }

    @Test
    void unrepresentableOrUnalignedLiteralsAndUnsupportedPrecisionsStayResidual() {
        for (int p = 0; p <= 6; p++) {
            if (p < 6) {
                assertRejected(
                        DataTypes.TIMESTAMP(p),
                        LocalDateTime.of(2026, 9, 5, 0, 0, 0, 1_000),
                        DataTypes.TIMESTAMP(6));
                assertRejected(
                        DataTypes.TIMESTAMP_LTZ(p),
                        Instant.ofEpochSecond(-1, 1_000),
                        DataTypes.TIMESTAMP_LTZ(6));
            }
            assertRejected(
                    DataTypes.TIMESTAMP(p),
                    LocalDateTime.of(2026, 9, 5, 0, 0, 0, 1),
                    DataTypes.TIMESTAMP(9));
            assertRejected(
                    DataTypes.TIMESTAMP_LTZ(p),
                    Instant.ofEpochSecond(-1, 1),
                    DataTypes.TIMESTAMP_LTZ(9));
            assertRejected(
                    DataTypes.TIMESTAMP(p),
                    LocalDateTime.of(0, 1, 1, 0, 0),
                    DataTypes.TIMESTAMP(p));
            assertRejected(DataTypes.TIMESTAMP_LTZ(p), Instant.MAX, DataTypes.TIMESTAMP_LTZ(p));
            if (p <= 3) {
                assertRejected(DataTypes.TIME(p), LocalTime.of(1, 2, 3, 1_000), DataTypes.TIME(6));
            }
        }
        for (DataType type :
                Arrays.asList(
                        DataTypes.TIME(4), DataTypes.TIMESTAMP(7), DataTypes.TIMESTAMP_LTZ(7))) {
            assertThat(
                            translate(
                                            type,
                                            call(BuiltInFunctionDefinitions.IS_NULL, field(type)),
                                            null)
                                    .rowRestriction())
                    .isNull();
        }
        assertRejected(DataTypes.TIME(4), LocalTime.NOON, DataTypes.TIME(4));
        assertRejected(
                DataTypes.TIMESTAMP(7), LocalDateTime.of(2026, 1, 1, 0, 0), DataTypes.TIMESTAMP(7));
        assertRejected(DataTypes.TIMESTAMP_LTZ(7), Instant.EPOCH, DataTypes.TIMESTAMP_LTZ(7));
        for (DataType type :
                Arrays.asList(
                        DataTypes.BYTES(),
                        DataTypes.BINARY(4),
                        DataTypes.TIME(3),
                        DataTypes.TIMESTAMP(3),
                        DataTypes.TIMESTAMP_LTZ(3))) {
            assertRejected(type, "not a typed scalar", DataTypes.STRING());
            assertRejected(type, null, type);
        }
    }

    @Test
    void hexEscapingConsumesTheCombinedUtf8Budget() {
        for (DataType type : Arrays.asList(DataTypes.BYTES(), DataTypes.BINARY(2))) {
            ResolvedExpression small = comparison(type, new byte[] {0}, type, 0, false);
            String rendered = translate(type, small, null).rowRestriction();
            int wrapper = BigQueryFilterPushDown.combinedRowRestriction("", "").length();
            int available =
                    BigQueryFilterPushDown.MAX_ROW_RESTRICTION_BYTES - wrapper - rendered.length();
            String explicit = "é".repeat(available / 2) + " ".repeat(available % 2);
            assertThat(translate(type, small, explicit).rowRestriction()).isEqualTo(rendered);
            assertThat(translate(type, small, explicit + " ").rowRestriction()).isNull();
            ResolvedExpression large = comparison(type, new byte[250_000], type, 0, false);
            BigQueryFilterPushDown.State state =
                    BigQueryFilterPushDown.translate(
                            rowType(type), Arrays.asList(large, small), null);
            assertThat(state.result().getAcceptedFilters()).containsExactly(small);
            assertThat(state.result().getRemainingFilters()).containsExactly(large, small);
            assertThat(state.rowRestriction()).isEqualTo(rendered);
        }
    }

    private static void assertTemporalMatrix(
            DataType type, Object value, String lower, String upper) {
        for (int op = 0; op < OPERATORS.length; op++) {
            for (boolean reverse : new boolean[] {false, true}) {
                int normalized = reverse ? REVERSED[op] : op;
                String expected;
                if (normalized == 0 && !lower.equals(upper)) {
                    expected = "(((`value` >= " + lower + ") AND (`value` <= " + upper + ")))";
                } else {
                    expected =
                            "((`value` "
                                    + SQL[normalized]
                                    + " "
                                    + (normalized == 3 || normalized == 4 ? upper : lower)
                                    + "))";
                }
                assertTranslation(type, value, type, op, reverse, expected);
            }
        }
    }

    private static void assertNullChecks(DataType type) {
        for (BuiltInFunctionDefinition function :
                Arrays.asList(
                        BuiltInFunctionDefinitions.IS_NULL,
                        BuiltInFunctionDefinitions.IS_NOT_NULL)) {
            ResolvedExpression filter = call(function, field(type));
            BigQueryFilterPushDown.State state = translate(type, filter, null);
            assertThat(state.rowRestriction())
                    .isEqualTo(
                            function == BuiltInFunctionDefinitions.IS_NULL
                                    ? "((`value` IS NULL))"
                                    : "((`value` IS NOT NULL))");
            assertThat(state.result().getAcceptedFilters()).containsExactly(filter);
            assertThat(state.result().getRemainingFilters()).containsExactly(filter);
        }
    }

    private static void assertTranslation(
            DataType type,
            Object value,
            DataType literalType,
            int op,
            boolean reverse,
            String expected) {
        ResolvedExpression filter = comparison(type, value, literalType, op, reverse);
        BigQueryFilterPushDown.State state = translate(type, filter, null);
        assertThat(state.rowRestriction())
                .as("%s %s %s, reversed=%s", type, SQL[op], value, reverse)
                .isEqualTo(expected);
        assertThat(state.result().getAcceptedFilters()).containsExactly(filter);
        assertThat(state.result().getRemainingFilters()).containsExactly(filter);
    }

    private static void assertRejected(DataType type, Object value, DataType literalType) {
        for (int op = 0; op < OPERATORS.length; op++) {
            ResolvedExpression filter = comparison(type, value, literalType, op, false);
            BigQueryFilterPushDown.State state = translate(type, filter, null);
            assertThat(state.rowRestriction()).as("%s %s %s", type, SQL[op], value).isNull();
            assertThat(state.result().getAcceptedFilters()).isEmpty();
            assertThat(state.result().getRemainingFilters()).containsExactly(filter);
        }
    }

    private static ResolvedExpression comparison(
            DataType type, Object value, DataType literalType, int op, boolean reverse) {
        ValueLiteralExpression literal =
                new ValueLiteralExpression(
                        value, value == null ? literalType : literalType.notNull());
        return reverse
                ? call(OPERATORS[op], literal, field(type))
                : call(OPERATORS[op], field(type), literal);
    }

    private static FieldReferenceExpression field(DataType type) {
        return new FieldReferenceExpression("value", type, 0, 0);
    }

    private static RowType rowType(DataType type) {
        return (RowType) DataTypes.ROW(DataTypes.FIELD("value", type)).getLogicalType();
    }

    private static BigQueryFilterPushDown.State translate(
            DataType type, ResolvedExpression filter, String explicit) {
        return BigQueryFilterPushDown.translate(
                rowType(type), Collections.singletonList(filter), explicit);
    }

    private static CallExpression call(
            BuiltInFunctionDefinition function, ResolvedExpression... children) {
        return CallExpression.permanent(function, Arrays.asList(children), DataTypes.BOOLEAN());
    }
}
