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

package io.github.flink.gcp.connector.bigquery.table.sink;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RowDataJsonRenderer}, which renders a marked {@code ROW} as JSON text. */
class RowDataJsonRendererTest {

    private static String render(DataType type, Object value) {
        return new RowDataJsonRenderer(type.getLogicalType(), "doc").render(value);
    }

    @Test
    void rendersAnObjectFromARow() {
        assertThat(
                        render(
                                DataTypes.ROW(
                                        DataTypes.FIELD("a", DataTypes.INT()),
                                        DataTypes.FIELD("b", DataTypes.STRING())),
                                GenericRowData.of(1, StringData.fromString("x"))))
                .isEqualTo("{\"a\":1,\"b\":\"x\"}");
    }

    @Test
    void aNullMemberIsJsonNull() {
        assertThat(
                        render(
                                DataTypes.ROW(DataTypes.FIELD("a", DataTypes.INT())),
                                GenericRowData.of((Object) null)))
                .isEqualTo("{\"a\":null}");
    }

    @Test
    void stringsAreEscapedPerRfc8259() {
        // The control character is built from its code point rather than written into the
        // literal, where it would be invisible to a reader and easy to lose in an edit.
        String withControlChar = "a\"b\\c\nd\te" + (char) 0x01;
        assertThat(render(DataTypes.STRING(), StringData.fromString(withControlChar)))
                .isEqualTo("\"a\\\"b\\\\c\\nd\\te\\u0001\"");
    }

    @Test
    void aControlCharacterEscapeKeepsAsciiDigitsUnderALocaleThatDoesNot() {
        // The escape is formatted with %x, which takes no locale-specific digits where %d does —
        // under th-TH-u-nu-thai the latter renders 1 as ๑, and a document carrying that is JSON no
        // parser accepts. Pinned here rather than left to the comment beside the format call.
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(
                    new Locale.Builder()
                            .setLanguage("th")
                            .setRegion("TH")
                            .setExtension('u', "nu-thai")
                            .build());
            assertThat(render(DataTypes.STRING(), StringData.fromString("" + (char) 0x01)))
                    .isEqualTo("\"\\u0001\"");
        } finally {
            Locale.setDefault(previous);
        }
    }

    @Test
    void bytesBecomeBase64AndDecimalsUnquotedNumbers() {
        assertThat(render(DataTypes.BYTES(), new byte[] {1, 2, 3})).isEqualTo("\"AQID\"");
        assertThat(
                        render(
                                DataTypes.DECIMAL(10, 2),
                                DecimalData.fromBigDecimal(new BigDecimal("1.50"), 10, 2)))
                .isEqualTo("1.50");
    }

    @Test
    void temporalTypesBecomeIsoStrings() {
        assertThat(render(DataTypes.DATE(), (int) LocalDate.of(2026, 8, 6).toEpochDay()))
                .isEqualTo("\"2026-08-06\"");
        assertThat(
                        render(
                                DataTypes.TIME(3),
                                (int)
                                        (LocalTime.of(12, 34, 56, 789_000_000).toNanoOfDay()
                                                / 1_000_000L)))
                .isEqualTo("\"12:34:56.789\"");
        assertThat(
                        render(
                                DataTypes.TIMESTAMP(6),
                                TimestampData.fromLocalDateTime(
                                        LocalDateTime.of(2026, 8, 6, 12, 0))))
                .isEqualTo("\"2026-08-06T12:00\"");
        assertThat(render(DataTypes.TIMESTAMP_LTZ(6), TimestampData.fromEpochMillis(0L)))
                .isEqualTo("\"1970-01-01T00:00:00Z\"");
    }

    @Test
    void arraysAndMapsNestAsJsonDoes() {
        assertThat(
                        render(
                                DataTypes.ARRAY(DataTypes.INT()),
                                new GenericArrayData(new Object[] {1, null, 3})))
                .isEqualTo("[1,null,3]");

        Map<Object, Object> map = new HashMap<>();
        map.put(StringData.fromString("k"), StringData.fromString("v"));
        assertThat(
                        render(
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()),
                                new GenericMapData(map)))
                .isEqualTo("{\"k\":\"v\"}");
    }

    @Test
    void aTypeWithNoJsonFormIsRejectedWhenTheRendererIsBuilt() {
        // Built once from the column's type, so an unrenderable member fails at graph construction
        // rather than on the first record carrying one.
        assertThatThrownBy(
                        () ->
                                new RowDataJsonRenderer(
                                        DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "m",
                                                                DataTypes.MULTISET(
                                                                        DataTypes.STRING())))
                                                .getLogicalType(),
                                        "doc"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no JSON form");
    }

    @Test
    void aMapWithNonStringKeysIsRejected() {
        assertThatThrownBy(
                        () ->
                                new RowDataJsonRenderer(
                                        DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())
                                                .getLogicalType(),
                                        "doc"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("keys are strings");
    }

    @Test
    void aValueJsonCannotRepresentFailsItsOwnRow() {
        assertThatThrownBy(() -> render(DataTypes.DOUBLE(), Double.NaN))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("JSON cannot represent");
    }
}
