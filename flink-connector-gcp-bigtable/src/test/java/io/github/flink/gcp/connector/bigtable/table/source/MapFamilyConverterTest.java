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

package io.github.flink.gcp.connector.bigtable.table.source;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;
import io.github.flink.gcp.connector.bigtable.table.TrailingBytes;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Reads a {@code MAP} column family: every qualifier with a cell becomes an entry holding its
 * latest value. Cells are listed the way the service orders them — by family, then qualifier, then
 * timestamp descending.
 */
class MapFamilyConverterTest {

    private static final String NULL_LITERAL = "NULL";

    /**
     * A ROW family {@code cf} of one string, a STRING-keyed map {@code m} of strings, and a
     * BYTES-keyed map {@code n} of BIGINT values.
     */
    private static final BigtableTableSchema SCHEMA =
            BigtableTableSchema.of(
                    (RowType)
                            DataTypes.ROW(
                                            DataTypes.FIELD("rowkey", DataTypes.STRING()),
                                            DataTypes.FIELD(
                                                    "cf",
                                                    DataTypes.ROW(
                                                            DataTypes.FIELD(
                                                                    "a", DataTypes.STRING()))),
                                            DataTypes.FIELD(
                                                    "m",
                                                    DataTypes.MAP(
                                                            DataTypes.STRING(),
                                                            DataTypes.STRING())),
                                            DataTypes.FIELD(
                                                    "n",
                                                    DataTypes.MAP(
                                                            DataTypes.BYTES(), DataTypes.BIGINT())))
                                    .getLogicalType());

    private static final byte[] LONG_7 = {0, 0, 0, 0, 0, 0, 0, 7};
    private static final byte[] LONG_1 = {0, 0, 0, 0, 0, 0, 0, 1};

    private static RowCell cell(String family, byte[] qualifier, long timestamp, byte[] value) {
        return RowCell.create(
                family,
                ByteString.copyFrom(qualifier),
                timestamp,
                Collections.emptyList(),
                ByteString.copyFrom(value));
    }

    private static RowCell cell(String family, String qualifier, long timestamp, String value) {
        return cell(family, utf8(qualifier), timestamp, utf8(value));
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static Row row(RowCell... cells) {
        List<RowCell> list = new ArrayList<>();
        Collections.addAll(list, cells);
        return Row.create(ByteString.copyFromUtf8("k1"), list);
    }

    private static RowToRowDataConverter converter(int[] projection) {
        return new RowToRowDataConverter(SCHEMA, projection, NULL_LITERAL, TrailingBytes.IGNORE);
    }

    @Test
    void everyQualifierWithACellBecomesAnEntryBesideARowFamily() {
        GenericRowData out =
                converter(null)
                        .convert(
                                row(
                                        cell("cf", "a", 1_000L, "x"),
                                        cell("m", "a", 1_000L, "x"),
                                        cell("m", "b", 1_000L, "y"),
                                        cell("n", new byte[] {0x00, (byte) 0xff}, 1_000L, LONG_7)));

        assertThat(out.getString(0)).isEqualTo(StringData.fromString("k1"));
        assertThat(out.getRow(1, 1)).isEqualTo(GenericRowData.of(StringData.fromString("x")));
        GenericMapData m = (GenericMapData) out.getMap(2);
        assertThat(m.size()).isEqualTo(2);
        assertThat(m.get(StringData.fromString("a"))).isEqualTo(StringData.fromString("x"));
        assertThat(m.get(StringData.fromString("b"))).isEqualTo(StringData.fromString("y"));
        GenericMapData n = (GenericMapData) out.getMap(3);
        assertThat(n.size()).isEqualTo(1);
        assertThat(n.keyArray().getBinary(0)).containsExactly(0x00, 0xff);
    }

    @Test
    void aBytesKeyIsFoundByItsContentRatherThanItsIdentity() {
        // Flink's generated code answers m[key] on a GenericMapData with Map.get, and a byte[]
        // hashes by identity: a hash map would answer every lookup below with null.
        GenericRowData out =
                converter(null)
                        .convert(row(cell("n", new byte[] {0x00, (byte) 0xff}, 1_000L, LONG_7)));

        GenericMapData n = (GenericMapData) out.getMap(3);
        assertThat(n.get(new byte[] {0x00, (byte) 0xff})).isEqualTo(7L);
        assertThat(n.get(new byte[] {0x00})).isNull();
    }

    @Test
    void theLatestVersionOfAnEntryWins() {
        GenericRowData out =
                converter(null)
                        .convert(
                                row(
                                        cell("n", utf8("q"), 2_000L, LONG_7),
                                        cell("n", utf8("q"), 1_000L, LONG_1)));

        GenericMapData n = (GenericMapData) out.getMap(3);
        assertThat(n.size()).isEqualTo(1);
        assertThat(n.get(utf8("q"))).isEqualTo(7L);
    }

    @Test
    void aFamilyWithNoCellReadsAsNullRatherThanAnEmptyMap() {
        // So m IS NOT NULL means "the family has a cell", which the family-existence prefilter
        // pushed for it assumes.
        GenericRowData out = converter(null).convert(row(cell("cf", "a", 1_000L, "x")));

        assertThat(out.isNullAt(2)).isTrue();
        assertThat(out.isNullAt(3)).isTrue();
    }

    @Test
    void aNullValueIsReadAsTheSinkWroteIt() {
        GenericRowData out =
                converter(null)
                        .convert(
                                row(
                                        cell("m", "a", 1_000L, NULL_LITERAL),
                                        cell("m", "b", 1_000L, ""),
                                        cell("n", utf8("q"), 1_000L, new byte[0])));

        GenericMapData m = (GenericMapData) out.getMap(2);
        assertThat(m.size()).isEqualTo(2);
        assertThat(m.get(StringData.fromString("a"))).isNull();
        assertThat(m.get(StringData.fromString("b"))).isEqualTo(StringData.fromString(""));
        // By position, so a key the lookup failed to find cannot pass for a null value.
        GenericMapData n = (GenericMapData) out.getMap(3);
        assertThat(n.size()).isEqualTo(1);
        assertThat(n.keyArray().getBinary(0)).isEqualTo(utf8("q"));
        assertThat(n.valueArray().isNullAt(0)).isTrue();
    }

    @Test
    void aQualifierIsAKeyEvenWhenItLooksLikeANull() {
        // Keys take the plain decoder: the null convention is a value's, and a qualifier spelled
        // like the null-string-literal, or an empty one, is still a qualifier.
        GenericRowData out =
                converter(null)
                        .convert(
                                row(
                                        cell("m", NULL_LITERAL, 1_000L, "x"),
                                        cell("n", new byte[0], 1_000L, LONG_7)));

        GenericMapData m = (GenericMapData) out.getMap(2);
        assertThat(m.keyArray().isNullAt(0)).isFalse();
        assertThat(m.get(StringData.fromString(NULL_LITERAL)))
                .isEqualTo(StringData.fromString("x"));
        GenericMapData n = (GenericMapData) out.getMap(3);
        assertThat(n.keyArray().getBinary(0)).isEmpty();
        assertThat(n.get(new byte[0])).isEqualTo(7L);
    }

    @Test
    void aNotNullBytesValueReadsAnEmptyCellAsEmptyBytes() {
        BigtableTableSchema schema =
                BigtableTableSchema.of(
                        (RowType)
                                DataTypes.ROW(
                                                DataTypes.FIELD("rowkey", DataTypes.STRING()),
                                                DataTypes.FIELD(
                                                        "m",
                                                        DataTypes.MAP(
                                                                DataTypes.BYTES(),
                                                                DataTypes.BYTES().notNull())))
                                        .getLogicalType());

        GenericRowData out =
                new RowToRowDataConverter(schema, null, NULL_LITERAL, TrailingBytes.IGNORE)
                        .convert(row(cell("m", utf8("q"), 1_000L, new byte[0])));

        assertThat((byte[]) ((GenericMapData) out.getMap(1)).get(utf8("q"))).isEmpty();
    }

    @Test
    void aProjectionOfTheMapAloneReadsIt() {
        GenericRowData out =
                converter(new int[] {2})
                        .convert(row(cell("cf", "a", 1_000L, "x"), cell("m", "a", 1_000L, "x")));

        assertThat(out.getArity()).isEqualTo(1);
        assertThat(((GenericMapData) out.getMap(0)).get(StringData.fromString("a")))
                .isEqualTo(StringData.fromString("x"));
    }

    @Test
    void anUndecodableValueNamesItsFamilyItsEscapedQualifierAndTheRow() {
        assertThatThrownBy(
                        () ->
                                converter(null)
                                        .convert(
                                                row(
                                                        cell(
                                                                "n",
                                                                new byte[] {(byte) 0xff},
                                                                1_000L,
                                                                new byte[] {1, 2}))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cell n:\\xff of the row with key 'k1' holds 2 byte(s)")
                .hasMessageContaining("declared map value type cannot decode");
    }

    @Test
    void theConverterSurvivesJavaSerialization() throws Exception {
        RowToRowDataConverter restored =
                InstantiationUtil.clone(converter(null), getClass().getClassLoader());

        GenericRowData out = restored.convert(row(cell("n", utf8("q"), 1_000L, LONG_7)));

        assertThat(((GenericMapData) out.getMap(3)).get(utf8("q"))).isEqualTo(7L);
    }
}
