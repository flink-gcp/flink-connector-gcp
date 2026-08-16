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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Converts client rows built through the library's own factories — {@code Row.create} and {@code
 * RowCell.create} are {@code @InternalApi} factories on {@code @InternalExtensionOnly} classes, so
 * calling them is sanctioned (the {@code TestRows} precedent).
 *
 * <p>Cells are listed the way the service orders them: by family, then qualifier, then timestamp
 * <em>descending</em> — which is what the latest-version rule leans on.
 */
class RowToRowDataConverterTest {

    private static final String NULL_LITERAL = "NULL";

    /** A row key, a family {@code cf1} of a string and a long, a family {@code cf2} of a double. */
    private static final BigtableTableSchema SCHEMA =
            BigtableTableSchema.of(
                    (RowType)
                            DataTypes.ROW(
                                            DataTypes.FIELD("rowkey", DataTypes.STRING()),
                                            DataTypes.FIELD(
                                                    "cf1",
                                                    DataTypes.ROW(
                                                            DataTypes.FIELD(
                                                                    "a", DataTypes.STRING()),
                                                            DataTypes.FIELD(
                                                                    "b", DataTypes.BIGINT()))),
                                            DataTypes.FIELD(
                                                    "cf2",
                                                    DataTypes.ROW(
                                                            DataTypes.FIELD(
                                                                    "m", DataTypes.DOUBLE()))))
                                    .getLogicalType());

    private static final byte[] LONG_7 = {0, 0, 0, 0, 0, 0, 0, 7};
    private static final byte[] DOUBLE_1 = {0x3f, (byte) 0xf0, 0, 0, 0, 0, 0, 0};

    private static RowCell cell(String family, String qualifier, long timestamp, byte[] value) {
        return RowCell.create(
                family,
                ByteString.copyFromUtf8(qualifier),
                timestamp,
                Collections.emptyList(),
                ByteString.copyFrom(value));
    }

    private static Row row(String key, RowCell... cells) {
        List<RowCell> list = new ArrayList<>();
        Collections.addAll(list, cells);
        return Row.create(ByteString.copyFromUtf8(key), list);
    }

    private static Row fullRow() {
        return row(
                "k1",
                cell("cf1", "a", 1_000L, "x".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                cell("cf1", "b", 1_000L, LONG_7),
                cell("cf2", "m", 1_000L, DOUBLE_1));
    }

    @Test
    void theIdentityConvertsEveryDeclaredCell() {
        RowToRowDataConverter converter = new RowToRowDataConverter(SCHEMA, null, NULL_LITERAL);

        RowData out = converter.convert(fullRow());

        assertThat(out)
                .isEqualTo(
                        GenericRowData.of(
                                StringData.fromString("k1"),
                                GenericRowData.of(StringData.fromString("x"), 7L),
                                GenericRowData.of(1.0d)));
    }

    @Test
    void aReorderedProjectionMapsOutputPositions() {
        // SELECT cf1, rowkey: output position 0 is physical column 1, position 1 is column 0.
        RowToRowDataConverter converter =
                new RowToRowDataConverter(SCHEMA, new int[] {1, 0}, NULL_LITERAL);

        RowData out = converter.convert(fullRow());

        assertThat(out)
                .isEqualTo(
                        GenericRowData.of(
                                GenericRowData.of(StringData.fromString("x"), 7L),
                                StringData.fromString("k1")));
    }

    @Test
    void aFamilyOnlyProjectionDropsTheRowKey() {
        RowToRowDataConverter converter =
                new RowToRowDataConverter(SCHEMA, new int[] {2}, NULL_LITERAL);

        RowData out = converter.convert(fullRow());

        assertThat(out).isEqualTo(GenericRowData.of(GenericRowData.of(1.0d)));
    }

    @Test
    void aRowKeyOnlyProjectionReadsTheKeyAlone() {
        RowToRowDataConverter converter =
                new RowToRowDataConverter(SCHEMA, new int[] {0}, NULL_LITERAL);

        // What the keys-only filter chain delivers: one cell, value stripped, family intact. The
        // family carries no slot here, so the cell is ignored and only the key is read.
        RowData out = converter.convert(row("k1", cell("cf1", "a", 1_000L, new byte[0])));

        assertThat(out).isEqualTo(GenericRowData.of(StringData.fromString("k1")));
    }

    @Test
    void anEmptyProjectionProducesEmptyRows() {
        // SELECT COUNT(*): no column at all, one output row per Bigtable row.
        RowToRowDataConverter converter =
                new RowToRowDataConverter(SCHEMA, new int[0], NULL_LITERAL);

        RowData out = converter.convert(row("k1", cell("cf1", "a", 1_000L, new byte[0])));

        assertThat(out).isEqualTo(new GenericRowData(0));
    }

    @Test
    void anUndeclaredFamilyIsIgnored() {
        RowToRowDataConverter converter = new RowToRowDataConverter(SCHEMA, null, NULL_LITERAL);

        RowData out =
                converter.convert(
                        row(
                                "k1",
                                cell("cf1", "b", 1_000L, LONG_7),
                                cell("other", "b", 1_000L, LONG_7)));

        assertThat(out)
                .isEqualTo(
                        GenericRowData.of(
                                StringData.fromString("k1"), GenericRowData.of(null, 7L), null));
    }

    @Test
    void anUndeclaredQualifierIsIgnored() {
        RowToRowDataConverter converter = new RowToRowDataConverter(SCHEMA, null, NULL_LITERAL);

        RowData out =
                converter.convert(
                        row(
                                "k1",
                                cell("cf1", "b", 1_000L, LONG_7),
                                cell("cf1", "zzz", 1_000L, LONG_7)));

        assertThat(out)
                .isEqualTo(
                        GenericRowData.of(
                                StringData.fromString("k1"), GenericRowData.of(null, 7L), null));
    }

    @Test
    void aFamilyWithOnlyUndeclaredQualifiersReadsAsNull() {
        // The family has cells, but none the DDL declares: indistinguishable from an unwritten
        // family through this schema, so it reads the same way — as a null field, not a row of
        // nulls.
        RowToRowDataConverter converter = new RowToRowDataConverter(SCHEMA, null, NULL_LITERAL);

        RowData out = converter.convert(row("k1", cell("cf1", "zzz", 1_000L, LONG_7)));

        assertThat(out).isEqualTo(GenericRowData.of(StringData.fromString("k1"), null, null));
    }

    @Test
    void theLatestVersionOfACellWins() {
        RowToRowDataConverter converter = new RowToRowDataConverter(SCHEMA, null, NULL_LITERAL);

        // Two versions of cf1.b, newest first — the order the service returns them in.
        RowData out =
                converter.convert(
                        row(
                                "k1",
                                cell("cf1", "b", 2_000L, LONG_7),
                                cell("cf1", "b", 1_000L, new byte[] {0, 0, 0, 0, 0, 0, 0, 1})));

        assertThat(out)
                .isEqualTo(
                        GenericRowData.of(
                                StringData.fromString("k1"), GenericRowData.of(null, 7L), null));
    }

    @Test
    void aFamilyWithNoCellReadsAsNull() {
        RowToRowDataConverter converter = new RowToRowDataConverter(SCHEMA, null, NULL_LITERAL);

        RowData out = converter.convert(row("k1", cell("cf2", "m", 1_000L, DOUBLE_1)));

        assertThat(out)
                .isEqualTo(
                        GenericRowData.of(
                                StringData.fromString("k1"), null, GenericRowData.of(1.0d)));
    }

    @Test
    void nullsDecodeAsTheSinkWroteThem() {
        RowToRowDataConverter converter = new RowToRowDataConverter(SCHEMA, null, NULL_LITERAL);

        // The sink writes a null string as the null-string-literal and a null of any other type
        // as an empty cell; both must read back as SQL NULL inside a present family row.
        RowData out =
                converter.convert(
                        row(
                                "k1",
                                cell(
                                        "cf1",
                                        "a",
                                        1_000L,
                                        NULL_LITERAL.getBytes(
                                                java.nio.charset.StandardCharsets.UTF_8)),
                                cell("cf1", "b", 1_000L, new byte[0])));

        assertThat(out)
                .isEqualTo(
                        GenericRowData.of(
                                StringData.fromString("k1"), GenericRowData.of(null, null), null));
    }

    @Test
    void aMalformedCellIsReportedWithItsAddress() {
        // An external writer stored four bytes where the DDL declares BIGINT: the raw decoder
        // failure is an ArrayIndexOutOfBoundsException naming nothing, so the converter wraps it
        // with the one thing the operator needs — which cell of which row.
        RowToRowDataConverter converter = new RowToRowDataConverter(SCHEMA, null, NULL_LITERAL);

        assertThatThrownBy(
                        () ->
                                converter.convert(
                                        row(
                                                "k1",
                                                cell("cf1", "b", 1_000L, new byte[] {0, 0, 0, 7}))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cf1:b")
                .hasMessageContaining("'k1'")
                .hasMessageContaining("4 byte(s)");
    }

    @Test
    void theConverterSurvivesJavaSerialization() throws Exception {
        // The schema itself is not Serializable; the constructor must have resolved everything
        // into state that is, because the deserializer travels in the job graph.
        RowToRowDataConverter converter = new RowToRowDataConverter(SCHEMA, null, NULL_LITERAL);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(converter);
        }
        RowToRowDataConverter copy;
        try (ObjectInputStream in =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            copy = (RowToRowDataConverter) in.readObject();
        }

        assertThat(copy.convert(fullRow())).isEqualTo(converter.convert(fullRow()));
    }
}
