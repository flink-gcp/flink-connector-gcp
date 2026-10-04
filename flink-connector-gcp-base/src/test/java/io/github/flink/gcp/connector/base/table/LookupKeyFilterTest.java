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

package io.github.flink.gcp.connector.base.table;

import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LookupKeyFilterTest {
    private static StringData text(String value) {
        return StringData.fromString(value);
    }

    private static final RowType SCALAR =
            (RowType)
                    DataTypes.ROW(
                                    DataTypes.FIELD("tier", DataTypes.STRING()),
                                    DataTypes.FIELD("id", DataTypes.STRING()),
                                    DataTypes.FIELD("payload", DataTypes.BYTES()))
                            .getLogicalType();

    private static LookupKeyFilter scalar() {
        return LookupKeyFilter.of(
                SCALAR, new int[][] {{0}, {1}, {2}}, null, new int[] {1}, false, "requires id");
    }

    private static LookupKeyFilter rowKeys() {
        RowType type =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.STRING()),
                                        DataTypes.FIELD(
                                                "family",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("tier", DataTypes.STRING()),
                                                        DataTypes.FIELD(
                                                                "optional", DataTypes.BIGINT()))))
                                .getLogicalType();
        return LookupKeyFilter.of(
                type, new int[][] {{0}, {1}}, null, new int[] {0}, true, "requires id");
    }

    @Test
    void filtersEveryExtraFieldAndPassesOnlyTheAddress() throws Exception {
        FakeReader reader =
                new FakeReader(GenericRowData.of(text("gold"), text("a"), new byte[] {1}));
        LookupFunction function = scalar().wrap(reader);
        assertThat(function.lookup(GenericRowData.of(text("gold"), text("a"), new byte[] {1})))
                .hasSize(1);
        assertThat(reader.lastKey).isEqualTo(GenericRowData.of(text("a")));
        assertThat(function.lookup(GenericRowData.of(text("silver"), text("a"), new byte[] {1})))
                .isEmpty();
        assertThat(function.lookup(GenericRowData.of(text("gold"), text("a"), new byte[] {2})))
                .isEmpty();
        reader.row = GenericRowData.of(null, text("a"), new byte[] {1});
        assertThat(function.lookup(GenericRowData.of(text("gold"), text("a"), new byte[] {1})))
                .isEmpty();
        reader.row = null;
        assertThat(function.lookup(GenericRowData.of(text("gold"), text("a"), new byte[] {1})))
                .isEmpty();
    }

    @Test
    void topLevelNullKeysDoNotRead() throws Exception {
        FakeReader reader =
                new FakeReader(GenericRowData.of(text("gold"), text("a"), new byte[] {1}));
        LookupFunction function = scalar().wrap(reader);
        for (int position = 0; position < 3; position++) {
            GenericRowData key = GenericRowData.of(text("gold"), text("a"), new byte[] {1});
            key.setField(position, null);
            assertThat(function.lookup(key)).isEmpty();
        }
        assertThat(reader.reads).isZero();
    }

    @Test
    void nullValuesAreHandledEvenForNotNullDeclarations() throws Exception {
        RowType type =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.BIGINT().notNull()),
                                        DataTypes.FIELD("extra", DataTypes.BIGINT().notNull()))
                                .getLogicalType();
        LookupKeyFilter keys =
                LookupKeyFilter.of(
                        type, new int[][] {{0}, {1}}, null, new int[] {0}, false, "requires id");
        FakeReader reader = new FakeReader(GenericRowData.of(7L, 9L));
        LookupFunction function = keys.wrap(reader);
        assertThat(function.lookup(GenericRowData.of(null, 9L))).isEmpty();
        assertThat(function.lookup(GenericRowData.of(7L, null))).isEmpty();
        assertThat(reader.reads).isZero();
        reader.row = GenericRowData.of(7L, null);
        assertThat(function.lookup(GenericRowData.of(7L, 9L))).isEmpty();

        RowType familyType =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.BIGINT().notNull()),
                                        DataTypes.FIELD(
                                                "family",
                                                DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "v",
                                                                        DataTypes.BIGINT()
                                                                                .notNull()))
                                                        .notNull()))
                                .getLogicalType();
        LookupKeyFilter families =
                LookupKeyFilter.of(
                        familyType,
                        new int[][] {{0}, {1}},
                        null,
                        new int[] {0},
                        true,
                        "requires id");
        reader.row = GenericRowData.of(7L, GenericRowData.of((Object) null));
        assertThat(
                        families.wrap(reader)
                                .lookup(GenericRowData.of(7L, GenericRowData.of((Object) null))))
                .hasSize(1);
        reader.row = GenericRowData.of(7L, null);
        assertThat(families.wrap(reader).lookup(GenericRowData.of(7L, GenericRowData.of(9L))))
                .isEmpty();
    }

    @Test
    void restoresCompositeAddressOrderAfterProjectionAndKeyReordering() throws Exception {
        RowType type =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                        DataTypes.FIELD("account", DataTypes.BIGINT()),
                                        DataTypes.FIELD("region", DataTypes.STRING()))
                                .getLogicalType();
        LookupKeyFilter keys =
                LookupKeyFilter.of(
                        type,
                        new int[][] {{1}, {0}, {2}},
                        new int[] {2, 1, 0},
                        new int[] {0, 1},
                        false,
                        "requires region and account");
        assertThat(keys.addressingPositions()).containsExactly(2, 0);
        FakeReader reader = new FakeReader(GenericRowData.of(text("Ada"), 7L, text("us")));
        assertThat(keys.wrap(reader).lookup(GenericRowData.of(7L, text("Ada"), text("us"))))
                .hasSize(1);
        assertThat(reader.lastKey).isEqualTo(GenericRowData.of(text("us"), 7L));
    }

    @Test
    void refusesMissingDuplicateNestedAndOutOfBoundsKeys() {
        for (int[][] keys : new int[][][] {{{0}}, {{1}, {1}}, {{1, 0}}, {{-1}}, {{3}}, {}}) {
            assertThatThrownBy(
                            () ->
                                    LookupKeyFilter.of(
                                            SCALAR,
                                            keys,
                                            null,
                                            new int[] {1},
                                            false,
                                            "requires id"))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining("requires id");
        }
        assertThatThrownBy(
                        () ->
                                LookupKeyFilter.of(
                                        SCALAR,
                                        new int[][] {{0}},
                                        null,
                                        new int[0],
                                        false,
                                        "requires id"))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void refusesMetadataAndUnsupportedAdditionalTypes() {
        assertThatThrownBy(
                        () ->
                                LookupKeyFilter.of(
                                        SCALAR,
                                        new int[][] {{1}, {2}},
                                        new int[] {0, 1},
                                        new int[] {1},
                                        false,
                                        "requires id"))
                .isInstanceOf(ValidationException.class);
        for (org.apache.flink.table.types.DataType extra :
                List.of(
                        DataTypes.ARRAY(DataTypes.STRING()),
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()),
                        DataTypes.ROW(DataTypes.FIELD("v", DataTypes.STRING())))) {
            RowType type =
                    (RowType)
                            DataTypes.ROW(
                                            DataTypes.FIELD("id", DataTypes.STRING()),
                                            DataTypes.FIELD("extra", extra))
                                    .getLogicalType();
            assertThatThrownBy(
                            () ->
                                    LookupKeyFilter.of(
                                            type,
                                            new int[][] {{0}, {1}},
                                            null,
                                            new int[] {0},
                                            false,
                                            "requires id"))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining("extra")
                    .hasMessageContaining("unsupported equality type");
        }
    }

    @Test
    void rowComparisonHandlesBinaryRepresentationAndNullableFields() throws Exception {
        BinaryRowData binary = new BinaryRowData(2);
        BinaryRowWriter writer = new BinaryRowWriter(binary);
        writer.writeString(0, text("gold"));
        writer.setNullAt(1);
        writer.complete();
        FakeReader reader = new FakeReader(GenericRowData.of(text("a"), binary));
        LookupFunction function = rowKeys().wrap(reader);
        assertThat(
                        function.lookup(
                                GenericRowData.of(
                                        text("a"), GenericRowData.of(text("gold"), null))))
                .hasSize(1);
        assertThat(
                        function.lookup(
                                GenericRowData.of(text("a"), GenericRowData.of(text("gold"), 1L))))
                .isEmpty();
        assertThat(
                        function.lookup(
                                GenericRowData.of(
                                        text("a"), GenericRowData.of(text("silver"), null))))
                .isEmpty();
        reader.row = GenericRowData.of(text("a"), null);
        assertThat(
                        function.lookup(
                                GenericRowData.of(
                                        text("a"), GenericRowData.of(text("gold"), null))))
                .isEmpty();
    }

    @Test
    void floatingEqualityMatchesSqlForZeroAndNan() throws Exception {
        RowType type =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.STRING()),
                                        DataTypes.FIELD("v", DataTypes.DOUBLE()))
                                .getLogicalType();
        LookupKeyFilter keys =
                LookupKeyFilter.of(
                        type, new int[][] {{0}, {1}}, null, new int[] {0}, false, "requires id");
        FakeReader reader = new FakeReader(GenericRowData.of(text("a"), -0d));
        LookupFunction function = keys.wrap(reader);
        assertThat(function.lookup(GenericRowData.of(text("a"), 0d))).hasSize(1);
        reader.row = GenericRowData.of(text("a"), Double.NaN);
        assertThat(function.lookup(GenericRowData.of(text("a"), Double.NaN))).isEmpty();
    }

    @Test
    void decimalAndNanosecondTimestampKeysCompareByValue() throws Exception {
        RowType type =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.STRING()),
                                        DataTypes.FIELD("balance", DataTypes.DECIMAL(38, 9)),
                                        DataTypes.FIELD("updated", DataTypes.TIMESTAMP_LTZ(9)))
                                .getLogicalType();
        LookupKeyFilter keys =
                LookupKeyFilter.of(
                        type,
                        new int[][] {{0}, {1}, {2}},
                        null,
                        new int[] {0},
                        false,
                        "requires id");
        FakeReader reader =
                new FakeReader(
                        GenericRowData.of(
                                text("a"),
                                DecimalData.fromBigDecimal(new BigDecimal("12.345678901"), 38, 9),
                                TimestampData.fromEpochMillis(1234, 567890)));
        LookupFunction function = keys.wrap(reader);
        GenericRowData key =
                GenericRowData.of(
                        text("a"),
                        DecimalData.fromBigDecimal(new BigDecimal("12.345678901"), 38, 9),
                        TimestampData.fromEpochMillis(1234, 567890));
        assertThat(function.lookup(key)).hasSize(1);
        key.setField(2, TimestampData.fromEpochMillis(1234, 567891));
        assertThat(function.lookup(key)).isEmpty();
        key.setField(2, TimestampData.fromEpochMillis(1234, 567890));
        key.setField(1, DecimalData.fromBigDecimal(new BigDecimal("12.345678902"), 38, 9));
        assertThat(function.lookup(key)).isEmpty();
    }

    @Test
    void asyncInvocationsSnapshotMutableKeysAndRemainIndependent() throws Exception {
        CompletableFuture<Collection<RowData>> first = new CompletableFuture<>();
        CompletableFuture<Collection<RowData>> second = new CompletableFuture<>();
        AsyncLookupFunction reader =
                new AsyncLookupFunction() {
                    @Override
                    public CompletableFuture<Collection<RowData>> asyncLookup(RowData key) {
                        return key.getString(0).toString().equals("a") ? first : second;
                    }
                };
        AsyncLookupFunction function = scalar().wrap(reader);
        byte[] bytes = {1};
        GenericRowData key = GenericRowData.of(text("gold"), text("a"), bytes);
        CompletableFuture<Collection<RowData>> a = function.asyncLookup(key);
        key.setField(0, text("silver"));
        key.setField(1, text("b"));
        bytes[0] = 2;
        CompletableFuture<Collection<RowData>> b = function.asyncLookup(key);
        second.complete(List.of(GenericRowData.of(text("silver"), text("b"), new byte[] {2})));
        first.complete(List.of(GenericRowData.of(text("gold"), text("a"), new byte[] {1})));
        assertThat(a.get()).hasSize(1);
        assertThat(b.get()).hasSize(1);
        key.setField(2, null);
        assertThat(function.asyncLookup(key).get()).isEmpty();
    }

    @Test
    void asyncInvocationsSnapshotMutableFamilyRows() throws Exception {
        CompletableFuture<Collection<RowData>> result = new CompletableFuture<>();
        AsyncLookupFunction reader =
                new AsyncLookupFunction() {
                    @Override
                    public CompletableFuture<Collection<RowData>> asyncLookup(RowData key) {
                        return result;
                    }
                };
        GenericRowData family = GenericRowData.of(text("gold"), 1L);
        CompletableFuture<Collection<RowData>> pending =
                rowKeys().wrap(reader).asyncLookup(GenericRowData.of(text("a"), family));
        family.setField(0, text("silver"));
        family.setField(1, 2L);
        result.complete(List.of(GenericRowData.of(text("a"), GenericRowData.of(text("gold"), 1L))));
        assertThat(pending.get()).hasSize(1);
    }

    @Test
    void partialCacheSeparatesCompleteTuplesAndRespectsMissingKeyPolicy() throws Exception {
        for (boolean missing : new boolean[] {false, true}) {
            LookupCache cache =
                    scalar().wrap(
                                    DefaultLookupCache.newBuilder()
                                            .maximumSize(10)
                                            .cacheMissingKey(missing)
                                            .build());
            cache.open(UnregisteredMetricsGroup.createCacheMetricGroup());
            try {
                GenericRowData gold = GenericRowData.of(text("gold"), text("a"), new byte[] {1});
                GenericRowData silver =
                        GenericRowData.of(text("silver"), text("a"), new byte[] {1});
                Collection<RowData> hit =
                        List.of(GenericRowData.of(text("gold"), text("a"), new byte[] {1}));
                cache.put(gold, hit);
                cache.put(silver, List.of());
                assertThat(
                                cache.getIfPresent(
                                        GenericRowData.of(text("gold"), text("a"), new byte[] {1})))
                        .isEqualTo(hit);
                if (missing) {
                    assertThat(cache.getIfPresent(silver)).isEmpty();
                } else {
                    assertThat(cache.getIfPresent(silver)).isNull();
                }
                gold.setField(0, text("changed"));
                assertThat(
                                cache.getIfPresent(
                                        GenericRowData.of(text("gold"), text("a"), new byte[] {1})))
                        .isEqualTo(hit);
                cache.invalidate(GenericRowData.of(text("gold"), text("a"), new byte[] {1}));
                assertThat(
                                cache.getIfPresent(
                                        GenericRowData.of(text("gold"), text("a"), new byte[] {1})))
                        .isNull();
            } finally {
                cache.close();
            }
        }
    }

    @Test
    void partialCacheNormalizesRowRepresentations() throws Exception {
        BinaryRowData binary = new BinaryRowData(2);
        BinaryRowWriter writer = new BinaryRowWriter(binary);
        writer.writeString(0, text("gold"));
        writer.setNullAt(1);
        writer.complete();
        LookupCache cache = rowKeys().wrap(DefaultLookupCache.newBuilder().maximumSize(10).build());
        cache.open(UnregisteredMetricsGroup.createCacheMetricGroup());
        try {
            Collection<RowData> result =
                    List.of(GenericRowData.of(text("a"), GenericRowData.of(text("gold"), null)));
            cache.put(GenericRowData.of(text("a"), binary), result);
            assertThat(cache.getIfPresent(GenericRowData.of(text("a"), binary))).isEqualTo(result);
            assertThat(
                            cache.getIfPresent(
                                    GenericRowData.of(
                                            text("a"), GenericRowData.of(text("gold"), null))))
                    .isEqualTo(result);
            cache.invalidate(GenericRowData.of(text("a"), binary));
            assertThat(
                            cache.getIfPresent(
                                    GenericRowData.of(
                                            text("a"), GenericRowData.of(text("gold"), null))))
                    .isNull();
        } finally {
            cache.close();
        }
    }

    @Test
    void serializesTheFilterWithItsMapping() throws Exception {
        LookupKeyFilter restored = InstantiationUtil.clone(scalar(), getClass().getClassLoader());
        FakeReader reader =
                new FakeReader(GenericRowData.of(text("gold"), text("a"), new byte[] {1}));
        assertThat(
                        restored.wrap(reader)
                                .lookup(GenericRowData.of(text("gold"), text("a"), new byte[] {1})))
                .hasSize(1);
        LookupKeyFilter restoredFamily =
                InstantiationUtil.clone(rowKeys(), getClass().getClassLoader());
        FakeReader familyReader =
                new FakeReader(GenericRowData.of(text("a"), GenericRowData.of(text("gold"), null)));
        assertThat(
                        restoredFamily
                                .wrap(familyReader)
                                .lookup(
                                        GenericRowData.of(
                                                text("a"), GenericRowData.of(text("gold"), null))))
                .hasSize(1);
    }

    private static final class FakeReader extends LookupFunction {
        private RowData row;
        private RowData lastKey;
        private int reads;

        private FakeReader(RowData row) {
            this.row = row;
        }

        @Override
        public Collection<RowData> lookup(RowData key) {
            reads++;
            lastKey = key;
            return row == null ? List.of() : List.of(row);
        }
    }
}
