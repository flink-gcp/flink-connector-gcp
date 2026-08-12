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

package io.github.flink.gcp.connector.bigtable.table.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource.LookupContext;
import org.apache.flink.table.connector.source.LookupTableSource.LookupRuntimeProvider;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.FullCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingAsyncLookupProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.NestedFieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.BigtableSourceConfig;
import io.github.flink.gcp.connector.bigtable.source.readrows.BigtableReadRowsSource;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRanges;
import io.github.flink.gcp.connector.bigtable.table.BigtableLookupConfig;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What the source hands the DataStream builder, read back through {@code
 * BigtableReadRowsSource.getConfig()}.
 */
class BigtableDynamicSourceTest {

    private static ByteString key(String value) {
        return ByteString.copyFromUtf8(value);
    }

    /** A row key {@code rowkey}, a family {@code cf1} of one string, a family {@code cf2}. */
    private static final DataType PHYSICAL =
            DataTypes.ROW(
                    DataTypes.FIELD("rowkey", DataTypes.STRING()),
                    DataTypes.FIELD("cf1", DataTypes.ROW(DataTypes.FIELD("a", DataTypes.STRING()))),
                    DataTypes.FIELD(
                            "cf2", DataTypes.ROW(DataTypes.FIELD("m", DataTypes.DOUBLE()))));

    private static final BigtableTableSchema SCHEMA =
            BigtableTableSchema.of((RowType) PHYSICAL.getLogicalType());

    private static BigtableDynamicSource.Builder minimal() {
        return BigtableDynamicSource.builder()
                .schema(SCHEMA)
                .destination(TableDestination.of("p", "i", "t"))
                .nullStringLiteral("null")
                // The provider builds the source's real clients, which would demand
                // application-default credentials on a machine that has them and fail in CI on one
                // that does not. The endpoint is never connected to.
                .emulatorEndpoint("localhost:1")
                .lookupOptions(BigtableLookupConfig.from(new Configuration()))
                .producedDataType(PHYSICAL);
    }

    private static BigtableLookupConfig lookupOptions(String... entries) {
        Configuration config = new Configuration();
        for (int i = 0; i < entries.length; i += 2) {
            config.setString(entries[i], entries[i + 1]);
        }
        return BigtableLookupConfig.from(config);
    }

    private static LookupRuntimeProvider lookupProvider(BigtableDynamicSource source, int key) {
        return source.getLookupRuntimeProvider(lookupContext(new int[][] {{key}}));
    }

    private static LookupContext lookupContext(int[][] keys) {
        return (LookupContext)
                Proxy.newProxyInstance(
                        LookupContext.class.getClassLoader(),
                        new Class<?>[] {LookupContext.class},
                        (proxy, method, arguments) -> {
                            switch (method.getName()) {
                                case "getKeys":
                                    return keys;
                                case "createTypeInformation":
                                    if (arguments[0] instanceof DataType) {
                                        return ScanRuntimeProviderContext.INSTANCE
                                                .createTypeInformation((DataType) arguments[0]);
                                    }
                                    return ScanRuntimeProviderContext.INSTANCE
                                            .createTypeInformation(
                                                    (org.apache.flink.table.types.logical
                                                                    .LogicalType)
                                                            arguments[0]);
                                case "createDataStructureConverter":
                                    return ScanRuntimeProviderContext.INSTANCE
                                            .createDataStructureConverter((DataType) arguments[0]);
                                case "preferCustomShuffle":
                                    return false;
                                default:
                                    throw new UnsupportedOperationException(method.toString());
                            }
                        });
    }

    private static BigtableSourceConfig<?> configOf(BigtableDynamicSource source) {
        SourceProvider provider =
                (SourceProvider) source.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        return ((BigtableReadRowsSource<?>) provider.createSource()).getConfig();
    }

    private static List<String> rangesOf(BigtableSourceConfig<?> config) {
        return config.getRanges().stream().map(RowRanges::format).collect(Collectors.toList());
    }

    private static DataType projectedType(String... fieldNames) {
        List<String> physicalNames = ((RowType) PHYSICAL.getLogicalType()).getFieldNames();
        DataTypes.Field[] fields =
                Arrays.stream(fieldNames)
                        .map(
                                name ->
                                        DataTypes.FIELD(
                                                name,
                                                PHYSICAL.getChildren()
                                                        .get(physicalNames.indexOf(name))))
                        .toArray(DataTypes.Field[]::new);
        return DataTypes.ROW(fields);
    }

    private static FieldReferenceExpression rowKey() {
        return new FieldReferenceExpression("rowkey", DataTypes.STRING(), 0, 0);
    }

    private static NestedFieldReferenceExpression qualifier() {
        return new NestedFieldReferenceExpression(
                new String[] {"cf1", "a"}, new int[] {1, 0}, DataTypes.STRING());
    }

    private static ResolvedExpression literal(String value) {
        return new ValueLiteralExpression(value);
    }

    private static CallExpression call(
            BuiltInFunctionDefinition function, ResolvedExpression... children) {
        return CallExpression.permanent(function, Arrays.asList(children), DataTypes.BOOLEAN());
    }

    @Test
    void anUnprojectedScanRetainsEveryDeclaredFamily() {
        // The filter is applied even with no projection: families the physical table has but the
        // DDL does not declare never leave the server.
        BigtableSourceConfig<?> config = configOf(minimal().build());

        assertThat(config.getFilter().toProto())
                .isEqualTo(
                        FILTERS.interleave()
                                .filter(FILTERS.family().exactMatch("cf1"))
                                .filter(FILTERS.family().exactMatch("cf2"))
                                .toProto());
    }

    @Test
    void aProjectionPrunesToItsFamilies() {
        BigtableDynamicSource source = minimal().build();
        source.applyProjection(new int[][] {{0}, {2}}, projectedType("rowkey", "cf2"));

        assertThat(configOf(source).getFilter().toProto())
                .isEqualTo(FILTERS.family().exactMatch("cf2").toProto());
    }

    @Test
    void aRowKeyOnlyProjectionScansKeysOnly() {
        BigtableDynamicSource source = minimal().build();
        source.applyProjection(new int[][] {{0}}, projectedType("rowkey"));

        assertThat(configOf(source).getFilter().toProto())
                .isEqualTo(
                        FILTERS.chain()
                                .filter(FILTERS.limit().cellsPerRow(1))
                                .filter(FILTERS.value().strip())
                                .toProto());
    }

    @Test
    void exactRowKeyPredicatesIntersectEachOtherAndConfiguredBounds() {
        BigtableDynamicSource source =
                minimal().rangeStartClosed(key("a")).rangeEndOpen(key("z")).build();
        ResolvedExpression lower =
                call(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, rowKey(), literal("b"));
        ResolvedExpression upper =
                call(BuiltInFunctionDefinitions.LESS_THAN, rowKey(), literal("m"));

        SupportsFilterPushDown.Result result = source.applyFilters(Arrays.asList(lower, upper));

        assertThat(result.getAcceptedFilters()).containsExactly(lower, upper);
        assertThat(result.getRemainingFilters()).isEmpty();
        assertThat(rangesOf(configOf(source))).containsExactly("[b, m)");
    }

    @Test
    void aRowKeyDisjunctionBecomesTwoDisjointRanges() {
        BigtableDynamicSource source = minimal().build();
        ResolvedExpression filter =
                call(
                        BuiltInFunctionDefinitions.OR,
                        call(BuiltInFunctionDefinitions.LESS_THAN, rowKey(), literal("b")),
                        call(
                                BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                                rowKey(),
                                literal("y")));

        SupportsFilterPushDown.Result result =
                source.applyFilters(java.util.Collections.singletonList(filter));

        assertThat(result.getAcceptedFilters()).containsExactly(filter);
        assertThat(result.getRemainingFilters()).isEmpty();
        assertThat(rangesOf(configOf(source))).containsExactly("(*, b)", "[y, *)");
    }

    @Test
    void aLiteralOnTheLeftReversesTheRowKeyComparison() {
        BigtableDynamicSource source = minimal().build();
        ResolvedExpression filter =
                call(BuiltInFunctionDefinitions.LESS_THAN, literal("b"), rowKey());

        source.applyFilters(java.util.Collections.singletonList(filter));

        assertThat(rangesOf(configOf(source))).containsExactly("(b, *)");
    }

    @Test
    void inAndNotEqualsBecomeAnExactRangeUnion() {
        BigtableDynamicSource source = minimal().build();
        ResolvedExpression in =
                call(
                        BuiltInFunctionDefinitions.IN,
                        rowKey(),
                        literal("a"),
                        literal("b"),
                        literal("c"));
        ResolvedExpression notB =
                call(BuiltInFunctionDefinitions.NOT_EQUALS, rowKey(), literal("b"));

        SupportsFilterPushDown.Result result = source.applyFilters(Arrays.asList(in, notB));

        assertThat(result.getAcceptedFilters()).containsExactly(in, notB);
        assertThat(result.getRemainingFilters()).isEmpty();
        assertThat(rangesOf(configOf(source))).containsExactly("[a, a]", "[c, c]");
    }

    @Test
    void aQualifierPredicatePrefiltersButRemainsForFlink() {
        BigtableDynamicSource source = minimal().build();
        source.applyProjection(new int[][] {{0}}, projectedType("rowkey"));
        ResolvedExpression filter =
                call(BuiltInFunctionDefinitions.EQUALS, qualifier(), literal("alice"));

        SupportsFilterPushDown.Result result =
                source.applyFilters(java.util.Collections.singletonList(filter));

        assertThat(result.getAcceptedFilters()).containsExactly(filter);
        assertThat(result.getRemainingFilters()).containsExactly(filter);
        assertThat(configOf(source).getFilter().toProto())
                .isEqualTo(
                        FILTERS.condition(
                                        FILTERS.chain()
                                                .filter(FILTERS.family().exactMatch("cf1"))
                                                .filter(FILTERS.qualifier().exactMatch("a")))
                                .then(
                                        FILTERS.chain()
                                                .filter(FILTERS.limit().cellsPerRow(1))
                                                .filter(FILTERS.value().strip()))
                                .toProto());
    }

    @Test
    void aNullableQualifierTestCannotPrefilterRows() {
        BigtableDynamicSource source = minimal().build();
        ResolvedExpression filter = call(BuiltInFunctionDefinitions.IS_NULL, qualifier());

        SupportsFilterPushDown.Result result =
                source.applyFilters(java.util.Collections.singletonList(filter));

        assertThat(result.getAcceptedFilters()).isEmpty();
        assertThat(result.getRemainingFilters()).containsExactly(filter);
        assertThat(configOf(source).getFilter().toProto())
                .isEqualTo(
                        FILTERS.interleave()
                                .filter(FILTERS.family().exactMatch("cf1"))
                                .filter(FILTERS.family().exactMatch("cf2"))
                                .toProto());
    }

    @Test
    void anUnsupportedOrBranchPreventsBestEffortPushdown() {
        BigtableDynamicSource source = minimal().build();
        ResolvedExpression filter =
                call(
                        BuiltInFunctionDefinitions.OR,
                        call(BuiltInFunctionDefinitions.EQUALS, qualifier(), literal("alice")),
                        call(BuiltInFunctionDefinitions.IS_NULL, qualifier()));

        SupportsFilterPushDown.Result result =
                source.applyFilters(java.util.Collections.singletonList(filter));

        assertThat(result.getAcceptedFilters()).isEmpty();
        assertThat(result.getRemainingFilters()).containsExactly(filter);
        assertThat(configOf(source).getFilter().toProto())
                .isEqualTo(
                        FILTERS.interleave()
                                .filter(FILTERS.family().exactMatch("cf1"))
                                .filter(FILTERS.family().exactMatch("cf2"))
                                .toProto());
    }

    @Test
    void anImpossibleRangeIntersectionUsesABlockFilter() {
        BigtableDynamicSource source = minimal().build();
        ResolvedExpression beforeA =
                call(BuiltInFunctionDefinitions.LESS_THAN, rowKey(), literal("a"));
        ResolvedExpression fromA =
                call(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, rowKey(), literal("a"));

        source.applyFilters(Arrays.asList(beforeA, fromA));

        assertThat(configOf(source).getFilter().toProto()).isEqualTo(FILTERS.block().toProto());
    }

    @Test
    void anEmptyRowKeyLiteralRemainsForFlinkBecauseTheSdkCannotBoundIt() {
        ResolvedExpression equality =
                call(BuiltInFunctionDefinitions.EQUALS, rowKey(), literal(""));
        ResolvedExpression in =
                call(BuiltInFunctionDefinitions.IN, rowKey(), literal(""), literal("a"));

        for (ResolvedExpression filter : Arrays.asList(equality, in)) {
            BigtableDynamicSource source = minimal().build();

            SupportsFilterPushDown.Result result =
                    source.applyFilters(java.util.Collections.singletonList(filter));

            assertThat(result.getAcceptedFilters()).isEmpty();
            assertThat(result.getRemainingFilters()).containsExactly(filter);
            assertThat(rangesOf(configOf(source))).containsExactly("(*, *)");
        }
    }

    @Test
    void prefixesAndTheRangeAreAdditive() {
        BigtableSourceConfig<?> config =
                configOf(
                        minimal()
                                .prefixes(Arrays.asList(key("user"), key("web")))
                                .rangeStartClosed(key("a"))
                                .rangeEndOpen(key("m"))
                                .build());

        assertThat(rangesOf(config)).containsExactly("[a, m)", "[user, uses)", "[web, wec)");
    }

    @Test
    void aOneSidedRangeLeavesTheOtherEndOpen() {
        assertThat(rangesOf(configOf(minimal().rangeStartClosed(key("b")).build())))
                .containsExactly("[b, *)");
        assertThat(rangesOf(configOf(minimal().rangeEndOpen(key("b")).build())))
                .containsExactly("(*, b)");
    }

    @Test
    void theAppProfileReachesTheSourceConfig() {
        assertThat(configOf(minimal().appProfileId("boost").build()).getAppProfileId())
                .isEqualTo("boost");
        assertThat(configOf(minimal().build()).getAppProfileId()).isNull();
    }

    @Test
    void theParallelismReachesTheProvider() {
        SourceProvider provider =
                (SourceProvider)
                        minimal()
                                .parallelism(5)
                                .build()
                                .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

        assertThat(provider.getParallelism()).contains(5);
        assertThat(provider.isBounded()).isTrue();
    }

    @Test
    void theProducedTypeFollowsTheProjection() {
        // The spec's own sentence: the source reports the producedDataType it was passed, never
        // toPhysicalRowDataType(). A source that kept the physical type here still answers every
        // in-JVM test — operators chain, so the mismatched serializer is never exercised — and
        // fails only at a real network exchange, which is why the reported type is pinned
        // directly.
        BigtableDynamicSource source = minimal().build();
        source.applyProjection(new int[][] {{0}}, projectedType("rowkey"));

        assertThat(configOf(source).getDeserializer().getProducedType())
                .isEqualTo(
                        ScanRuntimeProviderContext.INSTANCE.createTypeInformation(
                                projectedType("rowkey")));
    }

    @Test
    void copyCarriesTheProjectionState() {
        BigtableDynamicSource projected = minimal().build();
        projected.applyProjection(new int[][] {{2}}, projectedType("cf2"));
        projected.applyFilters(
                java.util.Collections.singletonList(
                        call(
                                BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                                rowKey(),
                                literal("b"))));

        DynamicTableSource copy = projected.copy();

        assertThat(copy).isEqualTo(projected);
        assertThat(((BigtableDynamicSource) copy).copy()).isEqualTo(projected);
        assertThat(rangesOf(configOf((BigtableDynamicSource) copy))).containsExactly("[b, *)");
    }

    @Test
    void copyCarriesConfiguredBinaryRanges() {
        ByteString prefix = ByteString.copyFrom(new byte[] {0x00});
        ByteString start = ByteString.copyFrom(new byte[] {(byte) 0x80, 0x00});
        ByteString end = ByteString.copyFrom(new byte[] {(byte) 0x80, (byte) 0xff});
        BigtableDynamicSource source =
                minimal()
                        .prefixes(java.util.Collections.singletonList(prefix))
                        .rangeStartClosed(start)
                        .rangeEndOpen(end)
                        .build();

        BigtableDynamicSource copy = (BigtableDynamicSource) source.copy();

        assertThat(copy).isEqualTo(source);
        assertThat(configOf(copy).getRanges())
                .containsExactly(
                        ByteStringRange.prefix(prefix),
                        ByteStringRange.unbounded().startClosed(start).endOpen(end));
    }

    @Test
    void theProjectionStateDistinguishesSources() {
        BigtableDynamicSource plain = minimal().build();
        BigtableDynamicSource projected = minimal().build();
        projected.applyProjection(new int[][] {{2}}, projectedType("cf2"));

        assertThat(plain).isEqualTo(minimal().build());
        assertThat(plain.hashCode()).isEqualTo(minimal().build().hashCode());
        assertThat(projected).isNotEqualTo(plain);
    }

    @Test
    void anEmptyExactRangeDistinguishesSources() {
        BigtableDynamicSource plain = minimal().build();
        BigtableDynamicSource impossible = minimal().build();
        impossible.applyFilters(
                java.util.Collections.singletonList(
                        call(BuiltInFunctionDefinitions.IS_NULL, rowKey())));

        assertThat(impossible).isNotEqualTo(plain);
    }

    @Test
    void anUnrepresentableLiteralRemainsForFlink() {
        DataType physical = DataTypes.ROW(DataTypes.FIELD("rowkey", DataTypes.INT()));
        BigtableTableSchema schema = BigtableTableSchema.of((RowType) physical.getLogicalType());
        FieldReferenceExpression rowKey =
                new FieldReferenceExpression("rowkey", DataTypes.INT(), 0, 0);
        ResolvedExpression filter =
                call(
                        BuiltInFunctionDefinitions.EQUALS,
                        rowKey,
                        new ValueLiteralExpression(new BigDecimal("2147483648")));

        BigtableFilterPushDown.State state =
                BigtableFilterPushDown.translate(
                        schema, java.util.Collections.singletonList(filter));

        assertThat(state.result().getAcceptedFilters()).isEmpty();
        assertThat(state.result().getRemainingFilters()).containsExactly(filter);
    }

    @Test
    void fixedWidthEqualityIncludesEveryIgnoredSuffixByte() {
        DataType physical = DataTypes.ROW(DataTypes.FIELD("rowkey", DataTypes.INT()));
        BigtableTableSchema schema = BigtableTableSchema.of((RowType) physical.getLogicalType());
        FieldReferenceExpression rowKey =
                new FieldReferenceExpression("rowkey", DataTypes.INT(), 0, 0);
        ValueLiteralExpression seven = new ValueLiteralExpression(new BigDecimal("7"));
        ByteString canonical = ByteString.copyFrom(new byte[] {0, 0, 0, 7});
        ByteString suffixed = canonical.concat(ByteString.copyFrom(new byte[] {42}));
        ByteString next = ByteString.copyFrom(new byte[] {0, 0, 0, 8});

        BigtableFilterPushDown.State equal =
                BigtableFilterPushDown.translate(
                        schema,
                        java.util.Collections.singletonList(
                                call(BuiltInFunctionDefinitions.EQUALS, rowKey, seven)));
        BigtableFilterPushDown.State notEqual =
                BigtableFilterPushDown.translate(
                        schema,
                        java.util.Collections.singletonList(
                                call(BuiltInFunctionDefinitions.NOT_EQUALS, rowKey, seven)));

        assertThat(equal.rowKeyRanges())
                .singleElement()
                .satisfies(
                        range -> {
                            assertThat(RowRanges.contains(range, canonical)).isTrue();
                            assertThat(RowRanges.contains(range, suffixed)).isTrue();
                            assertThat(RowRanges.contains(range, next)).isFalse();
                        });
        assertThat(notEqual.rowKeyRanges())
                .allSatisfy(range -> assertThat(RowRanges.contains(range, suffixed)).isFalse())
                .anySatisfy(range -> assertThat(RowRanges.contains(range, next)).isTrue());
    }

    @Test
    void selectsExactlyOneProviderShapeForEveryPointLookupMode() {
        assertThat(lookupProvider(minimal().build(), 0))
                .isInstanceOf(LookupFunctionProvider.class)
                .isNotInstanceOf(AsyncLookupFunctionProvider.class);

        assertThat(
                        lookupProvider(
                                minimal()
                                        .lookupOptions(lookupOptions("lookup.async", "true"))
                                        .build(),
                                0))
                .isInstanceOf(AsyncLookupFunctionProvider.class)
                .isNotInstanceOf(LookupFunctionProvider.class);

        assertThat(
                        lookupProvider(
                                minimal()
                                        .lookupOptions(
                                                lookupOptions(
                                                        "lookup.cache",
                                                        "partial",
                                                        "lookup.partial-cache.max-rows",
                                                        "10"))
                                        .build(),
                                0))
                .isInstanceOf(PartialCachingLookupProvider.class);

        assertThat(
                        lookupProvider(
                                minimal()
                                        .lookupOptions(
                                                lookupOptions(
                                                        "lookup.async",
                                                        "true",
                                                        "lookup.cache",
                                                        "partial",
                                                        "lookup.partial-cache.max-rows",
                                                        "10"))
                                        .build(),
                                0))
                .isInstanceOf(PartialCachingAsyncLookupProvider.class);

        assertThat(
                        lookupProvider(
                                minimal()
                                        .lookupOptions(
                                                lookupOptions(
                                                        "lookup.cache",
                                                        "full",
                                                        "lookup.full-cache.periodic-reload.interval",
                                                        "1 min"))
                                        .build(),
                                0))
                .isInstanceOf(FullCachingLookupProvider.class);
    }

    @Test
    void aFullCacheLoaderUsesTheFilteredScanPlan() {
        BigtableDynamicSource source =
                minimal()
                        .rangeStartClosed(key("a"))
                        .rangeEndOpen(key("z"))
                        .lookupOptions(
                                lookupOptions(
                                        "lookup.cache",
                                        "full",
                                        "lookup.full-cache.periodic-reload.interval",
                                        "1 min"))
                        .build();
        source.applyFilters(
                Arrays.asList(
                        call(
                                BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                                rowKey(),
                                literal("b")),
                        call(BuiltInFunctionDefinitions.EQUALS, qualifier(), literal("alice"))));

        FullCachingLookupProvider provider = (FullCachingLookupProvider) lookupProvider(source, 0);
        BigtableFullCacheInputFormat inputFormat =
                (BigtableFullCacheInputFormat)
                        ((InputFormatProvider) provider.getScanRuntimeProvider())
                                .createInputFormat();

        assertThat(inputFormat.getRanges().stream().map(RowRanges::format))
                .containsExactly("[b, z)");
        assertThat(inputFormat.getFilter().toProto())
                .isEqualTo(
                        FILTERS.condition(
                                        FILTERS.chain()
                                                .filter(FILTERS.family().exactMatch("cf1"))
                                                .filter(FILTERS.qualifier().exactMatch("a")))
                                .then(
                                        FILTERS.interleave()
                                                .filter(FILTERS.family().exactMatch("cf1"))
                                                .filter(FILTERS.family().exactMatch("cf2")))
                                .toProto());
    }

    @Test
    void anEmptyExactFilterMakesTheFullCacheLoaderFinishWithoutARead() throws Exception {
        BigtableDynamicSource source =
                minimal()
                        .lookupOptions(
                                lookupOptions(
                                        "lookup.cache",
                                        "full",
                                        "lookup.full-cache.periodic-reload.interval",
                                        "1 min"))
                        .build();
        source.applyFilters(
                java.util.Collections.singletonList(
                        call(BuiltInFunctionDefinitions.IS_NULL, rowKey())));
        FullCachingLookupProvider provider = (FullCachingLookupProvider) lookupProvider(source, 0);
        BigtableFullCacheInputFormat inputFormat =
                (BigtableFullCacheInputFormat)
                        ((InputFormatProvider) provider.getScanRuntimeProvider())
                                .createInputFormat();

        assertThat(inputFormat.getRanges()).isEmpty();
        try {
            inputFormat.open(new GenericInputSplit(0, 1));
            assertThat(inputFormat.reachedEnd()).isTrue();
        } finally {
            inputFormat.close();
        }
    }

    @Test
    void lookupKeysIndexThePostProjectionRow() {
        BigtableDynamicSource projected = minimal().build();
        projected.applyProjection(new int[][] {{2}, {0}}, projectedType("cf2", "rowkey"));

        assertThat(lookupProvider(projected, 1)).isInstanceOf(LookupFunctionProvider.class);
        assertThatThrownBy(() -> lookupProvider(projected, 0))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("row-key column 'rowkey'");
    }

    @Test
    void rejectsACompositeOrNestedLookupKey() {
        BigtableDynamicSource source = minimal().build();

        assertThatThrownBy(
                        () ->
                                source.getLookupRuntimeProvider(
                                        lookupContext(new int[][] {{0}, {1}})))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("row-key column 'rowkey'");
        assertThatThrownBy(
                        () -> source.getLookupRuntimeProvider(lookupContext(new int[][] {{1, 0}})))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("row-key column 'rowkey'");
    }
}
