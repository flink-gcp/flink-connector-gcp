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

package io.github.flink.gcp.connector.spanner.source.changestream;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

class SpannerChangeStreamRecordFilterTest {

    @Test
    void onlyConfiguredTableOrColumnPatternsActivateFiltering() {
        assertThat(SpannerChangeStreamRecordFilter.none().hasFilters()).isFalse();
        assertThat(filter(null, null, null, null, true).hasFilters()).isFalse();

        assertThat(filter("orders", null, null, null, false).hasFilters()).isTrue();
        assertThat(filter(null, "orders", null, null, false).hasFilters()).isTrue();
        assertThat(filter(null, null, "orders\\.status", null, false).hasFilters()).isTrue();
        assertThat(filter(null, null, null, "orders\\.secret", false).hasFilters()).isTrue();
    }

    @Test
    void tablePatternsUseFullMatchForIncludeAndExcludeLists() {
        DataChangeRecord orders = record("orders", columns(id()), mods("{\"status\":\"open\"}"));
        DataChangeRecord archive =
                record("orders_archive", columns(id()), mods("{\"status\":\"open\"}"));

        SpannerChangeStreamRecordFilter include = filter("orders", null, null, null, false);
        assertThat(include.filter(orders).getDisposition())
                .isEqualTo(SpannerChangeStreamRecordFilter.Result.Disposition.DELIVER);
        assertThat(include.filter(archive).getDisposition())
                .isEqualTo(SpannerChangeStreamRecordFilter.Result.Disposition.TABLE_FILTERED);

        SpannerChangeStreamRecordFilter exclude = filter(null, "orders", null, null, false);
        assertThat(exclude.filter(orders).getDisposition())
                .isEqualTo(SpannerChangeStreamRecordFilter.Result.Disposition.TABLE_FILTERED);
        assertThat(exclude.filter(archive).getDisposition())
                .isEqualTo(SpannerChangeStreamRecordFilter.Result.Disposition.DELIVER);
    }

    @Test
    void projectionUsesQualifiedColumnNamesAndPreservesTypeDescriptors() {
        DataChangeRecord record =
                record(
                        "public.orders",
                        columns(
                                id(),
                                column("status", "{\"code\":\"STRING\"}", false, 2),
                                column(
                                        "tokens",
                                        "{\"code\":\"TOKENLIST\",\"type_annotation\":\"SEARCH\"}",
                                        false,
                                        3),
                                column("future", "{\"code\":\"FUTURE_TYPE\"}", false, 4)),
                        Collections.singletonList(
                                new Mod(
                                        "{\"id\":1}",
                                        "{\"status\":\"open\",\"tokens\":null,\"future\":{\"x\":1}}",
                                        "{\"status\":\"new\",\"tokens\":\"old\"}")));
        SpannerChangeStreamRecordFilter filter =
                filter(null, null, "public\\.orders\\.(status|future)", null, false);

        SpannerChangeStreamRecordFilter.Result result = filter.filter(record);
        DataChangeRecord projected = result.getRecord();

        assertThat(result.getRemovedColumnOccurrences()).isEqualTo(3);
        assertThat(projected.getColumnTypes())
                .extracting(DataChangeRecord.ColumnType::getName)
                .containsExactly("id", "status", "future");
        assertThat(projected.getColumnTypes().get(2).getTypeDescriptorJson())
                .isEqualTo("{\"code\":\"FUTURE_TYPE\"}");
        assertThat(projected.getMods().get(0).getKeysJson()).isEqualTo("{\"id\":1}");
        assertThat(projected.getMods().get(0).getNewValuesJson())
                .contains("{\"future\":{\"x\":1},\"status\":\"open\"}");
        assertThat(projected.getMods().get(0).getOldValuesJson()).contains("{\"status\":\"new\"}");
        assertThat(projected.getCommitTimestamp()).isEqualTo(record.getCommitTimestamp());
        assertThat(projected.getRecordSequence()).isEqualTo(record.getRecordSequence());
        assertThat(projected.getServerTransactionId()).isEqualTo(record.getServerTransactionId());
        assertThat(projected.isLastRecordInTransactionInPartition())
                .isEqualTo(record.isLastRecordInTransactionInPartition());
        assertThat(projected.getModType()).isEqualTo(record.getModType());
        assertThat(projected.getValueCaptureType()).isEqualTo(record.getValueCaptureType());
        assertThat(projected.getNumberOfRecordsInTransaction())
                .isEqualTo(record.getNumberOfRecordsInTransaction());
        assertThat(projected.getNumberOfPartitionsInTransaction())
                .isEqualTo(record.getNumberOfPartitionsInTransaction());
        assertThat(projected.getTransactionTag()).isEqualTo(record.getTransactionTag());
        assertThat(projected.isSystemTransaction()).isEqualTo(record.isSystemTransaction());
    }

    @Test
    void sameNamedColumnsInDifferentTablesAreFilteredIndependently() {
        SpannerChangeStreamRecordFilter filter = filter(null, null, "orders\\.status", null, false);
        DataChangeRecord orders =
                record(
                        "orders",
                        columns(id(), column("status", "{\"code\":\"STRING\"}", false, 2)),
                        mods("{\"status\":\"open\"}"));
        DataChangeRecord audit =
                record(
                        "audit_orders",
                        columns(id(), column("status", "{\"code\":\"STRING\"}", false, 2)),
                        mods("{\"status\":\"open\"}"));

        assertThat(filter.filter(orders).getRecord().getMods().get(0).getNewValuesJson())
                .contains("{\"status\":\"open\"}");
        assertThat(filter.filter(audit).getRecord().getMods().get(0).getNewValuesJson())
                .contains("{}");
    }

    @Test
    void columnPatternThatKeepsEveryReportedColumnReturnsTheOriginalRecord() {
        DataChangeRecord record =
                record(
                        "orders",
                        columns(id(), column("status", "{\"code\":\"STRING\"}", false, 2)),
                        mods("{\"status\":\"open\"}"));

        assertThat(filter(null, null, "orders\\..*", null, false).filter(record).getRecord())
                .isSameAs(record);
    }

    @Test
    void anyPatternInAListCanMatch() {
        DataChangeRecord orders = record("orders", columns(id()), mods("{}"));
        SpannerChangeStreamRecordFilter filter =
                new SpannerChangeStreamRecordFilter(
                        Arrays.asList(Pattern.compile("audit"), Pattern.compile("orders")),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        false);

        assertThat(filter.filter(orders).getDisposition())
                .isEqualTo(SpannerChangeStreamRecordFilter.Result.Disposition.DELIVER);
    }

    @Test
    void primaryKeysRemainInMetadataKeysAndUnexpectedValueObjects() {
        DataChangeRecord record =
                record(
                        "orders",
                        columns(id(), column("secret", "{\"code\":\"STRING\"}", false, 2)),
                        Collections.singletonList(
                                new Mod("{\"id\":1}", "{\"id\":1,\"secret\":\"hidden\"}", null)));

        SpannerChangeStreamRecordFilter.Result result =
                filter(null, null, null, "orders\\..*", false).filter(record);

        assertThat(result.getRecord().getColumnTypes())
                .extracting(DataChangeRecord.ColumnType::getName)
                .containsExactly("id");
        assertThat(result.getRecord().getMods().get(0).getKeysJson()).isEqualTo("{\"id\":1}");
        assertThat(result.getRecord().getMods().get(0).getNewValuesJson()).contains("{\"id\":1}");
        assertThat(result.getRemovedColumnOccurrences()).isEqualTo(2);
    }

    @Test
    void emptyProjectionIsDeliveredByDefaultAndCanBeSkippedExplicitly() {
        DataChangeRecord record =
                record(
                        "orders",
                        columns(id(), column("secret", "{\"code\":\"STRING\"}", false, 2)),
                        mods("{\"secret\":\"hidden\"}"));

        SpannerChangeStreamRecordFilter.Result delivered =
                filter(null, null, "orders\\.visible", null, false).filter(record);
        assertThat(delivered.getDisposition())
                .isEqualTo(SpannerChangeStreamRecordFilter.Result.Disposition.DELIVER);
        assertThat(delivered.getRecord().getMods().get(0).getNewValuesJson()).contains("{}");
        assertThat(delivered.getRemovedColumnOccurrences()).isEqualTo(2);

        SpannerChangeStreamRecordFilter.Result skipped =
                filter(null, null, "orders\\.visible", null, true).filter(record);
        assertThat(skipped.getDisposition())
                .isEqualTo(
                        SpannerChangeStreamRecordFilter.Result.Disposition.SKIPPED_WITHOUT_CHANGE);
        assertThat(skipped.getRemovedColumnOccurrences()).isZero();
    }

    @Test
    void absentAndExplicitNullValuesRemainDistinct() {
        DataChangeRecord record =
                record(
                        "orders",
                        columns(id(), column("secret", "{\"code\":\"STRING\"}", false, 2)),
                        Collections.singletonList(new Mod("{\"id\":1}", "null", null)));

        SpannerChangeStreamRecordFilter.Result result =
                filter(null, null, null, "orders\\.secret", true).filter(record);

        assertThat(result.getDisposition())
                .isEqualTo(SpannerChangeStreamRecordFilter.Result.Disposition.DELIVER);
        assertThat(result.getRecord().getMods().get(0).getNewValuesJson()).contains("null");
        assertThat(result.getRecord().getMods().get(0).getOldValuesJson()).isEmpty();
        assertThat(result.getRemovedColumnOccurrences()).isEqualTo(1);
    }

    @Test
    void disabledFilterReturnsTheDecodedRecordUnchanged() {
        DataChangeRecord record = record("orders", columns(id()), mods("{}"));

        assertThat(SpannerChangeStreamRecordFilter.none().filter(record).getRecord())
                .isSameAs(record);
    }

    private static SpannerChangeStreamRecordFilter filter(
            String tableInclude,
            String tableExclude,
            String columnInclude,
            String columnExclude,
            boolean skip) {
        return new SpannerChangeStreamRecordFilter(
                patterns(tableInclude),
                patterns(tableExclude),
                patterns(columnInclude),
                patterns(columnExclude),
                skip);
    }

    private static List<Pattern> patterns(String expression) {
        return expression == null
                ? Collections.emptyList()
                : Collections.singletonList(Pattern.compile(expression));
    }

    private static List<DataChangeRecord.ColumnType> columns(
            DataChangeRecord.ColumnType... columns) {
        return Arrays.asList(columns);
    }

    private static List<Mod> mods(String newValues) {
        return Collections.singletonList(new Mod("{\"id\":1}", newValues, null));
    }

    private static DataChangeRecord.ColumnType id() {
        return column("id", "{\"code\":\"INT64\"}", true, 1);
    }

    private static DataChangeRecord.ColumnType column(
            String name, String descriptor, boolean primaryKey, long ordinal) {
        return new DataChangeRecord.ColumnType(name, descriptor, primaryKey, ordinal);
    }

    private static DataChangeRecord record(
            String table, List<DataChangeRecord.ColumnType> columns, List<Mod> mods) {
        return new DataChangeRecord(
                Instant.parse("2026-08-12T00:00:00Z"),
                "1",
                "tx",
                true,
                table,
                columns,
                mods,
                ModType.UPDATE,
                ValueCaptureType.NEW_ROW_AND_OLD_VALUES,
                1,
                1,
                "tag",
                false);
    }
}
