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

package io.github.flink.gcp.connector.bigtable.source.changestream.reader;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecordAdapter.ChangeStreamRecordBuilder;
import com.google.cloud.bigtable.data.v2.models.DefaultChangeStreamRecordAdapter;
import com.google.cloud.bigtable.data.v2.models.Range;
import com.google.cloud.bigtable.data.v2.models.Value;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutation;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutationFilter;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BigtableChangeStreamMutationConverterTest {

    @Test
    void mapsEverySdkFieldEntryValueAndTimestampBound() throws Exception {
        Instant commitTime = Instant.parse("2026-08-14T00:00:00.123456789Z");
        Instant lowWatermark = Instant.parse("2026-08-13T23:59:00.987654321Z");
        ChangeStreamRecordBuilder<ChangeStreamRecord> builder =
                new DefaultChangeStreamRecordAdapter().createChangeStreamRecordBuilder();
        builder.startUserMutation(ByteString.copyFromUtf8("row"), "cluster", commitTime, 7);
        builder.startCell("set", ByteString.copyFromUtf8("q1"), 11L);
        builder.cellValue(ByteString.copyFromUtf8("value"));
        builder.finishCell();
        builder.deleteCells(
                "delete", ByteString.copyFromUtf8("q2"), Range.TimestampRange.create(12L, 13L));
        builder.deleteCells(
                "delete", ByteString.copyFromUtf8("q3"), Range.TimestampRange.unbounded());
        builder.deleteFamily("family");
        builder.addToCell(
                "aggregate",
                Value.rawValue(ByteString.copyFromUtf8("q4")),
                Value.rawTimestamp(14L),
                Value.intValue(15L));
        builder.mergeToCell(
                "aggregate",
                Value.rawValue(ByteString.copyFromUtf8("q5")),
                Value.rawTimestamp(16L),
                Value.rawValue(ByteString.copyFromUtf8("input")));

        BigtableChangeStreamMutation mutation =
                BigtableChangeStreamMutationConverter.convert(
                        (ChangeStreamMutation)
                                builder.finishChangeStreamMutation("token", lowWatermark));

        assertThat(mutation.getRowKey()).isEqualTo(ByteString.copyFromUtf8("row"));
        assertThat(mutation.getType()).isEqualTo(BigtableChangeStreamMutation.MutationType.USER);
        assertThat(mutation.getSourceClusterId()).isEqualTo("cluster");
        assertThat(mutation.getCommitTime()).isEqualTo(commitTime);
        assertThat(mutation.getTieBreaker()).isEqualTo(7);
        assertThat(mutation.getToken()).isEqualTo("token");
        assertThat(mutation.getEstimatedLowWatermarkTime()).isEqualTo(lowWatermark);
        assertThat(mutation.getEntries())
                .containsExactly(
                        new BigtableChangeStreamMutation.SetCellEntry(
                                "set",
                                ByteString.copyFromUtf8("q1"),
                                11L,
                                ByteString.copyFromUtf8("value")),
                        new BigtableChangeStreamMutation.DeleteCellsEntry(
                                "delete",
                                ByteString.copyFromUtf8("q2"),
                                new BigtableChangeStreamMutation.TimestampRange(
                                        BigtableChangeStreamMutation.TimestampBound.closed(12L),
                                        BigtableChangeStreamMutation.TimestampBound.open(13L))),
                        new BigtableChangeStreamMutation.DeleteCellsEntry(
                                "delete",
                                ByteString.copyFromUtf8("q3"),
                                new BigtableChangeStreamMutation.TimestampRange(
                                        BigtableChangeStreamMutation.TimestampBound.unbounded(),
                                        BigtableChangeStreamMutation.TimestampBound.unbounded())),
                        new BigtableChangeStreamMutation.DeleteFamilyEntry("family"),
                        new BigtableChangeStreamMutation.AddToCellEntry(
                                "aggregate",
                                new BigtableChangeStreamMutation.RawValue(
                                        ByteString.copyFromUtf8("q4")),
                                new BigtableChangeStreamMutation.RawTimestamp(14L),
                                new BigtableChangeStreamMutation.Int64Value(15L)),
                        new BigtableChangeStreamMutation.MergeToCellEntry(
                                "aggregate",
                                new BigtableChangeStreamMutation.RawValue(
                                        ByteString.copyFromUtf8("q5")),
                                new BigtableChangeStreamMutation.RawTimestamp(16L),
                                new BigtableChangeStreamMutation.RawValue(
                                        ByteString.copyFromUtf8("input"))));
    }

    @Test
    void filtersSdkEntriesBeforeConvertingTheRetainedProjection() throws Exception {
        Instant commitTime = Instant.parse("2026-08-14T01:00:00.123456789Z");
        Instant lowWatermark = Instant.parse("2026-08-14T00:59:00.987654321Z");
        ChangeStreamRecordBuilder<ChangeStreamRecord> builder = builder(commitTime);
        builder.startCell("other", ByteString.copyFromUtf8("a"), 1L);
        builder.cellValue(ByteString.copyFromUtf8("value"));
        builder.finishCell();
        builder.deleteCells(
                "selected", ByteString.copyFromUtf8("a"), Range.TimestampRange.create(2L, 3L));
        builder.deleteFamily("selected");
        builder.addToCell(
                "other",
                Value.rawValue(ByteString.copyFromUtf8("a")),
                Value.rawTimestamp(4L),
                Value.intValue(5L));
        builder.mergeToCell(
                "selected",
                Value.rawValue(ByteString.copyFromUtf8("a")),
                Value.rawTimestamp(6L),
                Value.rawValue(ByteString.copyFromUtf8("input")));

        BigtableChangeStreamMutationConverter.Result result =
                BigtableChangeStreamMutationConverter.convertFiltered(
                        finish(builder, lowWatermark), familyInclude("selected", false));

        assertThat(result.isSkipped()).isFalse();
        assertThat(result.getRemovedEntries()).isEqualTo(2);
        BigtableChangeStreamMutation mutation = result.getMutation();
        assertThat(mutation.getRowKey()).isEqualTo(ByteString.copyFromUtf8("row"));
        assertThat(mutation.getSourceClusterId()).isEqualTo("cluster");
        assertThat(mutation.getCommitTime()).isEqualTo(commitTime);
        assertThat(mutation.getTieBreaker()).isEqualTo(7);
        assertThat(mutation.getToken()).isEqualTo("token");
        assertThat(mutation.getEstimatedLowWatermarkTime()).isEqualTo(lowWatermark);
        assertThat(mutation.getEntries())
                .containsExactly(
                        new BigtableChangeStreamMutation.DeleteCellsEntry(
                                "selected",
                                ByteString.copyFromUtf8("a"),
                                new BigtableChangeStreamMutation.TimestampRange(
                                        BigtableChangeStreamMutation.TimestampBound.closed(2L),
                                        BigtableChangeStreamMutation.TimestampBound.open(3L))),
                        new BigtableChangeStreamMutation.DeleteFamilyEntry("selected"),
                        new BigtableChangeStreamMutation.MergeToCellEntry(
                                "selected",
                                new BigtableChangeStreamMutation.RawValue(
                                        ByteString.copyFromUtf8("a")),
                                new BigtableChangeStreamMutation.RawTimestamp(6L),
                                new BigtableChangeStreamMutation.RawValue(
                                        ByteString.copyFromUtf8("input"))));
    }

    @Test
    void allFilteredEntriesSkipWithoutMaterializingAPublicMutation() throws Exception {
        Instant commitTime = Instant.parse("2026-08-14T02:00:00Z");
        ChangeStreamRecordBuilder<ChangeStreamRecord> builder = builder(commitTime);
        builder.deleteFamily("excluded");

        BigtableChangeStreamMutationConverter.Result result =
                BigtableChangeStreamMutationConverter.convertFiltered(
                        finish(builder, Instant.parse("2026-08-14T01:59:00Z")),
                        familyInclude("selected", true));

        assertThat(result.isSkipped()).isTrue();
        assertThat(result.getRemovedEntries()).isEqualTo(1);
        assertThatThrownBy(result::getMutation)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("skipped result");
    }

    @Test
    void originallyEmptyMutationRemainsDeliverableWhenFilteredEmptyMutationsAreSkipped()
            throws Exception {
        Instant lowWatermark = Instant.parse("2026-08-14T02:14:00Z");

        BigtableChangeStreamMutationConverter.Result result =
                BigtableChangeStreamMutationConverter.convertFiltered(
                        finish(builder(Instant.parse("2026-08-14T02:15:00Z")), lowWatermark),
                        familyInclude("selected", true));

        assertThat(result.isSkipped()).isFalse();
        assertThat(result.getRemovedEntries()).isZero();
        assertThat(result.getMutation().getEntries()).isEmpty();
        assertThat(result.getMutation().getToken()).isEqualTo("token");
        assertThat(result.getMutation().getEstimatedLowWatermarkTime()).isEqualTo(lowWatermark);
    }

    @Test
    void qualifierProjectionCoversEveryEntryKindAndKeepsFamilyDeletes() throws Exception {
        ChangeStreamRecordBuilder<ChangeStreamRecord> builder =
                builder(Instant.parse("2026-08-14T02:30:00Z"));
        builder.startCell("selected", ByteString.copyFromUtf8("a"), 1L);
        builder.cellValue(ByteString.copyFromUtf8("value"));
        builder.finishCell();
        builder.deleteCells(
                "selected", ByteString.copyFromUtf8("a"), Range.TimestampRange.unbounded());
        builder.deleteFamily("selected");
        builder.addToCell(
                "selected",
                Value.rawValue(ByteString.copyFromUtf8("a")),
                Value.rawTimestamp(2L),
                Value.intValue(3L));
        builder.mergeToCell(
                "selected",
                Value.rawValue(ByteString.copyFromUtf8("b")),
                Value.rawTimestamp(4L),
                Value.rawValue(ByteString.copyFromUtf8("input")));

        BigtableChangeStreamMutationConverter.Result result =
                BigtableChangeStreamMutationConverter.convertFiltered(
                        finish(builder, Instant.parse("2026-08-14T02:29:00Z")),
                        qualifierInclude("selected:YQ=="));

        assertThat(result.getRemovedEntries()).isEqualTo(1);
        assertThat(result.getMutation().getEntries())
                .extracting(BigtableChangeStreamMutation.Entry::getClass)
                .containsExactly(
                        BigtableChangeStreamMutation.SetCellEntry.class,
                        BigtableChangeStreamMutation.DeleteCellsEntry.class,
                        BigtableChangeStreamMutation.DeleteFamilyEntry.class,
                        BigtableChangeStreamMutation.AddToCellEntry.class);
    }

    @Test
    void allFilteredEntriesMaterializeOnlyAnEmptyMutationWhenSkipIsDisabled() throws Exception {
        Instant lowWatermark = Instant.parse("2026-08-14T02:59:00Z");
        ChangeStreamRecordBuilder<ChangeStreamRecord> builder =
                builder(Instant.parse("2026-08-14T03:00:00Z"));
        builder.deleteFamily("excluded");

        BigtableChangeStreamMutationConverter.Result result =
                BigtableChangeStreamMutationConverter.convertFiltered(
                        finish(builder, lowWatermark), familyInclude("selected", false));

        assertThat(result.isSkipped()).isFalse();
        assertThat(result.getRemovedEntries()).isEqualTo(1);
        assertThat(result.getMutation().getEntries()).isEmpty();
        assertThat(result.getMutation().getToken()).isEqualTo("token");
        assertThat(result.getMutation().getEstimatedLowWatermarkTime()).isEqualTo(lowWatermark);
    }

    private static ChangeStreamRecordBuilder<ChangeStreamRecord> builder(Instant commitTime) {
        ChangeStreamRecordBuilder<ChangeStreamRecord> builder =
                new DefaultChangeStreamRecordAdapter().createChangeStreamRecordBuilder();
        builder.startUserMutation(ByteString.copyFromUtf8("row"), "cluster", commitTime, 7);
        return builder;
    }

    private static ChangeStreamMutation finish(
            ChangeStreamRecordBuilder<ChangeStreamRecord> builder, Instant lowWatermark) {
        return (ChangeStreamMutation) builder.finishChangeStreamMutation("token", lowWatermark);
    }

    private static BigtableChangeStreamMutationFilter familyInclude(String family, boolean skip) {
        return new BigtableChangeStreamMutationFilter(
                Collections.singletonList(Pattern.compile(family)),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                skip);
    }

    private static BigtableChangeStreamMutationFilter qualifierInclude(String qualifiedColumn) {
        return new BigtableChangeStreamMutationFilter(
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(Pattern.compile(qualifiedColumn)),
                Collections.emptyList(),
                false);
    }
}
