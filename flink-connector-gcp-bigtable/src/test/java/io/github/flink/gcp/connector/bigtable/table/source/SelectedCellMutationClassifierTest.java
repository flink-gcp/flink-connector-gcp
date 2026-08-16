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

import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecordAdapter.ChangeStreamRecordBuilder;
import com.google.cloud.bigtable.data.v2.models.DefaultChangeStreamRecordAdapter;
import com.google.cloud.bigtable.data.v2.models.Range;
import com.google.cloud.bigtable.data.v2.models.Value;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamMutation;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.TestChangeStreamMutations;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SelectedCellMutationClassifierTest {

    private static final String FAMILY = "state";
    private static final ByteString QUALIFIER = ByteString.copyFromUtf8("current");

    private final SelectedCellMutationClassifier classifier =
            new SelectedCellMutationClassifier(FAMILY, QUALIFIER, "cluster-1");

    @Test
    void recognizesCanonicalColumnAndFamilyUpserts() throws Exception {
        ChangeStreamMutation columnUpsert =
                mutation(
                        builder -> {
                            builder.deleteCells(
                                    FAMILY, QUALIFIER, Range.TimestampRange.unbounded());
                            set(builder, "one");
                        });
        ChangeStreamMutation familyUpsert =
                mutation(
                        builder -> {
                            builder.deleteFamily(FAMILY);
                            set(builder, "two");
                        });

        assertThat(classifier.classify(columnUpsert).getKind())
                .isEqualTo(SelectedCellMutationClassifier.Kind.UPSERT);
        assertThat(classifier.classify(columnUpsert).getValue().toStringUtf8()).isEqualTo("one");
        assertThat(classifier.classify(familyUpsert).getValue().toStringUtf8()).isEqualTo("two");
    }

    @Test
    void recognizesCanonicalDeletesAndIgnoresUnrelatedEntries() throws Exception {
        ChangeStreamMutation columnDelete =
                mutation(
                        builder ->
                                builder.deleteCells(
                                        FAMILY, QUALIFIER, Range.TimestampRange.unbounded()));
        ChangeStreamMutation familyDelete = mutation(builder -> builder.deleteFamily(FAMILY));
        ChangeStreamMutation unrelated =
                mutation(
                        builder -> {
                            builder.deleteFamily("other");
                            builder.startCell(FAMILY, ByteString.copyFromUtf8("other"), 1L);
                            builder.cellValue(ByteString.copyFromUtf8("value"));
                            builder.finishCell();
                        });

        assertThat(classifier.classify(columnDelete).getKind())
                .isEqualTo(SelectedCellMutationClassifier.Kind.DELETE);
        assertThat(classifier.classify(familyDelete).getKind())
                .isEqualTo(SelectedCellMutationClassifier.Kind.DELETE);
        assertThat(classifier.classify(unrelated).getKind())
                .isEqualTo(SelectedCellMutationClassifier.Kind.UNRELATED);
    }

    @Test
    void rejectsStandaloneMultipleAndOutOfOrderSets() {
        assertProtocolFailure(mutation(builder -> set(builder, "one")), "must follow exactly one");
        assertProtocolFailure(
                mutation(
                        builder -> {
                            builder.deleteFamily(FAMILY);
                            set(builder, "one");
                            set(builder, "two");
                        }),
                "must follow exactly one");
        assertProtocolFailure(
                mutation(
                        builder -> {
                            builder.deleteFamily(FAMILY);
                            set(builder, "one");
                            builder.deleteFamily(FAMILY);
                        }),
                "after the cell is set");
    }

    @Test
    void rejectsPartialDeletesAggregatesAndWrongClusters() {
        assertProtocolFailure(
                mutation(
                        builder ->
                                builder.deleteCells(
                                        FAMILY, QUALIFIER, Range.TimestampRange.create(1L, 2L))),
                "timestamp-bounded delete");
        assertProtocolFailure(
                mutation(
                        builder ->
                                builder.addToCell(
                                        FAMILY,
                                        Value.rawValue(QUALIFIER),
                                        Value.rawTimestamp(1L),
                                        Value.intValue(1L))),
                "AddToCell");
        assertProtocolFailure(
                mutation(
                        builder ->
                                builder.mergeToCell(
                                        FAMILY,
                                        Value.rawValue(QUALIFIER),
                                        Value.rawTimestamp(1L),
                                        Value.rawValue(ByteString.copyFromUtf8("aggregate")))),
                "MergeToCell");

        SelectedCellMutationClassifier wrongCluster =
                new SelectedCellMutationClassifier(FAMILY, QUALIFIER, "cluster-2");
        assertThatThrownBy(
                        () ->
                                wrongCluster.classify(
                                        mutation(builder -> builder.deleteFamily(FAMILY))))
                .hasMessageContaining("not configured cluster 'cluster-2'");
    }

    @Test
    void rejectsGarbageCollectionThatAffectsTheSelectedCell() {
        ChangeStreamRecordBuilder<ChangeStreamRecord> builder =
                new DefaultChangeStreamRecordAdapter().createChangeStreamRecordBuilder();
        builder.startGcMutation(
                ByteString.copyFromUtf8("row-1"), Instant.parse("2026-08-13T00:00:00Z"), 0);
        builder.deleteCells(FAMILY, QUALIFIER, Range.TimestampRange.unbounded());
        ChangeStreamMutation mutation =
                TestChangeStreamMutations.convert(
                        (com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation)
                                builder.finishChangeStreamMutation(
                                        "token", Instant.parse("2026-08-12T23:59:00Z")));

        assertProtocolFailure(mutation, "garbage-collection mutation");
    }

    private static void assertProtocolFailure(ChangeStreamMutation mutation, String detail) {
        SelectedCellMutationClassifier classifier =
                new SelectedCellMutationClassifier(FAMILY, QUALIFIER, "cluster-1");
        assertThatThrownBy(() -> classifier.classify(mutation))
                .hasMessageContaining("Invalid Bigtable selected-cell producer protocol")
                .hasMessageContaining(detail);
    }

    private static ChangeStreamMutation mutation(
            Consumer<ChangeStreamRecordBuilder<ChangeStreamRecord>> entries) {
        ChangeStreamRecordBuilder<ChangeStreamRecord> builder =
                new DefaultChangeStreamRecordAdapter().createChangeStreamRecordBuilder();
        builder.startUserMutation(
                ByteString.copyFromUtf8("row-1"),
                "cluster-1",
                Instant.parse("2026-08-13T00:00:00Z"),
                0);
        entries.accept(builder);
        return TestChangeStreamMutations.convert(
                (com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation)
                        builder.finishChangeStreamMutation(
                                "token", Instant.parse("2026-08-12T23:59:00Z")));
    }

    private static void set(ChangeStreamRecordBuilder<ChangeStreamRecord> builder, String value) {
        builder.startCell(FAMILY, QUALIFIER, 1L);
        builder.cellValue(ByteString.copyFromUtf8(value));
        builder.finishCell();
    }
}
