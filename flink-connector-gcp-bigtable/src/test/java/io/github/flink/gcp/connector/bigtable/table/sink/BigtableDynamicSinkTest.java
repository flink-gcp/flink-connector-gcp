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

package io.github.flink.gcp.connector.bigtable.table.sink;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.types.RowKind;

import io.github.flink.gcp.connector.bigtable.table.InsertOnlyInputMode;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/**
 * What the sink tells the planner, and what survives a {@code copy()}.
 *
 * <p>In this package rather than beside the factory's other tests because {@link
 * CrossVersionChangelogMode} is package-private, as the seam beside its only caller should be —
 * asserting the mode against a hand-built {@code ChangelogMode} instead is what is not portable,
 * since {@code ChangelogMode.upsert(boolean)} does not exist on the 1.20 LTS build.
 */
class BigtableDynamicSinkTest {

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("rowkey", DataTypes.STRING()),
                    Column.physical(
                            "cf1", DataTypes.ROW(DataTypes.FIELD("q1", DataTypes.STRING()))));

    private static ResolvedSchema withRowKeyAsPrimaryKey() {
        return new ResolvedSchema(
                SCHEMA.getColumns(),
                Collections.emptyList(),
                UniqueConstraint.primaryKey("pk", Arrays.asList("rowkey")));
    }

    private static Map<String, String> options() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "bigtable");
        options.put("project", "my-project");
        options.put("instance", "my-instance");
        options.put("table", "my-table");
        return options;
    }

    private static DynamicTableSink sink(ResolvedSchema schema) {
        return FactoryMocks.createTableSink(schema, options());
    }

    private static DynamicTableSink sink(
            ResolvedSchema schema, InsertOnlyInputMode insertOnlyInputMode) {
        Map<String, String> configured = options();
        configured.put("sink.insert-only-input-mode", insertOnlyInputMode.toString());
        return FactoryMocks.createTableSink(schema, configured);
    }

    @Test
    void deletesCarryTheKeyAloneOnlyWhenAPrimaryKeyIsDeclared() {
        ChangelogMode withoutKey = sink(SCHEMA).getChangelogMode(ChangelogMode.all());
        ChangelogMode withKey =
                sink(withRowKeyAsPrimaryKey()).getChangelogMode(ChangelogMode.all());

        // The mode itself, not just its contained kinds: on 2.x a key-only-deletes upsert and a
        // full-deletes one carry the same three kinds and compare unequal. Both expectations go
        // through the seam rather than naming ChangelogMode.upsert(boolean), which the 1.20 build
        // does not have — there the two collapse to one value, which is the truth on that build:
        // 1.20 completes the row before every delete regardless.
        //
        // #470 is what the first of these pins. With no PRIMARY KEY the planner keys its upserts
        // on whatever the query is unique by, which need not be the row key, so a delete carrying
        // that key alone would reach the writer with the row-key column null.
        assertThat(withoutKey).isEqualTo(CrossVersionChangelogMode.upsert(false));
        assertThat(withKey).isEqualTo(CrossVersionChangelogMode.upsert(true));
        assertThat(withoutKey.getContainedKinds())
                .containsExactlyInAnyOrder(RowKind.INSERT, RowKind.UPDATE_AFTER, RowKind.DELETE);
        assertThat(withKey.getContainedKinds())
                .containsExactlyInAnyOrder(RowKind.INSERT, RowKind.UPDATE_AFTER, RowKind.DELETE);
    }

    @Test
    void anInsertOnlyQueryIsUpsertByDefault() {
        assertThat(sink(SCHEMA).getChangelogMode(ChangelogMode.insertOnly()))
                .as("without a PRIMARY KEY")
                .isEqualTo(CrossVersionChangelogMode.upsert(false));
        assertThat(sink(withRowKeyAsPrimaryKey()).getChangelogMode(ChangelogMode.insertOnly()))
                .as("with the row key declared as PRIMARY KEY")
                .isEqualTo(CrossVersionChangelogMode.upsert(true));
    }

    @Test
    void insertOnlyCompatibilityModeNarrowsAnInsertOnlyQuery() {
        assertThat(
                        sink(SCHEMA, InsertOnlyInputMode.INSERT_ONLY)
                                .getChangelogMode(ChangelogMode.insertOnly()))
                .as("without a PRIMARY KEY")
                .isEqualTo(ChangelogMode.insertOnly());
        assertThat(
                        sink(withRowKeyAsPrimaryKey(), InsertOnlyInputMode.INSERT_ONLY)
                                .getChangelogMode(ChangelogMode.insertOnly()))
                .as("with the row key declared as PRIMARY KEY")
                .isEqualTo(ChangelogMode.insertOnly());
    }

    @Test
    void insertOnlyCompatibilityModeDoesNotNarrowAnUpdatingQuery() {
        assertThat(
                        sink(SCHEMA, InsertOnlyInputMode.INSERT_ONLY)
                                .getChangelogMode(ChangelogMode.all()))
                .isEqualTo(CrossVersionChangelogMode.upsert(false));
        assertThat(
                        sink(withRowKeyAsPrimaryKey(), InsertOnlyInputMode.INSERT_ONLY)
                                .getChangelogMode(ChangelogMode.all()))
                .isEqualTo(CrossVersionChangelogMode.upsert(true));
    }

    @Test
    void listsTheCellTimestampMetadataAtMicrosecondPrecision() {
        assertThat(((SupportsWritingMetadata) sink(SCHEMA)).listWritableMetadata())
                .containsExactly(entry("timestamp", DataTypes.TIMESTAMP_LTZ(6).nullable()));
    }

    @Test
    void aCopyKeepsTheSelectedMetadataAndTruncationPolicy() {
        Map<String, String> truncatingOptions = options();
        truncatingOptions.put("sink.cell-timestamp.truncate-to-millis", "true");
        SupportsWritingMetadata selected =
                (SupportsWritingMetadata) FactoryMocks.createTableSink(SCHEMA, truncatingOptions);
        selected.applyWritableMetadata(
                Collections.singletonList("timestamp"),
                DataTypes.ROW(
                        DataTypes.FIELD("rowkey", DataTypes.STRING()),
                        DataTypes.FIELD(
                                "cf1", DataTypes.ROW(DataTypes.FIELD("q1", DataTypes.STRING()))),
                        DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP_LTZ(6))));
        DynamicTableSink original = (DynamicTableSink) selected;

        assertThat(original.copy()).isEqualTo(original).hasSameHashCodeAs(original);

        DynamicTableSink unselected = FactoryMocks.createTableSink(SCHEMA, truncatingOptions);
        assertThat(original).isNotEqualTo(unselected);

        SupportsWritingMetadata preserving = (SupportsWritingMetadata) sink(SCHEMA);
        preserving.applyWritableMetadata(Collections.singletonList("timestamp"), DataTypes.ROW());
        assertThat(original).isNotEqualTo(preserving);
    }

    @Test
    void twoSinksThatDifferOnlyInTheConsumedTypeAreTheSameSink() {
        // What pins the decision to discard consumedDataType. The test above cannot: its two
        // unequal sinks differ in their truncation policy as well, so it stays green against a
        // consumed type that is kept, compared in equals and carried through copy() — measured,
        // and only this test fails.
        SupportsWritingMetadata described = (SupportsWritingMetadata) sink(SCHEMA);
        described.applyWritableMetadata(
                Collections.singletonList("timestamp"),
                DataTypes.ROW(
                        DataTypes.FIELD("rowkey", DataTypes.STRING()),
                        DataTypes.FIELD(
                                "cf1", DataTypes.ROW(DataTypes.FIELD("q1", DataTypes.STRING()))),
                        DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP_LTZ(6))));
        SupportsWritingMetadata undescribed = (SupportsWritingMetadata) sink(SCHEMA);
        undescribed.applyWritableMetadata(Collections.singletonList("timestamp"), DataTypes.ROW());

        assertThat((DynamicTableSink) described)
                .isEqualTo(undescribed)
                .hasSameHashCodeAs(undescribed);
    }

    @Test
    void theSelectionSurvivesTheCallerReusingItsList() {
        // The list handed over is live plan state: WritingMetadataSpec passes its own field, which
        // feeds its equals and hashCode and is what a compiled plan serializes. The planner does
        // not mutate it, so this pins the copy against the sink doing so rather than against a
        // behaviour anyone has seen — but without a copy the sink would be aliasing that state.
        SupportsWritingMetadata sink = (SupportsWritingMetadata) sink(SCHEMA);
        List<String> keys = new ArrayList<>(Collections.singletonList("timestamp"));
        sink.applyWritableMetadata(keys, DataTypes.ROW());

        keys.clear();

        SupportsWritingMetadata reference = (SupportsWritingMetadata) sink(SCHEMA);
        reference.applyWritableMetadata(Collections.singletonList("timestamp"), DataTypes.ROW());
        assertThat((DynamicTableSink) sink).isEqualTo(reference);
    }

    @Test
    void aMetadataKeyThePlannerCannotHaveSentIsRejectedRatherThanDropped() {
        // Unreachable through a DDL, which DynamicSinkUtils.validateAndApplyMetadata rejects
        // first. Reachable through a restored compiled plan, whose WritingMetadataSpec applies the
        // keys it was serialized with and runs no validation — so a plan compiled against a build
        // that offered a key this one does not lands here. Selecting the key out with contains(),
        // as this sink used to, would have written every row without that column instead.
        SupportsWritingMetadata sink = (SupportsWritingMetadata) sink(SCHEMA);

        assertThatThrownBy(
                        () ->
                                sink.applyWritableMetadata(
                                        Collections.singletonList("cell-visibility"),
                                        DataTypes.ROW()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cell-visibility");
    }

    @Test
    void theCopyKeepsWhatTheDdlAnsweredAboutDeletes() {
        // A field dropped from copy() resets the mode silently, and the sink's own equality would
        // not show it either unless that field is compared. Both repetitions are asserted here
        // because nothing in the build checks that the four field lists in this class agree.
        DynamicTableSink withKey = sink(withRowKeyAsPrimaryKey());
        DynamicTableSink withoutKey = sink(SCHEMA);

        assertThat(withKey.copy()).isEqualTo(withKey).hasSameHashCodeAs(withKey);
        assertThat(withoutKey.copy()).isEqualTo(withoutKey).hasSameHashCodeAs(withoutKey);
        assertThat(withKey).isNotEqualTo(withoutKey);
        assertThat(withKey.copy().getChangelogMode(ChangelogMode.all()))
                .isEqualTo(CrossVersionChangelogMode.upsert(true));
        assertThat(withoutKey.copy().getChangelogMode(ChangelogMode.all()))
                .isEqualTo(CrossVersionChangelogMode.upsert(false));
    }

    @Test
    void theCopyKeepsTheInsertOnlyCompatibilityMode() {
        DynamicTableSink compatibility = sink(SCHEMA, InsertOnlyInputMode.INSERT_ONLY);
        DynamicTableSink defaultMode = sink(SCHEMA);

        assertThat(compatibility.copy())
                .isEqualTo(compatibility)
                .hasSameHashCodeAs(compatibility)
                .isNotEqualTo(defaultMode);
        assertThat(compatibility.copy().getChangelogMode(ChangelogMode.insertOnly()))
                .isEqualTo(ChangelogMode.insertOnly());
    }
}
