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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutation;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

class BigtableChangeStreamMutationToRowDataConverterTest {

    @Test
    void readableMetadataHasStableTypesAndOrderWithoutProtocolState() {
        assertThat(ChangeStreamReadableMetadata.listAll())
                .containsExactly(
                        entry("mutation-type", DataTypes.STRING().notNull()),
                        entry("source-cluster-id", DataTypes.STRING()),
                        entry("commit-timestamp", DataTypes.TIMESTAMP_LTZ(9).notNull()),
                        entry("tie-breaker", DataTypes.INT().notNull()),
                        entry("estimated-low-watermark", DataTypes.TIMESTAMP_LTZ(9).notNull()))
                .doesNotContainKeys("token", "continuation-token");
    }

    @Test
    void appendsSelectedMetadataInRequestedOrderAndPreservesNanoseconds() throws Exception {
        Instant commit = Instant.parse("2026-08-13T00:00:00.123456789Z");
        Instant watermark = Instant.parse("2026-08-12T23:59:00.987654321Z");
        BigtableChangeStreamMutation mutation =
                mutationWithMetadata(
                        BigtableChangeStreamMutation.MutationType.USER,
                        "cluster-1",
                        commit,
                        7,
                        "private-token",
                        watermark);
        BigtableChangeStreamMutationRowDataDeserializationSchema schema =
                new BigtableChangeStreamMutationRowDataDeserializationSchema(
                        new ChangeStreamReadableMetadata[] {
                            ChangeStreamReadableMetadata.ESTIMATED_LOW_WATERMARK,
                            ChangeStreamReadableMetadata.MUTATION_TYPE,
                            ChangeStreamReadableMetadata.SOURCE_CLUSTER_ID,
                            ChangeStreamReadableMetadata.COMMIT_TIMESTAMP,
                            ChangeStreamReadableMetadata.TIE_BREAKER
                        },
                        TypeInformation.of(RowData.class));
        List<RowData> output = new ArrayList<>();

        schema.deserialize(mutation, collectingInto(output));

        RowData row = output.get(0);
        assertThat(row.getArity()).isEqualTo(7);
        assertThat(row.getRowKind()).isEqualTo(RowKind.INSERT);
        assertThat(row.getTimestamp(2, 9).toInstant()).isEqualTo(watermark);
        assertThat(row.getString(3).toString()).isEqualTo("USER");
        assertThat(row.getString(4).toString()).isEqualTo("cluster-1");
        assertThat(row.getTimestamp(5, 9).toInstant()).isEqualTo(commit);
        assertThat(row.getInt(6)).isEqualTo(7);
    }

    @Test
    void garbageCollectionMetadataHasNoSourceCluster() throws Exception {
        BigtableChangeStreamMutation mutation =
                mutationWithMetadata(
                        BigtableChangeStreamMutation.MutationType.GARBAGE_COLLECTION,
                        "",
                        Instant.parse("2026-08-13T00:00:00Z"),
                        3,
                        "private-token",
                        Instant.parse("2026-08-12T23:59:00Z"));
        BigtableChangeStreamMutationRowDataDeserializationSchema schema =
                new BigtableChangeStreamMutationRowDataDeserializationSchema(
                        new ChangeStreamReadableMetadata[] {
                            ChangeStreamReadableMetadata.MUTATION_TYPE,
                            ChangeStreamReadableMetadata.SOURCE_CLUSTER_ID
                        },
                        TypeInformation.of(RowData.class));
        List<RowData> output = new ArrayList<>();

        schema.deserialize(mutation, collectingInto(output));

        RowData row = output.get(0);
        assertThat(row.getString(2).toString()).isEqualTo("GARBAGE_COLLECTION");
        assertThat(row.isNullAt(3)).isTrue();
    }

    @Test
    void preservesEveryConnectorEntryKindAndItsOrderedGenericValues() throws Exception {
        BigtableChangeStreamMutation mutation = mutationWithEveryEntryKind();
        RowData envelope = new BigtableChangeStreamMutationToRowDataConverter().convert(mutation);
        assertThat(envelope.getRowKind()).isEqualTo(RowKind.INSERT);
        assertThat(envelope.getBinary(0)).isEqualTo(ByteString.copyFromUtf8("row-1").toByteArray());
        ArrayData entries = envelope.getArray(1);
        assertThat(entries.size()).isEqualTo(6);

        RowData set = entries.getRow(0, 7);
        assertEntry(set, 0, "SET_CELL", "family");
        assertRawValue(set.getRow(3, 3), "qualifier");
        assertRawTimestamp(set.getRow(4, 3), 123_456L);
        assertRawValue(set.getRow(5, 3), "value");
        assertThat(set.isNullAt(6)).isTrue();

        RowData delete = entries.getRow(1, 7);
        assertEntry(delete, 1, "DELETE_CELLS", "family");
        assertRawValue(delete.getRow(3, 3), "qualifier");
        assertThat(delete.isNullAt(4)).isTrue();
        assertThat(delete.isNullAt(5)).isTrue();
        RowData range = delete.getRow(6, 4);
        assertThat(range.getString(0).toString()).isEqualTo("CLOSED");
        assertThat(range.getLong(1)).isEqualTo(10L);
        assertThat(range.getString(2).toString()).isEqualTo("OPEN");
        assertThat(range.getLong(3)).isEqualTo(20L);

        RowData unboundedDelete = entries.getRow(2, 7);
        assertEntry(unboundedDelete, 2, "DELETE_CELLS", "family");
        RowData unboundedRange = unboundedDelete.getRow(6, 4);
        assertThat(unboundedRange.getString(0).toString()).isEqualTo("UNBOUNDED");
        assertThat(unboundedRange.isNullAt(1)).isTrue();
        assertThat(unboundedRange.getString(2).toString()).isEqualTo("UNBOUNDED");
        assertThat(unboundedRange.isNullAt(3)).isTrue();

        RowData family = entries.getRow(3, 7);
        assertEntry(family, 3, "DELETE_FAMILY", "family");
        assertThat(family.isNullAt(3)).isTrue();
        assertThat(family.isNullAt(4)).isTrue();
        assertThat(family.isNullAt(5)).isTrue();
        assertThat(family.isNullAt(6)).isTrue();

        RowData add = entries.getRow(4, 7);
        assertEntry(add, 4, "ADD_TO_CELL", "aggregate");
        assertRawValue(add.getRow(3, 3), "add-qualifier");
        assertRawTimestamp(add.getRow(4, 3), 456_789L);
        assertInt64(add.getRow(5, 3), 7L);

        RowData merge = entries.getRow(5, 7);
        assertEntry(merge, 5, "MERGE_TO_CELL", "aggregate");
        assertRawValue(merge.getRow(3, 3), "merge-qualifier");
        assertRawTimestamp(merge.getRow(4, 3), 987_654L);
        assertRawValue(merge.getRow(5, 3), "fragment");
    }

    @Test
    void theSchemaCollectsOneConvertedMutationAndReportsItsProducedType() throws Exception {
        TypeInformation<RowData> producedType = TypeInformation.of(RowData.class);
        BigtableChangeStreamMutationRowDataDeserializationSchema schema =
                new BigtableChangeStreamMutationRowDataDeserializationSchema(producedType);
        List<RowData> output = new ArrayList<>();

        schema.deserialize(mutationWithEveryEntryKind(), collectingInto(output));

        assertThat(output)
                .singleElement()
                .isEqualTo(
                        new BigtableChangeStreamMutationToRowDataConverter()
                                .convert(mutationWithEveryEntryKind()));
        assertThat(schema.getProducedType()).isSameAs(producedType);
    }

    @Test
    void theSchemaSurvivesJavaSerialization() throws Exception {
        BigtableChangeStreamMutationRowDataDeserializationSchema schema =
                new BigtableChangeStreamMutationRowDataDeserializationSchema(
                        new ChangeStreamReadableMetadata[] {
                            ChangeStreamReadableMetadata.ESTIMATED_LOW_WATERMARK,
                            ChangeStreamReadableMetadata.MUTATION_TYPE
                        },
                        TypeInformation.of(RowData.class));

        BigtableChangeStreamMutationRowDataDeserializationSchema copy = roundTrip(schema);
        List<RowData> output = new ArrayList<>();
        Instant watermark = Instant.parse("2026-08-12T23:59:00.987654321Z");
        copy.deserialize(
                mutationWithMetadata(
                        BigtableChangeStreamMutation.MutationType.USER,
                        "cluster-1",
                        Instant.parse("2026-08-13T00:00:00.123456789Z"),
                        7,
                        "private-token",
                        watermark),
                collectingInto(output));

        assertThat(output)
                .singleElement()
                .satisfies(
                        row -> {
                            assertThat(row.getArity()).isEqualTo(4);
                            assertThat(row.getTimestamp(2, 9).toInstant()).isEqualTo(watermark);
                            assertThat(row.getString(3).toString()).isEqualTo("USER");
                        });
        assertThat(copy.getProducedType()).isEqualTo(schema.getProducedType());
    }

    @Test
    void theConverterSurvivesJavaSerialization() throws Exception {
        BigtableChangeStreamMutationToRowDataConverter converter =
                new BigtableChangeStreamMutationToRowDataConverter();

        BigtableChangeStreamMutationToRowDataConverter copy = roundTrip(converter);

        assertThat(copy.convert(mutationWithEveryEntryKind()))
                .isEqualTo(converter.convert(mutationWithEveryEntryKind()));
    }

    @SuppressWarnings("unchecked")
    private static <T> T roundTrip(T value) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(value);
        }
        try (ObjectInputStream in =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (T) in.readObject();
        }
    }

    private static Collector<RowData> collectingInto(List<RowData> output) {
        return new Collector<RowData>() {
            @Override
            public void collect(RowData record) {
                output.add(record);
            }

            @Override
            public void close() {}
        };
    }

    private static BigtableChangeStreamMutation mutationWithEntries(
            BigtableChangeStreamMutation.Entry... entries) {
        return new BigtableChangeStreamMutation(
                ByteString.copyFromUtf8("row-1"),
                BigtableChangeStreamMutation.MutationType.USER,
                "cluster-1",
                Instant.parse("2026-08-13T00:00:00Z"),
                0,
                "token",
                Instant.parse("2026-08-12T23:59:00Z"),
                java.util.Arrays.asList(entries));
    }

    private static BigtableChangeStreamMutation mutationWithMetadata(
            BigtableChangeStreamMutation.MutationType type,
            String sourceClusterId,
            Instant commitTime,
            int tieBreaker,
            String token,
            Instant estimatedLowWatermark) {
        BigtableChangeStreamMutation.Entry entry = mutationWithEveryEntryKind().getEntries().get(0);
        return new BigtableChangeStreamMutation(
                ByteString.copyFromUtf8("row-1"),
                type,
                sourceClusterId,
                commitTime,
                tieBreaker,
                token,
                estimatedLowWatermark,
                java.util.Collections.singletonList(entry));
    }

    private static BigtableChangeStreamMutation mutationWithEveryEntryKind() {
        return new BigtableChangeStreamMutation(
                ByteString.copyFromUtf8("row-1"),
                BigtableChangeStreamMutation.MutationType.USER,
                "cluster-1",
                Instant.parse("2026-08-13T00:00:00Z"),
                0,
                "token",
                Instant.parse("2026-08-12T23:59:00Z"),
                java.util.Arrays.asList(
                        new BigtableChangeStreamMutation.SetCellEntry(
                                "family",
                                ByteString.copyFromUtf8("qualifier"),
                                123_456L,
                                ByteString.copyFromUtf8("value")),
                        new BigtableChangeStreamMutation.DeleteCellsEntry(
                                "family",
                                ByteString.copyFromUtf8("qualifier"),
                                new BigtableChangeStreamMutation.TimestampRange(
                                        BigtableChangeStreamMutation.TimestampBound.closed(10L),
                                        BigtableChangeStreamMutation.TimestampBound.open(20L))),
                        new BigtableChangeStreamMutation.DeleteCellsEntry(
                                "family",
                                ByteString.copyFromUtf8("qualifier"),
                                new BigtableChangeStreamMutation.TimestampRange(
                                        BigtableChangeStreamMutation.TimestampBound.unbounded(),
                                        BigtableChangeStreamMutation.TimestampBound.unbounded())),
                        new BigtableChangeStreamMutation.DeleteFamilyEntry("family"),
                        new BigtableChangeStreamMutation.AddToCellEntry(
                                "aggregate",
                                new BigtableChangeStreamMutation.RawValue(
                                        ByteString.copyFromUtf8("add-qualifier")),
                                new BigtableChangeStreamMutation.RawTimestamp(456_789L),
                                new BigtableChangeStreamMutation.Int64Value(7L)),
                        new BigtableChangeStreamMutation.MergeToCellEntry(
                                "aggregate",
                                new BigtableChangeStreamMutation.RawValue(
                                        ByteString.copyFromUtf8("merge-qualifier")),
                                new BigtableChangeStreamMutation.RawTimestamp(987_654L),
                                new BigtableChangeStreamMutation.RawValue(
                                        ByteString.copyFromUtf8("fragment")))));
    }

    private static void assertEntry(RowData entry, int index, String kind, String family) {
        assertThat(entry.getInt(0)).isEqualTo(index);
        assertThat(entry.getString(1).toString()).isEqualTo(kind);
        assertThat(entry.getString(2).toString()).isEqualTo(family);
    }

    private static void assertRawValue(RowData value, String expected) {
        assertThat(value.getString(0).toString()).isEqualTo("RAW_VALUE");
        assertThat(value.getBinary(1)).isEqualTo(ByteString.copyFromUtf8(expected).toByteArray());
        assertThat(value.isNullAt(2)).isTrue();
    }

    private static void assertRawTimestamp(RowData value, long expected) {
        assertThat(value.getString(0).toString()).isEqualTo("RAW_TIMESTAMP");
        assertThat(value.isNullAt(1)).isTrue();
        assertThat(value.getLong(2)).isEqualTo(expected);
    }

    private static void assertInt64(RowData value, long expected) {
        assertThat(value.getString(0).toString()).isEqualTo("INT64");
        assertThat(value.isNullAt(1)).isTrue();
        assertThat(value.getLong(2)).isEqualTo(expected);
    }
}
