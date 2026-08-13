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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecordAdapter.ChangeStreamRecordBuilder;
import com.google.cloud.bigtable.data.v2.models.DefaultChangeStreamRecordAdapter;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.bigtable.data.v2.models.Range;
import com.google.cloud.bigtable.data.v2.models.Value;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

class ChangeStreamMutationToRowDataConverterTest {

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
        ChangeStreamMutation mutation =
                mutationWithMetadata(
                        ChangeStreamMutation.MutationType.USER,
                        "cluster-1",
                        commit,
                        7,
                        "private-token",
                        watermark);
        ChangeStreamMutationRowDataDeserializationSchema schema =
                new ChangeStreamMutationRowDataDeserializationSchema(
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
        ChangeStreamMutation mutation =
                mutationWithMetadata(
                        ChangeStreamMutation.MutationType.GARBAGE_COLLECTION,
                        "",
                        Instant.parse("2026-08-13T00:00:00Z"),
                        3,
                        "private-token",
                        Instant.parse("2026-08-12T23:59:00Z"));
        ChangeStreamMutationRowDataDeserializationSchema schema =
                new ChangeStreamMutationRowDataDeserializationSchema(
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
    void preservesEverySdkEntryKindAndItsOrderedGenericValues() throws Exception {
        ChangeStreamMutation mutation = mutationWithEveryEntryKind();
        RowData envelope = new ChangeStreamMutationToRowDataConverter().convert(mutation);
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
    void rejectsAnUnknownFutureEntryInsteadOfEmittingAPartialEnvelope() {
        Entry futureEntry = new Entry() {};
        Entry validEntry = mutationWithEveryEntryKind().getEntries().get(0);
        ChangeStreamMutation mutation = mutationWithEntries(validEntry, futureEntry);
        List<RowData> output = new ArrayList<>();
        ChangeStreamMutationRowDataDeserializationSchema schema =
                new ChangeStreamMutationRowDataDeserializationSchema(
                        TypeInformation.of(RowData.class));

        assertThatThrownBy(() -> schema.deserialize(mutation, collectingInto(output)))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining(futureEntry.getClass().getName())
                .hasMessageContaining("Upgrade the table envelope converter");
        assertThat(output).isEmpty();
    }

    @Test
    void theSchemaCollectsOneConvertedMutationAndReportsItsProducedType() throws Exception {
        TypeInformation<RowData> producedType = TypeInformation.of(RowData.class);
        ChangeStreamMutationRowDataDeserializationSchema schema =
                new ChangeStreamMutationRowDataDeserializationSchema(producedType);
        List<RowData> output = new ArrayList<>();

        schema.deserialize(mutationWithEveryEntryKind(), collectingInto(output));

        assertThat(output)
                .singleElement()
                .isEqualTo(
                        new ChangeStreamMutationToRowDataConverter()
                                .convert(mutationWithEveryEntryKind()));
        assertThat(schema.getProducedType()).isSameAs(producedType);
    }

    @Test
    void theSchemaSurvivesJavaSerialization() throws Exception {
        ChangeStreamMutationRowDataDeserializationSchema schema =
                new ChangeStreamMutationRowDataDeserializationSchema(
                        new ChangeStreamReadableMetadata[] {
                            ChangeStreamReadableMetadata.ESTIMATED_LOW_WATERMARK,
                            ChangeStreamReadableMetadata.MUTATION_TYPE
                        },
                        TypeInformation.of(RowData.class));

        ChangeStreamMutationRowDataDeserializationSchema copy = roundTrip(schema);
        List<RowData> output = new ArrayList<>();
        Instant watermark = Instant.parse("2026-08-12T23:59:00.987654321Z");
        copy.deserialize(
                mutationWithMetadata(
                        ChangeStreamMutation.MutationType.USER,
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
        ChangeStreamMutationToRowDataConverter converter =
                new ChangeStreamMutationToRowDataConverter();

        ChangeStreamMutationToRowDataConverter copy = roundTrip(converter);

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

    private static ChangeStreamMutation mutationWithEntries(Entry... entries) {
        return new ChangeStreamMutation() {
            @Override
            public ByteString getRowKey() {
                return ByteString.copyFromUtf8("row-1");
            }

            @Override
            public MutationType getType() {
                return MutationType.USER;
            }

            @Override
            public String getSourceClusterId() {
                return "cluster-1";
            }

            @Override
            public Instant getCommitTime() {
                return Instant.parse("2026-08-13T00:00:00Z");
            }

            @Override
            public int getTieBreaker() {
                return 0;
            }

            @Override
            public String getToken() {
                return "token";
            }

            @Override
            public Instant getEstimatedLowWatermarkTime() {
                return Instant.parse("2026-08-12T23:59:00Z");
            }

            @Override
            public ImmutableList<Entry> getEntries() {
                return ImmutableList.copyOf(entries);
            }
        };
    }

    private static ChangeStreamMutation mutationWithMetadata(
            ChangeStreamMutation.MutationType type,
            String sourceClusterId,
            Instant commitTime,
            int tieBreaker,
            String token,
            Instant estimatedLowWatermark) {
        Entry entry = mutationWithEveryEntryKind().getEntries().get(0);
        return new ChangeStreamMutation() {
            @Override
            public ByteString getRowKey() {
                return ByteString.copyFromUtf8("row-1");
            }

            @Override
            public MutationType getType() {
                return type;
            }

            @Override
            public String getSourceClusterId() {
                return sourceClusterId;
            }

            @Override
            public Instant getCommitTime() {
                return commitTime;
            }

            @Override
            public int getTieBreaker() {
                return tieBreaker;
            }

            @Override
            public String getToken() {
                return token;
            }

            @Override
            public Instant getEstimatedLowWatermarkTime() {
                return estimatedLowWatermark;
            }

            @Override
            public ImmutableList<Entry> getEntries() {
                return ImmutableList.of(entry);
            }
        };
    }

    private static ChangeStreamMutation mutationWithEveryEntryKind() {
        ChangeStreamRecordBuilder<ChangeStreamRecord> builder =
                new DefaultChangeStreamRecordAdapter().createChangeStreamRecordBuilder();
        builder.startUserMutation(
                ByteString.copyFromUtf8("row-1"),
                "cluster-1",
                Instant.parse("2026-08-13T00:00:00Z"),
                0);
        builder.startCell("family", ByteString.copyFromUtf8("qualifier"), 123_456L);
        builder.cellValue(ByteString.copyFromUtf8("value"));
        builder.finishCell();
        builder.deleteCells(
                "family",
                ByteString.copyFromUtf8("qualifier"),
                Range.TimestampRange.create(10L, 20L));
        builder.deleteCells(
                "family", ByteString.copyFromUtf8("qualifier"), Range.TimestampRange.unbounded());
        builder.deleteFamily("family");
        builder.addToCell(
                "aggregate",
                Value.rawValue(ByteString.copyFromUtf8("add-qualifier")),
                Value.rawTimestamp(456_789L),
                Value.intValue(7L));
        builder.mergeToCell(
                "aggregate",
                Value.rawValue(ByteString.copyFromUtf8("merge-qualifier")),
                Value.rawTimestamp(987_654L),
                Value.rawValue(ByteString.copyFromUtf8("fragment")));
        return (ChangeStreamMutation)
                builder.finishChangeStreamMutation("token", Instant.parse("2026-08-12T23:59:00Z"));
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
