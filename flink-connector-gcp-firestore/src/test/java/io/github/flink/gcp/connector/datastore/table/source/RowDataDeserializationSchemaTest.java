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

package io.github.flink.gcp.connector.datastore.table.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Blob;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityValue;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.LatLng;
import com.google.cloud.datastore.ListValue;
import com.google.cloud.datastore.LongValue;
import com.google.cloud.datastore.NullValue;
import com.google.cloud.datastore.StringValue;
import io.github.flink.gcp.connector.datastore.source.serializer.EntityMetadata;
import io.github.flink.gcp.connector.datastore.table.DatastoreTableSchema;
import io.github.flink.gcp.connector.datastore.table.TypeMismatchPolicy;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RowDataDeserializationSchemaTest {

    private static final Timestamp CREATED = Timestamp.ofTimeSecondsAndNanos(100, 1_234_567);
    private static final Timestamp UPDATED = Timestamp.ofTimeSecondsAndNanos(200, 0);
    // Finer than the metadata's microseconds, as a configured 'scan.read-time' may be.
    private static final Timestamp READ = Timestamp.ofTimeSecondsAndNanos(300, 999);
    private static final EntityMetadata METADATA = new EntityMetadata(9L, CREATED, UPDATED, READ);

    private static RowType row(DataTypes.Field... fields) {
        return (RowType) DataTypes.ROW(fields).getLogicalType();
    }

    private static RowDataDeserializationSchema schema(
            RowType row, int key, TypeMismatchPolicy policy, String... metadata) {
        return schema(
                row, key, IntStream.range(0, row.getFieldCount()).toArray(), policy, metadata);
    }

    private static RowDataDeserializationSchema schema(
            RowType row, int key, int[] columns, TypeMismatchPolicy policy, String... metadata) {
        return new RowDataDeserializationSchema(
                DatastoreTableSchema.of(row, key < 0 ? new int[0] : new int[] {key}, List.of()),
                columns,
                List.of(metadata),
                policy,
                TypeInformation.of(RowData.class));
    }

    private static RowData read(RowDataDeserializationSchema schema, Entity entity)
            throws IOException {
        List<RowData> rows = new ArrayList<>();
        schema.deserialize(entity, METADATA, new ListCollector(rows));
        assertThat(rows).hasSize(1);
        return rows.get(0);
    }

    private static Entity.Builder entity(String name) {
        return Entity.newBuilder(Key.newBuilder("p", "K", name).build());
    }

    @Test
    void everyMappedTypeReadsBack() throws IOException {
        RowType row =
                row(
                        DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("s", DataTypes.STRING()),
                        DataTypes.FIELD("b", DataTypes.BOOLEAN()),
                        DataTypes.FIELD("n", DataTypes.BIGINT()),
                        DataTypes.FIELD("d", DataTypes.DOUBLE()),
                        DataTypes.FIELD("bytes", DataTypes.BYTES()),
                        DataTypes.FIELD("ts", DataTypes.TIMESTAMP_LTZ(3)),
                        DataTypes.FIELD("tags", DataTypes.ARRAY(DataTypes.STRING())),
                        DataTypes.FIELD(
                                "nested",
                                DataTypes.ROW(
                                        DataTypes.FIELD("x", DataTypes.BIGINT()),
                                        DataTypes.FIELD("y", DataTypes.STRING()))),
                        DataTypes.FIELD("missing", DataTypes.STRING()),
                        DataTypes.FIELD("nil", DataTypes.STRING()));
        Entity entity =
                entity("o1")
                        .set("s", "text")
                        .set("b", true)
                        .set("n", 42L)
                        .set("d", 1.5d)
                        .set("bytes", Blob.copyFrom(new byte[] {1, 2}))
                        .set("ts", Timestamp.ofTimeSecondsAndNanos(-1, 123_456_789))
                        .set("tags", ListValue.of(StringValue.of("a"), NullValue.of()))
                        .set("nested", EntityValue.of(FullEntity.newBuilder().set("x", 1L).build()))
                        .setNull("nil")
                        .build();

        RowData read = read(schema(row, 0, TypeMismatchPolicy.FAIL), entity);

        assertThat(read.getString(0)).isEqualTo(StringData.fromString("o1"));
        assertThat(read.getString(1)).isEqualTo(StringData.fromString("text"));
        assertThat(read.getBoolean(2)).isTrue();
        assertThat(read.getLong(3)).isEqualTo(42L);
        assertThat(read.getDouble(4)).isEqualTo(1.5d);
        assertThat(read.getBinary(5)).containsExactly(1, 2);
        // Truncated to the column's milliseconds; a negative second keeps the instant.
        assertThat(read.getTimestamp(6, 3)).isEqualTo(TimestampData.fromEpochMillis(-1000 + 123));
        ArrayData tags = read.getArray(7);
        assertThat(tags.getString(0)).isEqualTo(StringData.fromString("a"));
        assertThat(tags.isNullAt(1)).isTrue();
        RowData nested = read.getRow(8, 2);
        assertThat(nested.getLong(0)).isEqualTo(1L);
        assertThat(nested.isNullAt(1)).as("a property the embedded entity lacks").isTrue();
        assertThat(read.isNullAt(9)).as("a property the entity lacks").isTrue();
        assertThat(read.isNullAt(10)).as("a property that holds null").isTrue();
    }

    @Test
    void aStringOrBlobReadsWholeWhateverLengthTheColumnDeclares() throws IOException {
        RowType row =
                row(
                        DataTypes.FIELD("s", DataTypes.VARCHAR(3)),
                        DataTypes.FIELD("c", DataTypes.CHAR(4)),
                        DataTypes.FIELD("b", DataTypes.BINARY(2)));
        Entity entity =
                entity("a")
                        .set("s", "abcdef")
                        .set("c", "ab")
                        .set("b", Blob.copyFrom(new byte[] {1, 2, 3}))
                        .build();

        RowData read = read(schema(row, -1, TypeMismatchPolicy.FAIL), entity);

        assertThat(read.getString(0)).isEqualTo(StringData.fromString("abcdef"));
        assertThat(read.getString(1)).as("not padded").isEqualTo(StringData.fromString("ab"));
        assertThat(read.getBinary(2)).containsExactly(1, 2, 3);
    }

    @Test
    void anIntegerReadsIntoADoubleOnlyWhenItIsExact() throws IOException {
        RowType row = row(DataTypes.FIELD("d", DataTypes.DOUBLE()));
        RowDataDeserializationSchema schema = schema(row, -1, TypeMismatchPolicy.FAIL);

        assertThat(read(schema, entity("a").set("d", 1L << 53).build()).getDouble(0))
                .isEqualTo((double) (1L << 53));
        assertThat(read(schema, entity("a").set("d", -(1L << 53)).build()).getDouble(0))
                .isEqualTo((double) -(1L << 53));
        assertThatThrownBy(() -> read(schema, entity("a").set("d", (1L << 53) + 1).build()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("beyond the 2^53 in magnitude");
    }

    @Test
    void aMismatchFailsUnderFailAndReadsAsNullUnderNull() throws IOException {
        RowType row =
                row(
                        DataTypes.FIELD("n", DataTypes.BIGINT()),
                        DataTypes.FIELD("where", DataTypes.STRING()),
                        DataTypes.FIELD(
                                "nested", DataTypes.ROW(DataTypes.FIELD("x", DataTypes.BIGINT()))),
                        DataTypes.FIELD("tags", DataTypes.ARRAY(DataTypes.BIGINT())));
        Entity entity =
                entity("a")
                        .set("n", "text")
                        .set("where", LatLng.of(1, 2))
                        .set(
                                "nested",
                                EntityValue.of(FullEntity.newBuilder().set("x", "y").build()))
                        .set("tags", ListValue.of(LongValue.of(1), StringValue.of("two")))
                        .build();

        assertThatThrownBy(() -> read(schema(row, -1, TypeMismatchPolicy.FAIL), entity))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(
                        "Property 'n' holds a value of type STRING, which is not an integer")
                .hasMessageContaining("'type-mismatch-policy' = 'null'");
        RowData lenient = read(schema(row, -1, TypeMismatchPolicy.NULL), entity);
        assertThat(lenient.isNullAt(0)).isTrue();
        assertThat(lenient.isNullAt(1)).as("a geographical point is not a string").isTrue();
        assertThat(lenient.getRow(2, 1).isNullAt(0)).as("the innermost nullable field").isTrue();
        assertThat(lenient.getArray(3).getLong(0)).isEqualTo(1L);
        assertThat(lenient.getArray(3).isNullAt(1)).isTrue();
    }

    @Test
    void aProjectionReadsEachProducedColumnFromItsOwnProperty() throws IOException {
        RowType row =
                row(
                        DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("n", DataTypes.BIGINT()),
                        DataTypes.FIELD("tags", DataTypes.ARRAY(DataTypes.STRING())));
        Entity entity = entity("a").set("n", "not read").set("tags", "t1", "t2").build();

        RowData read =
                read(schema(row, 0, new int[] {2, 0}, TypeMismatchPolicy.FAIL, "version"), entity);

        assertThat(read.getArity()).isEqualTo(3);
        assertThat(read.getArray(0).getString(1)).isEqualTo(StringData.fromString("t2"));
        assertThat(read.getString(1)).isEqualTo(StringData.fromString("a"));
        assertThat(read.getLong(2)).isEqualTo(9L);
    }

    @Test
    void aNotNullNestedFieldReadsTheNullableFieldAroundItAsNull() throws IOException {
        RowType row =
                row(
                        DataTypes.FIELD(
                                "nested",
                                DataTypes.ROW(DataTypes.FIELD("x", DataTypes.BIGINT().notNull()))),
                        DataTypes.FIELD("tags", DataTypes.ARRAY(DataTypes.BIGINT().notNull())));
        Entity entity =
                entity("a")
                        .set(
                                "nested",
                                EntityValue.of(FullEntity.newBuilder().set("x", "y").build()))
                        .set("tags", ListValue.of(LongValue.of(1), StringValue.of("two")))
                        .build();

        RowData lenient = read(schema(row, -1, TypeMismatchPolicy.NULL), entity);

        assertThat(lenient.isNullAt(0)).as("the ROW around a NOT NULL field").isTrue();
        assertThat(lenient.isNullAt(1)).as("the ARRAY around a NOT NULL element").isTrue();
        assertThatThrownBy(() -> read(schema(row, -1, TypeMismatchPolicy.FAIL), entity))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Property 'nested.x' holds a value of type STRING")
                .hasMessageContaining("'type-mismatch-policy' = 'null'");
    }

    @Test
    void aNotNullColumnFailsWhateverThePolicy() {
        RowType row = row(DataTypes.FIELD("n", DataTypes.BIGINT().notNull()));

        assertThatThrownBy(
                        () ->
                                read(
                                        schema(row, -1, TypeMismatchPolicy.NULL),
                                        entity("a").set("n", "text").build()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("The column 'n' is NOT NULL, so no policy can read it");
        assertThatThrownBy(
                        () -> read(schema(row, -1, TypeMismatchPolicy.NULL), entity("a").build()))
                .hasMessageContaining(
                        "Property 'n' is missing or holds null, but the table declares it NOT NULL");
    }

    @Test
    void theKeyColumnReadsTheNameOrTheIdAndRefusesTheOtherForm() throws IOException {
        RowType byName =
                row(
                        DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("v", DataTypes.BIGINT()));
        RowType byId =
                row(
                        DataTypes.FIELD("id", DataTypes.BIGINT().notNull()),
                        DataTypes.FIELD("v", DataTypes.BIGINT()));
        Entity named = entity("a").set("v", 1L).build();
        Entity numbered = Entity.newBuilder(Key.newBuilder("p", "K", 7L).build()).build();

        assertThat(read(schema(byName, 0, TypeMismatchPolicy.FAIL), named).getString(0))
                .isEqualTo(StringData.fromString("a"));
        assertThat(read(schema(byId, 0, TypeMismatchPolicy.NULL), numbered).getLong(0))
                .isEqualTo(7L);
        assertThatThrownBy(() -> read(schema(byName, 0, TypeMismatchPolicy.NULL), numbered))
                .hasMessageContaining("its key has the numeric id 7");
        assertThatThrownBy(() -> read(schema(byId, 0, TypeMismatchPolicy.NULL), named))
                .hasMessageContaining("its key has the name 'a'");
    }

    @Test
    void aKeyWithAParentFailsAKeyedReadAndReadsIntoATableWithoutAKey() throws IOException {
        Key parent = Key.newBuilder("p", "Parent", 1L).build();
        Entity child =
                Entity.newBuilder(Key.newBuilder(parent, "K", "a").build()).set("v", 1L).build();
        RowType keyed =
                row(
                        DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("v", DataTypes.BIGINT()));
        RowType keyless = row(DataTypes.FIELD("v", DataTypes.BIGINT()));

        assertThatThrownBy(() -> read(schema(keyed, 0, TypeMismatchPolicy.NULL), child))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("its key has the parent")
                .hasMessageContaining("through a table without a PRIMARY KEY");
        // Also when the query does not read the key column, which the planner prunes from a
        // GROUP BY over the key.
        assertThatThrownBy(
                        () -> read(schema(keyed, 0, new int[] {1}, TypeMismatchPolicy.NULL), child))
                .hasMessageContaining("its key has the parent");
        assertThatThrownBy(
                        () ->
                                read(
                                        schema(keyed, 0, new int[] {1}, TypeMismatchPolicy.NULL),
                                        Entity.newBuilder(Key.newBuilder("p", "K", 7L).build())
                                                .build()))
                .hasMessageContaining("its key has the numeric id 7");
        RowData read = read(schema(keyless, -1, TypeMismatchPolicy.FAIL, "key-name"), child);
        assertThat(read.getLong(0)).isEqualTo(1L);
        assertThat(read.getString(1)).isEqualTo(StringData.fromString("a"));
    }

    @Test
    void theMetadataFollowsThePhysicalColumns() throws IOException {
        RowType row = row(DataTypes.FIELD("v", DataTypes.BIGINT()));
        RowDataDeserializationSchema schema =
                schema(
                        row,
                        -1,
                        TypeMismatchPolicy.FAIL,
                        "key-name",
                        "key-id",
                        "version",
                        "create-time",
                        "update-time",
                        "read-time");

        RowData named = read(schema, entity("a").set("v", 1L).build());
        RowData numbered =
                read(schema, Entity.newBuilder(Key.newBuilder("p", "K", 7L).build()).build());

        assertThat(named.getString(1)).isEqualTo(StringData.fromString("a"));
        assertThat(named.isNullAt(2)).isTrue();
        assertThat(named.getLong(3)).isEqualTo(9L);
        // Each time truncated to the declared microseconds.
        assertThat(named.getTimestamp(4, 6))
                .isEqualTo(TimestampData.fromEpochMillis(100_001, 234_000));
        assertThat(named.getTimestamp(5, 6)).isEqualTo(TimestampData.fromEpochMillis(200_000));
        assertThat(named.getTimestamp(6, 6)).isEqualTo(TimestampData.fromEpochMillis(300_000));
        assertThat(numbered.isNullAt(1)).isTrue();
        assertThat(numbered.getLong(2)).isEqualTo(7L);
    }

    @Test
    void aResultWithoutACreateTimeFailsAReadThatAsksForIt() {
        RowType row = row(DataTypes.FIELD("v", DataTypes.BIGINT()));
        RowDataDeserializationSchema schema =
                schema(row, -1, TypeMismatchPolicy.FAIL, "create-time");
        List<RowData> rows = new ArrayList<>();

        assertThatThrownBy(
                        () ->
                                schema.deserialize(
                                        entity("a").build(),
                                        new EntityMetadata(1L, null, UPDATED, READ),
                                        new ListCollector(rows)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("without its 'create-time'");
    }

    @Test
    void aResultWithoutAVersionFailsAReadThatAsksForIt() {
        RowType row = row(DataTypes.FIELD("v", DataTypes.BIGINT()));
        RowDataDeserializationSchema schema = schema(row, -1, TypeMismatchPolicy.FAIL, "version");
        List<RowData> rows = new ArrayList<>();

        assertThatThrownBy(
                        () ->
                                schema.deserialize(
                                        entity("a").build(),
                                        new EntityMetadata(0L, CREATED, UPDATED, READ),
                                        new ListCollector(rows)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("without its 'version'");
    }

    @Test
    void anUnknownMetadataKeyIsRefused() {
        assertThatThrownBy(
                        () ->
                                schema(
                                        row(DataTypes.FIELD("v", DataTypes.BIGINT())),
                                        -1,
                                        TypeMismatchPolicy.FAIL,
                                        "document-path"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no readable metadata 'document-path'");
    }

    @Test
    void theSchemaSurvivesJavaSerializationAndKeepsItsEquality() throws Exception {
        RowType row = row(DataTypes.FIELD("v", DataTypes.BIGINT()));
        RowDataDeserializationSchema original = schema(row, -1, TypeMismatchPolicy.NULL, "version");
        RowDataDeserializationSchema copy =
                InstantiationUtil.clone(original, getClass().getClassLoader());

        assertThat(copy).isEqualTo(original).hasSameHashCodeAs(original);
        assertThat(copy)
                .isNotEqualTo(schema(row, -1, TypeMismatchPolicy.FAIL, "version"))
                .isNotEqualTo(schema(row, -1, TypeMismatchPolicy.NULL));
        assertThat(read(copy, entity("a").set("v", 3L).build()).getLong(1)).isEqualTo(9L);
    }

    /** Collects into a list. */
    private static final class ListCollector implements org.apache.flink.util.Collector<RowData> {
        private final List<RowData> rows;

        ListCollector(List<RowData> rows) {
            this.rows = rows;
        }

        @Override
        public void collect(RowData record) {
            rows.add(record);
        }

        @Override
        public void close() {}
    }
}
