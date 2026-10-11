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
import org.apache.flink.table.types.DataType;
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
import com.google.cloud.datastore.PathElement;
import com.google.cloud.datastore.StringValue;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
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
    // Finer than the metadata's microseconds, which neither the service's times nor the
    // builder's truncated read time are: the column still truncates whatever arrives.
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
        return schema(
                DatastoreTableSchema.of(
                        row,
                        key < 0 ? new int[0] : new int[] {key},
                        List.of(),
                        List.of(),
                        List.of()),
                columns,
                policy,
                metadata);
    }

    private static RowDataDeserializationSchema schema(
            DatastoreTableSchema schema,
            int[] columns,
            TypeMismatchPolicy policy,
            String... metadata) {
        return schema(schema, DatabaseDestination.of("p"), "", columns, policy, metadata);
    }

    private static RowDataDeserializationSchema schema(
            DatastoreTableSchema schema,
            DatabaseDestination database,
            String namespace,
            int[] columns,
            TypeMismatchPolicy policy,
            String... metadata) {
        return new RowDataDeserializationSchema(
                schema,
                database,
                namespace,
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
        DatastoreTableSchema table =
                DatastoreTableSchema.of(row, new int[0], List.of(), List.of(), List.of());
        assertThat(copy)
                .isNotEqualTo(schema(row, -1, TypeMismatchPolicy.FAIL, "version"))
                .isNotEqualTo(schema(row, -1, TypeMismatchPolicy.NULL))
                .isNotEqualTo(
                        schema(
                                table,
                                DatabaseDestination.of("p", "db"),
                                "",
                                new int[] {0},
                                TypeMismatchPolicy.NULL,
                                "version"))
                .isNotEqualTo(
                        schema(
                                table,
                                DatabaseDestination.of("p"),
                                "tenant",
                                new int[] {0},
                                TypeMismatchPolicy.NULL,
                                "version"));
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

    private static DataType pathElement(DataType name, DataType id) {
        return DataTypes.ROW(
                DataTypes.FIELD("kind", DataTypes.STRING()),
                DataTypes.FIELD("name", name),
                DataTypes.FIELD("id", id));
    }

    private static final DataType PATH_ELEMENT =
            pathElement(DataTypes.STRING(), DataTypes.BIGINT());

    private static final RowType MARKED =
            row(
                    DataTypes.FIELD(
                            "at",
                            DataTypes.ROW(
                                    DataTypes.FIELD("latitude", DataTypes.DOUBLE()),
                                    DataTypes.FIELD("longitude", DataTypes.DOUBLE()))),
                    DataTypes.FIELD("owner", PATH_ELEMENT),
                    DataTypes.FIELD(
                            "child",
                            DataTypes.ROW(
                                    DataTypes.FIELD("kind", DataTypes.STRING()),
                                    DataTypes.FIELD("name", DataTypes.STRING()),
                                    DataTypes.FIELD("id", DataTypes.BIGINT()),
                                    DataTypes.FIELD("ancestors", DataTypes.ARRAY(PATH_ELEMENT)))),
                    DataTypes.FIELD("refs", DataTypes.ARRAY(PATH_ELEMENT)),
                    DataTypes.FIELD(
                            "named",
                            pathElement(DataTypes.STRING().notNull(), DataTypes.BIGINT())));

    private static RowDataDeserializationSchema marked(TypeMismatchPolicy policy) {
        return schema(
                DatastoreTableSchema.of(
                        MARKED,
                        new int[0],
                        List.of(),
                        List.of("at"),
                        List.of("owner", "child", "refs", "named")),
                IntStream.range(0, MARKED.getFieldCount()).toArray(),
                policy);
    }

    @Test
    void markedColumnsReadGeoPointsAndKeyValues() throws IOException {
        Entity entity =
                entity("a")
                        .set("at", LatLng.of(-35.5, 139.25))
                        .set("owner", Key.newBuilder("p", "User", "u1").build())
                        .set(
                                "child",
                                Key.newBuilder("p", "Line", 3L)
                                        .addAncestors(
                                                PathElement.of("Root", "r"),
                                                PathElement.of("Mid", -7L))
                                        .build())
                        .set(
                                "refs",
                                ListValue.of(
                                        com.google.cloud.datastore.KeyValue.of(
                                                Key.newBuilder("p", "User", 9L).build()),
                                        NullValue.of()))
                        .set("named", Key.newBuilder("p", "User", "u2").build())
                        .build();

        RowData read = read(marked(TypeMismatchPolicy.FAIL), entity);

        RowData at = read.getRow(0, 2);
        assertThat(at.getDouble(0)).isEqualTo(-35.5);
        assertThat(at.getDouble(1)).isEqualTo(139.25);
        RowData owner = read.getRow(1, 3);
        assertThat(owner.getString(0)).isEqualTo(StringData.fromString("User"));
        assertThat(owner.getString(1)).isEqualTo(StringData.fromString("u1"));
        assertThat(owner.isNullAt(2)).isTrue();
        RowData child = read.getRow(2, 4);
        assertThat(child.getString(0)).isEqualTo(StringData.fromString("Line"));
        assertThat(child.isNullAt(1)).isTrue();
        assertThat(child.getLong(2)).isEqualTo(3L);
        ArrayData ancestors = child.getArray(3);
        assertThat(ancestors.size()).isEqualTo(2);
        assertThat(ancestors.getRow(0, 3).getString(1)).isEqualTo(StringData.fromString("r"));
        assertThat(ancestors.getRow(1, 3).getString(0)).isEqualTo(StringData.fromString("Mid"));
        assertThat(ancestors.getRow(1, 3).getLong(2)).isEqualTo(-7L);
        ArrayData refs = read.getArray(3);
        assertThat(refs.getRow(0, 3).getLong(2)).isEqualTo(9L);
        assertThat(refs.isNullAt(1)).isTrue();
        assertThat(read.getRow(4, 3).getString(1)).isEqualTo(StringData.fromString("u2"));
    }

    @Test
    void aKeyWithoutAncestorsReadsAnEmptyAncestorsArray() throws IOException {
        Entity entity = entity("a").set("child", Key.newBuilder("p", "Line", "l").build()).build();

        RowData child = read(marked(TypeMismatchPolicy.FAIL), entity).getRow(2, 4);

        assertThat(child.getArray(3).size()).isZero();
    }

    @Test
    void aKeyOutsideTheTablesPartitionOrWithUndeclaredAncestorsIsAMismatch() throws IOException {
        Key[] outside = {
            Key.newBuilder("other", "User", "u").build(),
            Key.newBuilder("p", "User", "u", "db").build(),
            Key.newBuilder("p", "User", "u").setNamespace("tenant").build(),
        };
        for (Key key : outside) {
            Entity entity = entity("a").set("owner", key).build();
            assertThatThrownBy(() -> read(marked(TypeMismatchPolicy.FAIL), entity))
                    .as(key.toString())
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Property 'owner' holds the key")
                    .hasMessageContaining(
                            "which is not in the table's project, database and namespace")
                    .hasMessageContaining("'type-mismatch-policy' = 'null'");
            assertThat(read(marked(TypeMismatchPolicy.NULL), entity).isNullAt(1)).isTrue();
        }
        Entity withParent =
                entity("a")
                        .set(
                                "owner",
                                Key.newBuilder("p", "User", "u")
                                        .addAncestor(PathElement.of("Org", "o"))
                                        .build())
                        .build();
        assertThatThrownBy(() -> read(marked(TypeMismatchPolicy.FAIL), withParent))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("which has ancestors, but its ROW declares no 'ancestors'");
    }

    @Test
    void aMarkedColumnMismatchesTheOtherValueTypesAndAnUnmarkedOneMismatchesTheseValues()
            throws IOException {
        Entity entity =
                entity("a")
                        .set("at", Key.newBuilder("p", "User", "u").build())
                        .set("owner", LatLng.of(1, 2))
                        .set("named", Key.newBuilder("p", "User", 4L).build())
                        .build();

        assertThatThrownBy(() -> read(marked(TypeMismatchPolicy.FAIL), entity))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(
                        "Property 'at' holds a value of type KEY, which is not a geographical"
                                + " point");
        RowData lenient = read(marked(TypeMismatchPolicy.NULL), entity);
        assertThat(lenient.isNullAt(0)).isTrue();
        assertThat(lenient.isNullAt(1)).as("a point is not a key").isTrue();
        assertThat(lenient.isNullAt(4)).as("an id key in a NOT NULL name").isTrue();
        assertThatThrownBy(
                        () ->
                                read(
                                        marked(TypeMismatchPolicy.FAIL),
                                        entity("a")
                                                .set(
                                                        "named",
                                                        Key.newBuilder("p", "User", 4L).build())
                                                .build()))
                .hasMessageContaining(
                        "Property 'named' holds a key whose path element User has no name, but"
                                + " the table declares its name NOT NULL");

        // Unmarked, the same ROWs are embedded entities.
        RowType unmarked = MARKED;
        RowDataDeserializationSchema plain = schema(unmarked, -1, TypeMismatchPolicy.FAIL);
        assertThatThrownBy(
                        () ->
                                read(
                                        plain,
                                        entity("a")
                                                .set("owner", Key.newBuilder("p", "U", "u").build())
                                                .build()))
                .hasMessageContaining(
                        "Property 'owner' holds a value of type KEY, which is not an embedded"
                                + " entity");
        assertThatThrownBy(() -> read(plain, entity("a").set("at", LatLng.of(1, 2)).build()))
                .hasMessageContaining(
                        "Property 'at' holds a value of type LAT_LNG, which is not an embedded"
                                + " entity");
    }

    @Test
    void aKeyValueIsComparedWithTheTablesOwnDatabaseAndNamespace() throws IOException {
        RowType row = row(DataTypes.FIELD("owner", PATH_ELEMENT));
        RowDataDeserializationSchema named =
                schema(
                        DatastoreTableSchema.of(
                                row, new int[0], List.of(), List.of(), List.of("owner")),
                        DatabaseDestination.of("p", "db"),
                        "tenant",
                        new int[] {0},
                        TypeMismatchPolicy.FAIL);
        Key inTable = Key.newBuilder("p", "User", "u", "db").setNamespace("tenant").build();

        assertThat(read(named, entity("a").set("owner", inTable).build()).getRow(0, 3).getString(1))
                .isEqualTo(StringData.fromString("u"));
        for (Key outside :
                List.of(
                        Key.newBuilder("p", "User", "u").setNamespace("tenant").build(),
                        Key.newBuilder("p", "User", "u", "db").build())) {
            assertThatThrownBy(() -> read(named, entity("a").set("owner", outside).build()))
                    .as(outside.toString())
                    .hasMessageContaining(
                            "which is not in the table's project, database and namespace");
        }
    }

    @Test
    void aNotNullNameOrIdIsHeldForEveryPathElement() {
        RowType row =
                row(
                        DataTypes.FIELD(
                                "child",
                                DataTypes.ROW(
                                        DataTypes.FIELD("kind", DataTypes.STRING()),
                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                        DataTypes.FIELD("id", DataTypes.BIGINT().notNull()),
                                        DataTypes.FIELD(
                                                "ancestors",
                                                DataTypes.ARRAY(
                                                        pathElement(
                                                                DataTypes.STRING().notNull(),
                                                                DataTypes.BIGINT()))))));
        RowDataDeserializationSchema schema =
                schema(
                        DatastoreTableSchema.of(
                                row, new int[0], List.of(), List.of(), List.of("child")),
                        new int[] {0},
                        TypeMismatchPolicy.FAIL);

        assertThatThrownBy(
                        () ->
                                read(
                                        schema,
                                        entity("a")
                                                .set(
                                                        "child",
                                                        Key.newBuilder("p", "Line", "l").build())
                                                .build()))
                .hasMessageContaining(
                        "Property 'child' holds a key whose path element Line has no id, but the"
                                + " table declares its id NOT NULL");
        assertThatThrownBy(
                        () ->
                                read(
                                        schema,
                                        entity("a")
                                                .set(
                                                        "child",
                                                        Key.newBuilder("p", "Line", 3L)
                                                                .addAncestor(
                                                                        PathElement.of("Mid", -7L))
                                                                .build())
                                                .build()))
                .hasMessageContaining(
                        "Property 'child' holds a key whose path element Mid has no name, but the"
                                + " table declares its name NOT NULL");
    }
}
