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

package io.github.flink.gcp.connector.datastore.table.sink;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Blob;
import com.google.cloud.datastore.EntityValue;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyValue;
import com.google.cloud.datastore.LatLng;
import com.google.cloud.datastore.LatLngValue;
import com.google.cloud.datastore.ListValue;
import com.google.cloud.datastore.PathElement;
import com.google.cloud.datastore.Value;
import com.google.cloud.datastore.ValueType;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.sink.DatastoreKeyAllocator;
import io.github.flink.gcp.connector.datastore.sink.DatastoreMutation;
import io.github.flink.gcp.connector.datastore.table.DatastoreTableSchema;
import io.github.flink.gcp.connector.datastore.table.WriteMode;
import io.github.flink.gcp.connector.testutils.TestContexts;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RowDataSerializationSchemaTest {

    private static final DatabaseDestination DATABASE = DatabaseDestination.of("p", "db");

    private static final RowType EVERY_TYPE =
            (RowType)
                    DataTypes.ROW(
                                    DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                                    DataTypes.FIELD("s", DataTypes.STRING()),
                                    DataTypes.FIELD("b", DataTypes.BOOLEAN()),
                                    DataTypes.FIELD("n", DataTypes.BIGINT()),
                                    DataTypes.FIELD("d", DataTypes.DOUBLE()),
                                    DataTypes.FIELD("bytes", DataTypes.BYTES()),
                                    DataTypes.FIELD("ts", DataTypes.TIMESTAMP_LTZ(9)),
                                    DataTypes.FIELD("tags", DataTypes.ARRAY(DataTypes.STRING())),
                                    DataTypes.FIELD(
                                            "nested",
                                            DataTypes.ROW(
                                                    DataTypes.FIELD("x", DataTypes.BIGINT()),
                                                    DataTypes.FIELD(
                                                            "inner",
                                                            DataTypes.ROW(
                                                                    DataTypes.FIELD(
                                                                            "y",
                                                                            DataTypes.STRING()))))),
                                    DataTypes.FIELD("missing", DataTypes.STRING()))
                            .getLogicalType();

    private static RowDataSerializationSchema serializer(DatastoreTableSchema schema) {
        return serializer(schema, "");
    }

    private static RowDataSerializationSchema serializer(
            DatastoreTableSchema schema, String namespace) {
        RowDataSerializationSchema serializer =
                new RowDataSerializationSchema(
                        schema, DATABASE, namespace, "Order", WriteMode.MERGE);
        return serializer;
    }

    private static DatastoreTableSchema everyType(List<String> unindexed) {
        return DatastoreTableSchema.of(EVERY_TYPE, new int[] {0}, unindexed, List.of(), List.of());
    }

    private static GenericRowData everyTypeRow(String id) {
        return GenericRowData.of(
                StringData.fromString(id),
                StringData.fromString("text"),
                true,
                42L,
                1.5d,
                new byte[] {1, 2},
                TimestampData.fromInstant(Instant.ofEpochSecond(-1, 123_456_789)),
                new GenericArrayData(new Object[] {StringData.fromString("a"), null}),
                GenericRowData.of(1L, GenericRowData.of(StringData.fromString("deep"))),
                null);
    }

    @Test
    void everyColumnButTheKeyIsAPropertyOfItsDatastoreType() throws IOException {
        DatastoreMutation mutation =
                serializer(everyType(List.of())).serialize(everyTypeRow("o1"), TestContexts.NO_OP);

        assertThat(mutation.getOperation()).isEqualTo(DatastoreMutation.Operation.UPSERT_MERGE);
        assertThat(mutation.getKey())
                .isEqualTo(Key.newBuilder("p", "Order", "o1", "db").setNamespace("").build());
        FullEntity<Key> entity = mutation.getEntity();
        assertThat(entity.getNames())
                .containsExactlyInAnyOrder(
                        "s", "b", "n", "d", "bytes", "ts", "tags", "nested", "missing");
        assertThat(entity.getString("s")).isEqualTo("text");
        assertThat(entity.getBoolean("b")).isTrue();
        assertThat(entity.getLong("n")).isEqualTo(42L);
        assertThat(entity.getDouble("d")).isEqualTo(1.5d);
        assertThat(entity.getBlob("bytes")).isEqualTo(Blob.copyFrom(new byte[] {1, 2}));
        // A negative epoch second with nanos: the floor division keeps the instant.
        assertThat(entity.getTimestamp("ts"))
                .isEqualTo(Timestamp.ofTimeSecondsAndNanos(-1, 123_456_789));
        List<Value<?>> tags = entity.getList("tags");
        assertThat(tags)
                .extracting(Value::getType)
                .containsExactly(ValueType.STRING, ValueType.NULL);
        FullEntity<?> nested = entity.getEntity("nested");
        assertThat(nested.getKey()).isNull();
        assertThat(nested.getLong("x")).isEqualTo(1L);
        assertThat(nested.getEntity("inner").getString("y")).isEqualTo("deep");
        assertThat(entity.isNull("missing")).isTrue();
        // Without sink.unindexed-columns every value stays indexed, nested ones included.
        assertThat(entity.getProperties().values())
                .allSatisfy(value -> assertThat(value.excludeFromIndexes()).isFalse());
        assertThat(tags).allSatisfy(tag -> assertThat(tag.excludeFromIndexes()).isFalse());
        assertThat(nested.getProperties().values())
                .allSatisfy(value -> assertThat(value.excludeFromIndexes()).isFalse());
        assertThat(nested.getEntity("inner").getValue("y").excludeFromIndexes()).isFalse();
    }

    @Test
    void anUnindexedColumnIsExcludedWithEveryValueNestedInIt() throws IOException {
        FullEntity<Key> entity =
                serializer(everyType(List.of("s", "tags", "nested", "missing")))
                        .serialize(everyTypeRow("o1"), TestContexts.NO_OP)
                        .getEntity();

        assertThat(entity.getValue("s").excludeFromIndexes()).isTrue();
        assertThat(entity.getValue("missing").excludeFromIndexes()).isTrue();
        assertThat(entity.getValue("n").excludeFromIndexes()).isFalse();
        // Datastore refuses the mark on an array value, so the elements carry it.
        ListValue tags = entity.getValue("tags");
        assertThat(tags.excludeFromIndexes()).isFalse();
        assertThat(tags.get()).allSatisfy(v -> assertThat(v.excludeFromIndexes()).isTrue());
        EntityValue nested = entity.getValue("nested");
        assertThat(nested.excludeFromIndexes()).isTrue();
        assertThat(nested.get().getValue("x").excludeFromIndexes()).isTrue();
        EntityValue inner = nested.get().getValue("inner");
        assertThat(inner.excludeFromIndexes()).isTrue();
        assertThat(inner.get().getValue("y").excludeFromIndexes()).isTrue();
    }

    @Test
    void anArrayOfRowsIsAnArrayOfEmbeddedEntities() throws IOException {
        RowType row =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                                        DataTypes.FIELD(
                                                "items",
                                                DataTypes.ARRAY(
                                                        DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "sku",
                                                                        DataTypes.STRING())))))
                                .getLogicalType();
        DatastoreTableSchema schema =
                DatastoreTableSchema.of(row, new int[] {0}, List.of("items"), List.of(), List.of());

        FullEntity<Key> entity =
                serializer(schema)
                        .serialize(
                                GenericRowData.of(
                                        StringData.fromString("o1"),
                                        new GenericArrayData(
                                                new Object[] {
                                                    GenericRowData.of(StringData.fromString("a"))
                                                })),
                                TestContexts.NO_OP)
                        .getEntity();

        List<Value<?>> items = entity.getList("items");
        assertThat(items).singleElement().isInstanceOf(EntityValue.class);
        EntityValue item = (EntityValue) items.get(0);
        assertThat(item.excludeFromIndexes()).isTrue();
        assertThat(item.get().getString("sku")).isEqualTo("a");
        assertThat(item.get().getValue("sku").excludeFromIndexes()).isTrue();
    }

    @Test
    void theWriteModeChoosesBetweenAMergeAndAWholeUpsert() throws IOException {
        DatastoreTableSchema schema = everyType(List.of());

        assertThat(
                        new RowDataSerializationSchema(
                                        schema, DATABASE, "", "Order", WriteMode.MERGE)
                                .serialize(everyTypeRow("o1"), TestContexts.NO_OP)
                                .getOperation())
                .isEqualTo(DatastoreMutation.Operation.UPSERT_MERGE);
        assertThat(
                        new RowDataSerializationSchema(
                                        schema, DATABASE, "", "Order", WriteMode.REPLACE)
                                .serialize(everyTypeRow("o1"), TestContexts.NO_OP)
                                .getOperation())
                .isEqualTo(DatastoreMutation.Operation.UPSERT);
    }

    @Test
    void anUpdateAfterIsAnUpsertAndADeleteDeletesTheKey() throws IOException {
        DatastoreTableSchema schema = everyType(List.of());
        GenericRowData updateAfter = everyTypeRow("o1");
        updateAfter.setRowKind(RowKind.UPDATE_AFTER);
        GenericRowData delete = everyTypeRow("o1");
        delete.setRowKind(RowKind.DELETE);

        assertThat(serializer(schema).serialize(updateAfter, TestContexts.NO_OP).getOperation())
                .isEqualTo(DatastoreMutation.Operation.UPSERT_MERGE);
        DatastoreMutation deleted = serializer(schema).serialize(delete, TestContexts.NO_OP);
        assertThat(deleted.getOperation()).isEqualTo(DatastoreMutation.Operation.DELETE);
        assertThat(deleted.getKey()).isEqualTo(Key.newBuilder("p", "Order", "o1", "db").build());
        assertThat(deleted.getEntity()).isNull();
    }

    @Test
    void aKeyOnlyDeleteReadsOnlyTheKey() throws IOException {
        // A Flink 2.x key-only delete leaves even NOT NULL property columns null; the delete
        // needs the key alone.
        RowType row =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                                        DataTypes.FIELD("n", DataTypes.BIGINT().notNull()),
                                        DataTypes.FIELD("s", DataTypes.STRING().notNull()))
                                .getLogicalType();
        GenericRowData delete = new GenericRowData(RowKind.DELETE, 3);
        delete.setField(0, StringData.fromString("o1"));

        DatastoreMutation mutation =
                serializer(
                                DatastoreTableSchema.of(
                                        row, new int[] {0}, List.of(), List.of(), List.of()))
                        .serialize(delete, TestContexts.NO_OP);

        assertThat(mutation.getOperation()).isEqualTo(DatastoreMutation.Operation.DELETE);
        assertThat(mutation.getKey().getName()).isEqualTo("o1");
    }

    @Test
    void aBigintKeyIsTheKeysIdInTheNamespace() throws IOException {
        RowType row =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.BIGINT().notNull()),
                                        DataTypes.FIELD("s", DataTypes.STRING()))
                                .getLogicalType();

        DatastoreMutation mutation =
                serializer(
                                DatastoreTableSchema.of(
                                        row, new int[] {0}, List.of(), List.of(), List.of()),
                                "tenant-a")
                        .serialize(
                                GenericRowData.of(-7L, StringData.fromString("v")),
                                TestContexts.NO_OP);

        assertThat(mutation.getKey())
                .isEqualTo(
                        Key.newBuilder("p", "Order", -7L, "db").setNamespace("tenant-a").build());
        assertThat(mutation.getEntity().getNames()).containsExactly("s");
    }

    @Test
    void theDefaultDatabaseIsTheEmptyIdTheWriterExpects() throws IOException {
        RowDataSerializationSchema serializer =
                new RowDataSerializationSchema(
                        everyType(List.of()),
                        DatabaseDestination.of("p"),
                        "",
                        "Order",
                        WriteMode.MERGE);

        assertThat(serializer.serialize(everyTypeRow("o1"), TestContexts.NO_OP).getKey())
                .satisfies(
                        key -> {
                            assertThat(key.getDatabaseId()).isEmpty();
                            assertThat(key.getNamespace()).isEmpty();
                            assertThat(key.getProjectId()).isEqualTo("p");
                        });
    }

    @Test
    void aKeyThatAddressesNoEntityFailsTheRecord() {
        RowType idRow =
                (RowType)
                        DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT().notNull()))
                                .getLogicalType();
        RowDataSerializationSchema byId =
                serializer(
                        DatastoreTableSchema.of(
                                idRow, new int[] {0}, List.of(), List.of(), List.of()));
        RowDataSerializationSchema byName = serializer(everyType(List.of()));

        assertThatThrownBy(() -> byId.serialize(GenericRowData.of(0L), TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("holds 0, which is not a key id");
        assertThatThrownBy(() -> byName.serialize(everyTypeRow(""), TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("holds '', which is not a key name");
        GenericRowData nullKey = everyTypeRow("o1");
        nullKey.setField(0, null);
        assertThatThrownBy(() -> byName.serialize(nullKey, TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("holds NULL, which is not a key");
    }

    /**
     * Allocates batches of {@code batch} ids counting up from 1, recording each call's template.
     */
    private static final class CountingAllocator implements DatastoreKeyAllocator {
        private final int batch;
        private final List<IncompleteKey> templates = new ArrayList<>();
        private long next = 1;

        CountingAllocator(int batch) {
            this.batch = batch;
        }

        @Override
        public List<Key> allocate(IncompleteKey key) {
            templates.add(key);
            int count = batch;
            List<Key> keys = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                keys.add(Key.newBuilder(key, next++).build());
            }
            return keys;
        }
    }

    @Test
    void aTableWithoutAKeyUpsertsEachRowUnderAnAllocatedIdFetchedInBatches() throws IOException {
        RowType row =
                (RowType) DataTypes.ROW(DataTypes.FIELD("s", DataTypes.STRING())).getLogicalType();
        RowDataSerializationSchema serializer =
                serializer(
                        DatastoreTableSchema.of(row, new int[0], List.of(), List.of(), List.of()),
                        "tenant-a");
        CountingAllocator allocator = new CountingAllocator(7);
        serializer.setKeyAllocator(allocator);

        List<Long> ids = new ArrayList<>();
        int rows = 7 * 2 + 1;
        for (int i = 0; i < rows; i++) {
            DatastoreMutation mutation =
                    serializer.serialize(
                            GenericRowData.of(StringData.fromString("v")), TestContexts.NO_OP);
            assertThat(mutation.getOperation()).isEqualTo(DatastoreMutation.Operation.UPSERT);
            assertThat(mutation.getEntity().getString("s")).isEqualTo("v");
            ids.add(mutation.getKey().getId());
        }

        // One allocation per batch the allocator returns, each id used once, in order.
        assertThat(allocator.templates).hasSize(3);
        assertThat(ids).doesNotHaveDuplicates().startsWith(1L, 2L, 3L).hasSize(rows);
        assertThat(allocator.templates)
                .allSatisfy(
                        template -> {
                            assertThat(template.getProjectId()).isEqualTo("p");
                            assertThat(template.getDatabaseId()).isEqualTo("db");
                            assertThat(template.getNamespace()).isEqualTo("tenant-a");
                            assertThat(template.getKind()).isEqualTo("Order");
                        });
    }

    @Test
    void onlyTheIdIsTakenFromTheAllocationAnswer() throws IOException {
        RowType row =
                (RowType) DataTypes.ROW(DataTypes.FIELD("s", DataTypes.STRING())).getLogicalType();
        RowDataSerializationSchema serializer =
                serializer(
                        DatastoreTableSchema.of(row, new int[0], List.of(), List.of(), List.of()),
                        "tenant-a");
        // An answer in another namespace, database and kind: the key must stay the table's.
        serializer.setKeyAllocator(
                key ->
                        Collections.nCopies(
                                3,
                                Key.newBuilder("p", "Other", 42L, "elsewhere")
                                        .setNamespace("")
                                        .build()));

        Key key =
                serializer
                        .serialize(
                                GenericRowData.of(StringData.fromString("v")), TestContexts.NO_OP)
                        .getKey();

        assertThat(key)
                .isEqualTo(
                        Key.newBuilder("p", "Order", 42L, "db").setNamespace("tenant-a").build());
    }

    @Test
    void anAllocationAnswerWithoutAnIdFailsTheRecord() {
        RowType row =
                (RowType) DataTypes.ROW(DataTypes.FIELD("s", DataTypes.STRING())).getLogicalType();
        RowDataSerializationSchema serializer =
                serializer(
                        DatastoreTableSchema.of(row, new int[0], List.of(), List.of(), List.of()),
                        "");
        serializer.setKeyAllocator(
                key -> Collections.nCopies(3, Key.newBuilder("p", "Order", "x").build()));

        assertThatThrownBy(
                        () ->
                                serializer.serialize(
                                        GenericRowData.of(StringData.fromString("v")),
                                        TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("which has no id");
    }

    @Test
    void aTableWithoutAKeyNeedsTheWritersAllocator() {
        RowType row =
                (RowType) DataTypes.ROW(DataTypes.FIELD("s", DataTypes.STRING())).getLogicalType();
        RowDataSerializationSchema serializer =
                serializer(
                        DatastoreTableSchema.of(row, new int[0], List.of(), List.of(), List.of()),
                        "");

        assertThatThrownBy(
                        () ->
                                serializer.serialize(
                                        GenericRowData.of(StringData.fromString("v")),
                                        TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("no allocator was given");
    }

    @Test
    void anAllocationFailureFailsTheRecord() {
        RowType row =
                (RowType) DataTypes.ROW(DataTypes.FIELD("s", DataTypes.STRING())).getLogicalType();
        RowDataSerializationSchema serializer =
                serializer(
                        DatastoreTableSchema.of(row, new int[0], List.of(), List.of(), List.of()),
                        "");
        serializer.setKeyAllocator(
                key -> {
                    throw new IOException("allocation refused");
                });

        assertThatThrownBy(
                        () ->
                                serializer.serialize(
                                        GenericRowData.of(StringData.fromString("v")),
                                        TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessage("allocation refused");
    }

    @Test
    void aDeleteWithoutAKeyAndAnUpdateBeforeFailTheRecord() {
        RowType row =
                (RowType) DataTypes.ROW(DataTypes.FIELD("s", DataTypes.STRING())).getLogicalType();
        GenericRowData delete = GenericRowData.of(StringData.fromString("v"));
        delete.setRowKind(RowKind.DELETE);
        GenericRowData updateBefore = everyTypeRow("o1");
        updateBefore.setRowKind(RowKind.UPDATE_BEFORE);

        assertThatThrownBy(
                        () ->
                                serializer(
                                                DatastoreTableSchema.of(
                                                        row,
                                                        new int[0],
                                                        List.of(),
                                                        List.of(),
                                                        List.of()))
                                        .serialize(delete, TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("has no entity to delete");
        assertThatThrownBy(
                        () ->
                                serializer(everyType(List.of()))
                                        .serialize(updateBefore, TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("does not consume UPDATE_BEFORE");
    }

    @Test
    void theSchemaSurvivesJavaSerializationAndKeepsItsEquality() throws Exception {
        RowDataSerializationSchema original =
                new RowDataSerializationSchema(
                        everyType(List.of("s")), DATABASE, "tenant-a", "Order", WriteMode.MERGE);
        RowDataSerializationSchema copy =
                InstantiationUtil.clone(original, getClass().getClassLoader());

        assertThat(copy).isEqualTo(original).hasSameHashCodeAs(original);
        assertThat(copy)
                .isNotEqualTo(
                        new RowDataSerializationSchema(
                                everyType(List.of()),
                                DATABASE,
                                "tenant-a",
                                "Order",
                                WriteMode.MERGE))
                .isNotEqualTo(
                        new RowDataSerializationSchema(
                                everyType(List.of("s")), DATABASE, "", "Order", WriteMode.MERGE))
                .isNotEqualTo(
                        new RowDataSerializationSchema(
                                everyType(List.of("s")),
                                DATABASE,
                                "tenant-a",
                                "Other",
                                WriteMode.MERGE))
                .isNotEqualTo(
                        new RowDataSerializationSchema(
                                everyType(List.of("s")),
                                DATABASE,
                                "tenant-a",
                                "Order",
                                WriteMode.REPLACE))
                .isNotEqualTo(
                        new RowDataSerializationSchema(
                                everyType(List.of("s")),
                                DatabaseDestination.of("p", "other"),
                                "tenant-a",
                                "Order",
                                WriteMode.MERGE));
        assertThat(
                        copy.serialize(everyTypeRow("o1"), TestContexts.NO_OP)
                                .getEntity()
                                .getString("s"))
                .isEqualTo("text");
    }

    private static final DataType PATH_ELEMENT =
            DataTypes.ROW(
                    DataTypes.FIELD("kind", DataTypes.STRING()),
                    DataTypes.FIELD("name", DataTypes.STRING()),
                    DataTypes.FIELD("id", DataTypes.BIGINT()));

    private static final RowType MARKED =
            (RowType)
                    DataTypes.ROW(
                                    DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                                    DataTypes.FIELD(
                                            "at",
                                            DataTypes.ROW(
                                                    DataTypes.FIELD("latitude", DataTypes.DOUBLE()),
                                                    DataTypes.FIELD(
                                                            "longitude", DataTypes.DOUBLE()))),
                                    DataTypes.FIELD("owner", PATH_ELEMENT),
                                    DataTypes.FIELD(
                                            "child",
                                            DataTypes.ROW(
                                                    DataTypes.FIELD("kind", DataTypes.STRING()),
                                                    DataTypes.FIELD("name", DataTypes.STRING()),
                                                    DataTypes.FIELD("id", DataTypes.BIGINT()),
                                                    DataTypes.FIELD(
                                                            "ancestors",
                                                            DataTypes.ARRAY(PATH_ELEMENT)))),
                                    DataTypes.FIELD("refs", DataTypes.ARRAY(PATH_ELEMENT)))
                            .getLogicalType();

    private static DatastoreTableSchema marked(List<String> unindexed) {
        return DatastoreTableSchema.of(
                MARKED, new int[] {0}, unindexed, List.of("at"), List.of("owner", "child", "refs"));
    }

    private static GenericRowData element(String kind, String name, Long id) {
        return GenericRowData.of(
                kind == null ? null : StringData.fromString(kind),
                name == null ? null : StringData.fromString(name),
                id);
    }

    private static GenericRowData markedRow(
            GenericRowData at, GenericRowData owner, GenericRowData child, Object[] refs) {
        return GenericRowData.of(
                StringData.fromString("o1"),
                at,
                owner,
                child,
                refs == null ? null : new GenericArrayData(refs));
    }

    @Test
    void markedRowsAreWrittenAsGeoPointsAndKeyValuesInTheTablesPartition() throws IOException {
        GenericRowData child =
                GenericRowData.of(
                        StringData.fromString("Line"),
                        null,
                        3L,
                        new GenericArrayData(
                                new Object[] {
                                    element("Root", "r", null), element("Mid", null, -7L)
                                }));

        FullEntity<Key> entity =
                serializer(marked(List.of()), "tenant-a")
                        .serialize(
                                markedRow(
                                        GenericRowData.of(35.5d, -139.25d),
                                        element("User", "u1", null),
                                        child,
                                        new Object[] {
                                            element("User", "u2", null), element("User", null, 9L)
                                        }),
                                TestContexts.NO_OP)
                        .getEntity();

        assertThat(entity.getLatLng("at")).isEqualTo(LatLng.of(35.5d, -139.25d));
        assertThat(entity.getKey("owner"))
                .isEqualTo(
                        Key.newBuilder("p", "User", "u1", "db").setNamespace("tenant-a").build());
        assertThat(entity.getKey("child"))
                .isEqualTo(
                        Key.newBuilder("p", "Line", 3L, "db")
                                .setNamespace("tenant-a")
                                .addAncestors(
                                        PathElement.of("Root", "r"), PathElement.of("Mid", -7L))
                                .build());
        List<Value<?>> refs = entity.getList("refs");
        assertThat(refs)
                .extracting(value -> ((KeyValue) value).get())
                .containsExactly(
                        Key.newBuilder("p", "User", "u2", "db").setNamespace("tenant-a").build(),
                        Key.newBuilder("p", "User", 9L, "db").setNamespace("tenant-a").build());
        assertThat(entity.getValue("at").excludeFromIndexes()).isFalse();
        assertThat(entity.getValue("owner").excludeFromIndexes()).isFalse();
    }

    @Test
    void aNullOrEmptyAncestorsFieldWritesAKeyWithoutAParentAndNullsStayNull() throws IOException {
        FullEntity<Key> entity =
                serializer(marked(List.of()))
                        .serialize(
                                markedRow(
                                        null,
                                        null,
                                        GenericRowData.of(
                                                StringData.fromString("Line"),
                                                StringData.fromString("l"),
                                                null,
                                                null),
                                        null),
                                TestContexts.NO_OP)
                        .getEntity();
        FullEntity<Key> empty =
                serializer(marked(List.of()))
                        .serialize(
                                markedRow(
                                        null,
                                        null,
                                        GenericRowData.of(
                                                StringData.fromString("Line"),
                                                StringData.fromString("l"),
                                                null,
                                                new GenericArrayData(new Object[0])),
                                        null),
                                TestContexts.NO_OP)
                        .getEntity();

        assertThat(entity.getKey("child").getParent()).isNull();
        assertThat(empty.getKey("child")).isEqualTo(entity.getKey("child"));
        assertThat(entity.getValue("at").getType()).isEqualTo(ValueType.NULL);
        assertThat(entity.getValue("owner").getType()).isEqualTo(ValueType.NULL);
        assertThat(entity.getValue("refs").getType()).isEqualTo(ValueType.NULL);
    }

    @Test
    void anUnindexedColumnMarksItsGeoPointsAndKeyValues() throws IOException {
        FullEntity<Key> entity =
                serializer(marked(List.of("at", "owner", "refs")))
                        .serialize(
                                markedRow(
                                        GenericRowData.of(0d, 0d),
                                        element("User", "u1", null),
                                        null,
                                        new Object[] {element("User", "u2", null)}),
                                TestContexts.NO_OP)
                        .getEntity();

        assertThat((Object) entity.getValue("at")).isInstanceOf(LatLngValue.class);
        assertThat(entity.getValue("at").excludeFromIndexes()).isTrue();
        assertThat(entity.getValue("owner").excludeFromIndexes()).isTrue();
        assertThat(entity.getList("refs").get(0).excludeFromIndexes()).isTrue();
        assertThat(entity.getValue("refs").excludeFromIndexes()).isFalse();
    }

    @Test
    void aGeoPointWithANullOrOutOfRangeCoordinateFailsTheRecord() {
        RowDataSerializationSchema serializer = serializer(marked(List.of()));

        assertThatThrownBy(
                        () ->
                                serializer.serialize(
                                        markedRow(GenericRowData.of(1d, null), null, null, null),
                                        TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessage(
                        "Field 'at' is a geographical point, so its latitude and longitude must"
                                + " not be NULL; write a NULL point instead.");
        assertThatThrownBy(
                        () ->
                                serializer.serialize(
                                        markedRow(GenericRowData.of(90.5d, 0d), null, null, null),
                                        TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageStartingWith("Field 'at' is not a valid geographical point: latitude");
        assertThatThrownBy(
                        () ->
                                serializer.serialize(
                                        markedRow(GenericRowData.of(0d, -180.5d), null, null, null),
                                        TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageStartingWith("Field 'at' is not a valid geographical point:");
    }

    @Test
    void aKeyValueThatIsNotACompleteKeyFailsTheRecord() {
        RowDataSerializationSchema serializer = serializer(marked(List.of()));
        Object[][] cases = {
            {element("User", null, null), "its last path element has neither a name nor an id"},
            {element("User", "u", 1L), "its last path element has both a name and an id"},
            {element(null, "u", null), "its last path element has no kind"},
            {element("", "u", null), "its last path element has no kind"},
            {element("User", "", null), "name must not be empty or null"},
            {element("User", null, 0L), "id must not be equal to zero"},
        };
        for (Object[] c : cases) {
            assertThatThrownBy(
                            () ->
                                    serializer.serialize(
                                            markedRow(null, (GenericRowData) c[0], null, null),
                                            TestContexts.NO_OP))
                    .as((String) c[1])
                    .isInstanceOf(IOException.class)
                    .hasMessageStartingWith("Field 'owner' is")
                    .hasMessageContaining((String) c[1]);
        }
        assertThatThrownBy(
                        () ->
                                serializer.serialize(
                                        markedRow(
                                                null,
                                                null,
                                                GenericRowData.of(
                                                        StringData.fromString("Line"),
                                                        StringData.fromString("l"),
                                                        null,
                                                        new GenericArrayData(
                                                                new Object[] {
                                                                    element("Root", "r", null), null
                                                                })),
                                                null),
                                        TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessage(
                        "Field 'child' is a key value, but its ancestor 2 is NULL; every ancestor"
                                + " is a path element.");
        assertThatThrownBy(
                        () ->
                                serializer.serialize(
                                        markedRow(
                                                null,
                                                null,
                                                GenericRowData.of(
                                                        StringData.fromString("Line"),
                                                        StringData.fromString("l"),
                                                        null,
                                                        new GenericArrayData(
                                                                new Object[] {
                                                                    element("Root", null, null)
                                                                })),
                                                null),
                                        TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("its ancestor 1 has neither a name nor an id");
        assertThatThrownBy(
                        () ->
                                serializer.serialize(
                                        markedRow(
                                                null,
                                                null,
                                                null,
                                                new Object[] {element("User", "u", 2L)}),
                                        TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageStartingWith("Field 'refs' is a key value");
    }
}
