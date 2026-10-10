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

package io.github.flink.gcp.connector.firestore.table.sink;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.Blob;
import com.google.cloud.firestore.GeoPoint;
import io.github.flink.gcp.connector.firestore.sink.FirestoreDocumentReference;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWrite;
import io.github.flink.gcp.connector.firestore.table.FirestoreTableSchema;
import io.github.flink.gcp.connector.firestore.table.WriteMode;
import io.github.flink.gcp.connector.testutils.TestContexts;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RowDataSerializationSchemaTest {

    private static final DataType GEO =
            DataTypes.ROW(
                    DataTypes.FIELD("latitude", DataTypes.DOUBLE()),
                    DataTypes.FIELD("longitude", DataTypes.DOUBLE()));

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
                                            "attrs",
                                            DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())),
                                    DataTypes.FIELD(
                                            "nested",
                                            DataTypes.ROW(
                                                    DataTypes.FIELD("x", DataTypes.BIGINT()),
                                                    DataTypes.FIELD("where", GEO))),
                                    DataTypes.FIELD("location", GEO),
                                    DataTypes.FIELD("author", DataTypes.STRING()),
                                    DataTypes.FIELD("related", DataTypes.ARRAY(DataTypes.STRING())),
                                    DataTypes.FIELD("missing", DataTypes.STRING()))
                            .getLogicalType();

    private static FirestoreTableSchema everyTypeSchema(int[] key) {
        return FirestoreTableSchema.of(
                EVERY_TYPE, key, List.of("location", "nested.where"), List.of("author", "related"));
    }

    private static RowDataSerializationSchema serializer(
            FirestoreTableSchema schema, WriteMode mode) {
        return new RowDataSerializationSchema(schema, "users/alice/orders", mode);
    }

    private static GenericRowData everyTypeRow(String id) {
        Map<StringData, Long> attrs = new LinkedHashMap<>();
        attrs.put(StringData.fromString("k"), 7L);
        return GenericRowData.of(
                StringData.fromString(id),
                StringData.fromString("text"),
                true,
                42L,
                1.5d,
                new byte[] {1, 2},
                TimestampData.fromInstant(Instant.ofEpochSecond(-1, 123_456_789)),
                new GenericArrayData(new Object[] {StringData.fromString("a"), null}),
                new GenericMapData(attrs),
                GenericRowData.of(1L, GenericRowData.of(35.6, 139.7)),
                GenericRowData.of(-33.9, 151.2),
                StringData.fromString("users/bob"),
                new GenericArrayData(new Object[] {StringData.fromString("c/1")}),
                null);
    }

    @Test
    void everyTypeBecomesItsFirestoreValueAndTheKeyBecomesThePath() throws IOException {
        FirestoreWrite write =
                serializer(everyTypeSchema(new int[] {0}), WriteMode.SET)
                        .serialize(everyTypeRow("o1"), TestContexts.NO_OP);

        assertThat(write.getOperation()).isEqualTo(FirestoreWrite.Operation.SET);
        assertThat(write.getDocumentPath()).isEqualTo("users/alice/orders/o1");
        Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("s", "text");
        expected.put("b", true);
        expected.put("n", 42L);
        expected.put("d", 1.5d);
        expected.put("bytes", Blob.fromBytes(new byte[] {1, 2}));
        // A negative epoch second with a positive nano part, as Firestore's Timestamp holds it.
        expected.put("ts", Timestamp.ofTimeSecondsAndNanos(-1, 123_456_789));
        expected.put("tags", Arrays.asList("a", null));
        expected.put("attrs", Map.of("k", 7L));
        expected.put("nested", Map.of("x", 1L, "where", new GeoPoint(35.6, 139.7)));
        expected.put("location", new GeoPoint(-33.9, 151.2));
        expected.put("author", FirestoreDocumentReference.of("users/bob"));
        expected.put("related", List.of(FirestoreDocumentReference.of("c/1")));
        expected.put("missing", null);
        assertThat(write.getFields()).containsExactlyEntriesOf(expected);
    }

    @Test
    void theWriteModeChoosesTheOperationAndADeleteNeedsOnlyTheKey() throws IOException {
        FirestoreTableSchema schema = everyTypeSchema(new int[] {0});

        assertThat(
                        serializer(schema, WriteMode.MERGE)
                                .serialize(everyTypeRow("o1"), TestContexts.NO_OP)
                                .getOperation())
                .isEqualTo(FirestoreWrite.Operation.SET_MERGE);
        GenericRowData updateAfter = everyTypeRow("o1");
        updateAfter.setRowKind(RowKind.UPDATE_AFTER);
        assertThat(
                        serializer(schema, WriteMode.UPDATE)
                                .serialize(updateAfter, TestContexts.NO_OP)
                                .getOperation())
                .isEqualTo(FirestoreWrite.Operation.UPDATE);

        // A key-only delete, as Flink 2's upsert(true) changelog may deliver one.
        GenericRowData delete = new GenericRowData(RowKind.DELETE, EVERY_TYPE.getFieldCount());
        delete.setField(0, StringData.fromString("o1"));
        FirestoreWrite deleted =
                serializer(schema, WriteMode.SET).serialize(delete, TestContexts.NO_OP);
        assertThat(deleted.getOperation()).isEqualTo(FirestoreWrite.Operation.DELETE);
        assertThat(deleted.getDocumentPath()).isEqualTo("users/alice/orders/o1");
    }

    @Test
    void charBinaryAndMarkedMapValuesConvertAndAKeyOnlyDeleteIsWritten() throws IOException {
        RowType type =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.CHAR(3).notNull()),
                                        DataTypes.FIELD("code", DataTypes.CHAR(2)),
                                        DataTypes.FIELD("hash", DataTypes.BINARY(2)),
                                        DataTypes.FIELD("count", DataTypes.BIGINT().notNull()),
                                        DataTypes.FIELD(
                                                "owners",
                                                DataTypes.MAP(
                                                        DataTypes.STRING(), DataTypes.STRING())),
                                        DataTypes.FIELD(
                                                "places", DataTypes.MAP(DataTypes.STRING(), GEO)))
                                .getLogicalType();
        FirestoreTableSchema schema =
                FirestoreTableSchema.of(
                        type, new int[] {0}, List.of("places.value"), List.of("owners.value"));
        RowDataSerializationSchema serializer = serializer(schema, WriteMode.SET);
        Map<StringData, StringData> owners = new LinkedHashMap<>();
        owners.put(StringData.fromString("lead"), StringData.fromString("staff/alice"));
        Map<StringData, Object> places = new LinkedHashMap<>();
        places.put(StringData.fromString("home"), GenericRowData.of(1.0, 2.0));

        FirestoreWrite write =
                serializer.serialize(
                        GenericRowData.of(
                                StringData.fromString("abc"),
                                StringData.fromString("JP"),
                                new byte[] {9, 8},
                                5L,
                                new GenericMapData(owners),
                                new GenericMapData(places)),
                        TestContexts.NO_OP);

        assertThat(write.getDocumentPath()).isEqualTo("users/alice/orders/abc");
        assertThat(write.getFields())
                .containsEntry("code", "JP")
                .containsEntry("hash", Blob.fromBytes(new byte[] {9, 8}))
                .containsEntry(
                        "owners", Map.of("lead", FirestoreDocumentReference.of("staff/alice")))
                .containsEntry("places", Map.of("home", new GeoPoint(1.0, 2.0)));

        // A key-only delete leaves every other field null, the NOT NULL count included, as
        // Flink 2's upsert(true) changelog may deliver it.
        GenericRowData delete = new GenericRowData(RowKind.DELETE, type.getFieldCount());
        delete.setField(0, StringData.fromString("abc"));
        assertThat(serializer.serialize(delete, TestContexts.NO_OP).getOperation())
                .isEqualTo(FirestoreWrite.Operation.DELETE);
    }

    @Test
    void aDeleteUnderUpdateFailsTheRecordInsteadOfDeleting() {
        GenericRowData delete = new GenericRowData(RowKind.DELETE, EVERY_TYPE.getFieldCount());
        delete.setField(0, StringData.fromString("o1"));

        assertThatThrownBy(
                        () ->
                                serializer(everyTypeSchema(new int[] {0}), WriteMode.UPDATE)
                                        .serialize(delete, TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("which takes no deletes")
                .hasMessageContaining("Nothing was deleted");
    }

    @Test
    void aDeleteForATableWithoutAKeyFailsTheRecord() {
        GenericRowData delete = new GenericRowData(RowKind.DELETE, EVERY_TYPE.getFieldCount());
        delete.setField(0, StringData.fromString("o1"));

        // The sink declares insert-only without a key, so the planner sends none; the guard keeps
        // one that still arrives a routed failure rather than an index error from the writer.
        assertThatThrownBy(
                        () ->
                                serializer(everyTypeSchema(new int[0]), WriteMode.SET)
                                        .serialize(delete, TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("without a PRIMARY KEY, which has no document to delete");
    }

    @Test
    void withoutAKeyEveryRowIsAddedUnderAFreshDrawnId() throws IOException {
        RowDataSerializationSchema serializer =
                serializer(everyTypeSchema(new int[0]), WriteMode.SET);

        FirestoreWrite first = serializer.serialize(everyTypeRow("ignored"), TestContexts.NO_OP);
        FirestoreWrite second = serializer.serialize(everyTypeRow("ignored"), TestContexts.NO_OP);

        // A create, so a drawn id that names an existing document never replaces it: the writer
        // draws again.
        assertThat(first.getOperation()).isEqualTo(FirestoreWrite.Operation.CREATE);
        assertThat(first.hasDrawnId()).isTrue();
        assertThat(first.getDocumentPath()).matches("users/alice/orders/[A-Za-z0-9]{20}");
        assertThat(second.getDocumentPath()).isNotEqualTo(first.getDocumentPath());
        // Without a key the id column is an ordinary field.
        assertThat(first.getFields()).containsEntry("id", "ignored");
    }

    @Test
    void aKeyThatWouldNameAnotherDocumentIsRefusedNamingTheColumn() {
        RowDataSerializationSchema serializer =
                serializer(everyTypeSchema(new int[] {0}), WriteMode.SET);

        for (String id : List.of("", "a/b", "a/b/c")) {
            assertThatThrownBy(() -> serializer.serialize(everyTypeRow(id), TestContexts.NO_OP))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("PRIMARY KEY column 'id' holds '" + id + "'")
                    .hasMessageContaining("has no '/'");
        }
    }

    @Test
    void aValueWithoutAFirestoreFormFailsTheRecordNamingTheField() {
        RowDataSerializationSchema serializer =
                serializer(everyTypeSchema(new int[] {0}), WriteMode.SET);

        GenericRowData badReference = everyTypeRow("o1");
        badReference.setField(11, StringData.fromString("users"));
        assertThatThrownBy(() -> serializer.serialize(badReference, TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Field 'author' is a reference");

        GenericRowData outOfRange = everyTypeRow("o1");
        outOfRange.setField(10, GenericRowData.of(91.0, 0.0));
        assertThatThrownBy(() -> serializer.serialize(outOfRange, TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Field 'location' is not a valid geographical point");

        GenericRowData halfPoint = everyTypeRow("o1");
        halfPoint.setField(10, GenericRowData.of(1.0, null));
        assertThatThrownBy(() -> serializer.serialize(halfPoint, TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Field 'location' is a geographical point");

        Map<StringData, Long> nullKey = new HashMap<>();
        nullKey.put(null, 1L);
        GenericRowData badMap = everyTypeRow("o1");
        badMap.setField(8, new GenericMapData(nullKey));
        assertThatThrownBy(() -> serializer.serialize(badMap, TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Field 'attrs' is a map with a NULL key");
    }

    @Test
    void theSchemaSurvivesJavaSerialization() throws Exception {
        RowDataSerializationSchema serializer =
                new RowDataSerializationSchema(
                        everyTypeSchema(new int[] {0}), "users/alice/orders", WriteMode.MERGE);

        RowDataSerializationSchema copy = InstantiationUtil.clone(serializer);

        assertThat(copy).isEqualTo(serializer);
        assertThat(copy.serialize(everyTypeRow("o1"), TestContexts.NO_OP).getFields())
                .containsEntry("location", new GeoPoint(-33.9, 151.2));
    }
}
