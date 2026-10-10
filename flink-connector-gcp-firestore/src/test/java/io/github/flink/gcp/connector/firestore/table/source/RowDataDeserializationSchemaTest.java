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

package io.github.flink.gcp.connector.firestore.table.source;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.Value;
import io.github.flink.gcp.connector.firestore.TestDocuments;
import io.github.flink.gcp.connector.firestore.table.FirestoreTableSchema;
import io.github.flink.gcp.connector.firestore.table.TypeMismatchPolicy;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RowDataDeserializationSchemaTest {

    private static final Timestamp READ_TIME =
            Timestamp.ofTimeSecondsAndNanos(1_700_000_000L, 5000);

    private static final RowType TYPE =
            (RowType)
                    DataTypes.ROW(
                                    DataTypes.FIELD("n", DataTypes.BIGINT()),
                                    DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                                    DataTypes.FIELD("a.b", DataTypes.STRING()),
                                    DataTypes.FIELD("missing", DataTypes.STRING()))
                            .getLogicalType();

    private static final FirestoreTableSchema SCHEMA =
            FirestoreTableSchema.of(TYPE, new int[] {1}, List.of(), List.of());

    private static RowDataDeserializationSchema schema(
            int[] columns, List<String> metadata, TypeMismatchPolicy policy) {
        TypeInformation<RowData> type = InternalTypeInfo.of(TYPE);
        return new RowDataDeserializationSchema(SCHEMA, columns, metadata, policy, type);
    }

    private static List<RowData> read(RowDataDeserializationSchema schema, DocumentSnapshot doc)
            throws IOException {
        List<RowData> rows = new ArrayList<>();
        schema.deserialize(doc, new ListCollector<>(rows));
        return rows;
    }

    @Test
    void theKeyIsTheIdTheFieldsAreLiteralAndMetadataFollowsInOrder() throws Exception {
        try (Firestore client = TestDocuments.offlineClient("p")) {
            DocumentSnapshot doc =
                    TestDocuments.document(
                            client,
                            "users/alice/orders/o1",
                            Map.of("n", 3L, "a.b", "dotted"),
                            READ_TIME);

            List<RowData> rows =
                    read(
                            schema(
                                    new int[] {2, 1, 0, 3},
                                    List.of("read-time", "document-path"),
                                    TypeMismatchPolicy.FAIL),
                            doc);

            assertThat(rows)
                    .containsExactly(
                            GenericRowData.of(
                                    StringData.fromString("dotted"),
                                    StringData.fromString("o1"),
                                    3L,
                                    null,
                                    TimestampData.fromEpochMillis(1_700_000_000_000L, 5000),
                                    StringData.fromString("users/alice/orders/o1")));
        }
    }

    @Test
    void eachMetadataTimeIsItsOwnWithItsMilliseconds() throws Exception {
        try (Firestore client = TestDocuments.offlineClient("p")) {
            DocumentSnapshot doc =
                    TestDocuments.document(
                            client,
                            "c/d",
                            Map.of(),
                            Timestamp.ofTimeSecondsAndNanos(10, 123_456_000),
                            Timestamp.ofTimeSecondsAndNanos(20, 234_567_000),
                            Timestamp.ofTimeSecondsAndNanos(30, 345_678_000));

            assertThat(
                            read(
                                    schema(
                                            new int[] {1},
                                            List.of("create-time", "update-time", "read-time"),
                                            TypeMismatchPolicy.FAIL),
                                    doc))
                    .containsExactly(
                            GenericRowData.of(
                                    StringData.fromString("d"),
                                    TimestampData.fromEpochMillis(10_123, 456_000),
                                    TimestampData.fromEpochMillis(20_234, 567_000),
                                    TimestampData.fromEpochMillis(30_345, 678_000)));
        }
    }

    @Test
    void theKeyIsTheDocumentIdEvenBesideAFieldOfTheSameName() throws Exception {
        try (Firestore client = TestDocuments.offlineClient("p")) {
            DocumentSnapshot doc =
                    TestDocuments.document(client, "c/real-id", Map.of("id", "a field"), READ_TIME);

            assertThat(read(schema(new int[] {1}, List.of(), TypeMismatchPolicy.FAIL), doc))
                    .containsExactly(GenericRowData.of(StringData.fromString("real-id")));
        }
    }

    @Test
    void aNotNullColumnTheDocumentLacksFailsUnderEitherPolicyWithoutThePolicyRemedy()
            throws Exception {
        RowType type =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                                        DataTypes.FIELD("must", DataTypes.BIGINT().notNull()))
                                .getLogicalType();
        FirestoreTableSchema schema =
                FirestoreTableSchema.of(type, new int[] {0}, List.of(), List.of());
        try (Firestore client = TestDocuments.offlineClient("p")) {
            DocumentSnapshot doc = TestDocuments.document(client, "c/d", Map.of(), READ_TIME);
            for (TypeMismatchPolicy policy : TypeMismatchPolicy.values()) {
                RowDataDeserializationSchema deserializer =
                        new RowDataDeserializationSchema(
                                schema,
                                new int[] {1},
                                List.of(),
                                policy,
                                InternalTypeInfo.of(type));
                assertThatThrownBy(() -> read(deserializer, doc))
                        .as("%s", policy)
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining(
                                "is missing or holds null, but the table declares it NOT NULL")
                        .hasMessageContaining("The column 'must' is NOT NULL, so no policy")
                        .hasMessageNotContaining("'type-mismatch-policy' = 'null'");
            }
        }
    }

    @Test
    void aMismatchedValueInANotNullColumnIsNotOfferedThePolicy() throws Exception {
        RowType type =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                                        DataTypes.FIELD("must", DataTypes.BIGINT().notNull()))
                                .getLogicalType();
        FirestoreTableSchema schema =
                FirestoreTableSchema.of(type, new int[] {0}, List.of(), List.of());
        try (Firestore client = TestDocuments.offlineClient("p")) {
            DocumentSnapshot doc =
                    TestDocuments.document(client, "c/d", Map.of("must", "text"), READ_TIME);
            for (TypeMismatchPolicy policy : TypeMismatchPolicy.values()) {
                RowDataDeserializationSchema deserializer =
                        new RowDataDeserializationSchema(
                                schema,
                                new int[] {1},
                                List.of(),
                                policy,
                                InternalTypeInfo.of(type));
                assertThatThrownBy(() -> read(deserializer, doc))
                        .as("%s", policy)
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining("Field 'must' holds a value of type java.lang.String")
                        .hasMessageContaining("The column 'must' is NOT NULL, so no policy")
                        .hasMessageNotContaining("'type-mismatch-policy' = 'null'");
            }
        }
    }

    @Test
    void aMismatchInsideANullableElementOfANotNullColumnOffersThePolicy() throws Exception {
        RowType type =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                                        DataTypes.FIELD(
                                                "tags",
                                                DataTypes.ARRAY(DataTypes.STRING()).notNull()))
                                .getLogicalType();
        FirestoreTableSchema schema =
                FirestoreTableSchema.of(type, new int[] {0}, List.of(), List.of());
        Value tags =
                Value.newBuilder()
                        .setArrayValue(
                                ArrayValue.newBuilder()
                                        .addValues(Value.newBuilder().setStringValue("a"))
                                        .addValues(Value.newBuilder().setIntegerValue(1)))
                        .build();
        try (Firestore client = TestDocuments.offlineClient("p")) {
            DocumentSnapshot doc =
                    TestDocuments.document(client, "c/d", Map.of("tags", tags), READ_TIME);

            assertThatThrownBy(
                            () ->
                                    read(
                                            new RowDataDeserializationSchema(
                                                    schema,
                                                    new int[] {1},
                                                    List.of(),
                                                    TypeMismatchPolicy.FAIL,
                                                    InternalTypeInfo.of(type)),
                                            doc))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("'type-mismatch-policy' = 'null'")
                    .hasMessageNotContaining("no policy");
            assertThat(
                            read(
                                    new RowDataDeserializationSchema(
                                            schema,
                                            new int[] {1},
                                            List.of(),
                                            TypeMismatchPolicy.NULL,
                                            InternalTypeInfo.of(type)),
                                    doc))
                    .containsExactly(
                            GenericRowData.of(
                                    new GenericArrayData(
                                            new Object[] {StringData.fromString("a"), null})));
        }
    }

    @Test
    void aReferenceIntoAnotherDatabaseIsAMismatch() throws Exception {
        RowType type =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                                        DataTypes.FIELD("ref", DataTypes.STRING()))
                                .getLogicalType();
        FirestoreTableSchema schema =
                FirestoreTableSchema.of(type, new int[] {0}, List.of(), List.of("ref"));
        RowDataDeserializationSchema deserializer =
                new RowDataDeserializationSchema(
                        schema,
                        new int[] {1},
                        List.of(),
                        TypeMismatchPolicy.FAIL,
                        InternalTypeInfo.of(type));
        try (Firestore client = TestDocuments.offlineClient("p")) {
            DocumentSnapshot here =
                    TestDocuments.document(
                            client,
                            "c/here",
                            Map.of(
                                    "ref",
                                    reference("projects/p/databases/(default)/documents/x/y")),
                            READ_TIME);
            DocumentSnapshot elsewhere =
                    TestDocuments.document(
                            client,
                            "c/elsewhere",
                            Map.of("ref", reference("projects/p/databases/other/documents/x/y")),
                            READ_TIME);

            assertThat(read(deserializer, here))
                    .containsExactly(GenericRowData.of(StringData.fromString("x/y")));
            assertThatThrownBy(() -> read(deserializer, elsewhere))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("a reference into another database");
        }
    }

    private static Value reference(String name) {
        return Value.newBuilder().setReferenceValue(name).build();
    }

    @Test
    void aMismatchNamesTheDocumentAndTheRemedy() throws Exception {
        try (Firestore client = TestDocuments.offlineClient("p")) {
            DocumentSnapshot doc =
                    TestDocuments.document(client, "c/bad", Map.of("n", "text"), READ_TIME);

            assertThatThrownBy(
                            () ->
                                    read(
                                            schema(
                                                    new int[] {0},
                                                    List.of(),
                                                    TypeMismatchPolicy.FAIL),
                                            doc))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Document 'c/bad' cannot be read into the table")
                    .hasMessageContaining("Field 'n' holds a value of type java.lang.String")
                    .hasMessageContaining("'type-mismatch-policy' = 'null'");
            assertThat(read(schema(new int[] {0}, List.of(), TypeMismatchPolicy.NULL), doc))
                    .containsExactly(GenericRowData.of((Object) null));
        }
    }

    @Test
    void theSchemaSurvivesJavaSerialization() throws Exception {
        RowDataDeserializationSchema schema =
                schema(new int[] {1, 0}, List.of("update-time"), TypeMismatchPolicy.NULL);

        assertThat(InstantiationUtil.clone(schema)).isEqualTo(schema);
        assertThat(schema)
                .isNotEqualTo(
                        schema(new int[] {1, 0}, List.of("update-time"), TypeMismatchPolicy.FAIL));
    }

    @Test
    void anUndeclaredMetadataKeyIsRefused() {
        assertThatThrownBy(() -> schema(new int[] {0}, List.of("version"), TypeMismatchPolicy.FAIL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no readable metadata 'version'");
    }
}
