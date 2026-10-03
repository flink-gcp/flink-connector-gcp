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

package io.github.flink.gcp.connector.firestore.sink;

import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.Blob;
import com.google.cloud.firestore.BsonObjectId;
import com.google.cloud.firestore.BsonTimestamp;
import com.google.cloud.firestore.Decimal128Value;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.FieldPath;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.GeoPoint;
import com.google.cloud.firestore.Int32Value;
import com.google.cloud.firestore.MaxKey;
import com.google.cloud.firestore.MinKey;
import com.google.cloud.firestore.RegexValue;
import com.google.cloud.firestore.SetOptions;
import com.google.cloud.firestore.WriteBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FirestoreWriteTest {

    private static final Timestamp TIME = Timestamp.ofTimeSecondsAndNanos(1_700_000_000, 5);

    @Test
    void everyOperationCarriesItsPathFieldsAndPrecondition() {
        Map<String, Object> fields = Map.of("v", 1L);

        assertThat(FirestoreWrite.set("c/a", fields).getOperation())
                .isEqualTo(FirestoreWrite.Operation.SET);
        assertThat(FirestoreWrite.setMerge("c/a", fields).getOperation())
                .isEqualTo(FirestoreWrite.Operation.SET_MERGE);
        assertThat(FirestoreWrite.create("c/a", fields).getOperation())
                .isEqualTo(FirestoreWrite.Operation.CREATE);
        assertThat(FirestoreWrite.update("c/a", fields).getLastUpdateTime()).isNull();
        assertThat(FirestoreWrite.update("c/a", fields, TIME).getLastUpdateTime()).isEqualTo(TIME);
        assertThat(FirestoreWrite.delete("c/a").getFields()).isEmpty();
        assertThat(FirestoreWrite.delete("c/a", TIME).getLastUpdateTime()).isEqualTo(TIME);
        assertThat(FirestoreWrite.set("c/a/sub/b", fields).getDocumentPath())
                .isEqualTo("c/a/sub/b");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "c", "c/a/sub", "/c/a", "c/a/", "c//a/b", "c/a//"})
    void aMalformedDocumentPathIsRejected(String path) {
        assertThatThrownBy(() -> FirestoreWrite.set(path, Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Document path");
    }

    @Test
    void anUpdateMustNameAField() {
        assertThatThrownBy(() -> FirestoreWrite.update("c/a", Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one field");
    }

    @Test
    void everyValueOfTheVocabularyIsAcceptedAndCopied() throws Exception {
        List<Object> list = new ArrayList<>(List.of("x", 2L));
        Map<String, Object> nested = new HashMap<>(Map.of("inner", true));
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("null", null);
        fields.put("string", "s");
        fields.put("long", 1L);
        fields.put("double", 1.5d);
        fields.put("boolean", false);
        fields.put("timestamp", TIME);
        fields.put("geoPoint", new GeoPoint(35.6, 139.7));
        fields.put("blob", Blob.fromBytes(new byte[] {1, 2}));
        fields.put("list", list);
        fields.put("map", nested);
        fields.put("a.b", "a field whose name contains a dot");

        FirestoreWrite write = FirestoreWrite.set("c/a", fields);
        list.add("after");
        nested.put("after", 1L);

        assertThat(write.getFields()).containsOnlyKeys(fields.keySet());
        assertThat(write.getFields().get("list")).isEqualTo(List.of("x", 2L));
        assertThat(write.getFields().get("map")).isEqualTo(Map.of("inner", true));
        assertThatThrownBy(() -> write.getFields().put("k", 1L))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(InstantiationUtil.clone(write)).isEqualTo(write);
    }

    @Test
    void aValueOutsideTheVocabularyIsRejectedAndNamed() {
        assertThatThrownBy(() -> FirestoreWrite.set("c/a", Map.of("n", Map.of("i", 1))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'n.i'")
                .hasMessageContaining("java.lang.Integer");
        assertThatThrownBy(() -> FirestoreWrite.set("c/a", Map.of("l", List.of(new byte[0]))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'l[0]'");
        assertThatThrownBy(() -> FirestoreWrite.set("c/a", Map.of("s", Set.of("x"))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anEmptyOrNonStringFieldNameIsRejected() {
        assertThatThrownBy(() -> FirestoreWrite.set("c/a", Map.of("", 1L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-empty strings");
        assertThatThrownBy(
                        () ->
                                FirestoreWrite.set(
                                        "c/a", Map.of("m", Collections.singletonMap(1, 1L))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("field 'm'");
    }

    @Test
    void nestingIsBoundedBelowTheClientLibrarysOwnLimit() {
        Object value = 1L;
        for (int depth = 1; depth < FirestoreWrite.MAX_NESTING_DEPTH; depth++) {
            value = List.of(value);
        }
        Object deepest = value;

        assertThat(FirestoreWrite.set("c/a", Map.of("v", deepest))).isNotNull();
        assertThatThrownBy(() -> FirestoreWrite.set("c/a", Map.of("v", List.of(deepest))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nested deeper");
    }

    @Test
    void aBsonBinaryBlobOfAnotherSubtypeIsRejectedAndSubtypeZeroIsAccepted() {
        // A subtype other than 0 is encoded as a reserved map rather than bytes, which the writer's
        // size accounting does not count (#1589); subtype 0 is the plain bytes value.
        assertThat(FirestoreWrite.set("c/a", Map.of("v", Blob.createBsonBinary(0, new byte[] {1}))))
                .isNotNull();
        assertThatThrownBy(
                        () ->
                                FirestoreWrite.set(
                                        "c/a",
                                        Map.of(
                                                "outer",
                                                List.of(
                                                        Blob.createBsonBinary(
                                                                128, new byte[] {1})))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outer[0]")
                .hasMessageContaining("subtype 128")
                .hasMessageContaining("Blob.fromBytes");
    }

    static Stream<Object> bsonValues() {
        return Stream.of(
                MinKey.instance(),
                MaxKey.instance(),
                new RegexValue("^a.*", "i"),
                new BsonObjectId("507f1f77bcf86cd799439011"),
                new BsonTimestamp(1_700_000_000L, 3L),
                new Int32Value(7),
                new Decimal128Value("1.50"));
    }

    @ParameterizedTest
    @MethodSource("bsonValues")
    void aBsonValueClassIsRejected(Object value) {
        // The library encodes each as a reserved map the writer's size accounting does not count,
        // and #1589 kept them outside the closed list.
        assertThatThrownBy(() -> FirestoreWrite.set("c/a", Map.of("v", value)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(value.getClass().getName());
    }

    @Test
    void toStringNamesTheFieldsButNotTheirValues() {
        assertThat(FirestoreWrite.update("c/a", Map.of("secret", "value"), TIME).toString())
                .contains("UPDATE", "c/a", "secret", "lastUpdateTime")
                .doesNotContain("value");
    }

    @Test
    void everyAcceptedValueIsOneTheClientLibraryEncodes() throws Exception {
        // The closed vocabulary exists so that no accepted value makes the library throw after it
        // has queued an operation (ADR-0171). WriteBatch converts synchronously through the same
        // encoder BulkWriter uses and sends nothing until commit, so an unreachable emulator
        // endpoint is enough.
        Object deepest = 1L;
        for (int depth = 1; depth < FirestoreWrite.MAX_NESTING_DEPTH; depth++) {
            deepest = List.of(deepest);
        }
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("null", null);
        fields.put("string", "s");
        fields.put("long", 1L);
        fields.put("double", 1.5d);
        fields.put("boolean", true);
        fields.put("timestamp", TIME);
        fields.put("geoPoint", new GeoPoint(35.6, 139.7));
        fields.put("blob", Blob.fromBytes(new byte[] {1}));
        fields.put("list", List.of("x", Map.of("k", 1L)));
        fields.put("map", Map.of("inner", List.of(2L)));
        fields.put("a.b", "dotted");
        fields.put("deepest", deepest);
        Map<String, Object> copied = FirestoreWrite.set("c/a", fields).getFields();

        try (Firestore firestore =
                FirestoreOptions.newBuilder()
                        .setProjectId("p")
                        .setEmulatorHost("localhost:1")
                        .build()
                        .getService()) {
            DocumentReference document = firestore.document("c/a");
            WriteBatch batch = firestore.batch();
            batch.set(document, copied);
            batch.set(document, copied, SetOptions.merge());
            batch.create(document, copied);
            for (Map.Entry<String, Object> entry : copied.entrySet()) {
                batch.update(document, FieldPath.of(entry.getKey()), entry.getValue());
            }
        }
    }
}
