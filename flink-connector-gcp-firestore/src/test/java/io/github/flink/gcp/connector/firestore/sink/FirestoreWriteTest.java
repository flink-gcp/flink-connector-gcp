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
import com.google.cloud.firestore.FieldValue;
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

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
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
    void anAddIsACreateUnderAFreshDrawnIdInTheCollection() throws Exception {
        Map<String, Object> fields = Map.of("v", 1L);

        FirestoreWrite first = FirestoreWrite.add("users/alice/orders", fields);
        FirestoreWrite second = FirestoreWrite.add("users/alice/orders", fields);

        assertThat(first.getOperation()).isEqualTo(FirestoreWrite.Operation.CREATE);
        assertThat(first.hasDrawnId()).isTrue();
        assertThat(first.getDocumentPath()).matches("users/alice/orders/[A-Za-z0-9]{20}");
        assertThat(first.getFields()).isEqualTo(fields);
        assertThat(second.getDocumentPath()).isNotEqualTo(first.getDocumentPath());
        assertThat(first.toString()).contains("drawnId=true");
        assertThat(InstantiationUtil.clone(first)).isEqualTo(first);
        // The flag is part of the value: the writer draws again only for a drawn id.
        FirestoreWrite chosen = FirestoreWrite.create(first.getDocumentPath(), fields);
        assertThat(chosen.hasDrawnId()).isFalse();
        assertThat(chosen).isNotEqualTo(first);
        assertThat(chosen.toString()).doesNotContain("drawnId");
    }

    @Test
    void aDrawnIdUsesTheWholeAlphabet() {
        Set<Character> seen = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            String path = FirestoreWrite.add("c", Map.of()).getDocumentPath();
            for (char ch : path.substring(2).toCharArray()) {
                seen.add(ch);
            }
        }

        // 20,000 draws from 62 symbols miss one with probability below 10^-130.
        assertThat(seen).hasSize(62);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "c/a", "c/a/sub/b", "/c", "c/", "c//sub", "c/a//"})
    void aMalformedCollectionPathIsRejected(String path) {
        assertThatThrownBy(() -> FirestoreWrite.add(path, Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Collection path '" + path + "'");
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
        fields.put("reference", FirestoreDocumentReference.of("users/alice"));
        fields.put("vector", FieldValue.vector(new double[] {1, 2}));
        fields.put("a.b", "a field whose name contains a dot");

        FirestoreWrite write = FirestoreWrite.set("c/a", fields);
        list.add("after");
        nested.put("after", 1L);

        assertThat(write.getFields()).containsOnlyKeys(fields.keySet());
        assertThat(write.getFields().get("list")).isEqualTo(List.of("x", 2L));
        assertThat(write.getFields().get("map")).isEqualTo(Map.of("inner", true));
        assertThat(write.getFields().get("reference"))
                .isEqualTo(FirestoreDocumentReference.of("users/alice"));
        assertThat(write.getFields().get("vector"))
                .isEqualTo(FieldValue.vector(new double[] {1, 2}));
        assertThatThrownBy(() -> write.getFields().put("k", 1L))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(InstantiationUtil.clone(write)).isEqualTo(write);
    }

    @Test
    void aConvenienceTypeIsConvertedToTheValueItIsSentAs() throws Exception {
        java.sql.Timestamp sqlTimestamp = new java.sql.Timestamp(-500);
        sqlTimestamp.setNanos(500_000_123);
        Date date = new Date(1_500);
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("integer", Integer.MIN_VALUE);
        fields.put("float", 0.1f);
        fields.put("date", date);
        // Before the epoch, a millisecond count's remainder is negative: the client library sends
        // that as negative nanos, which the service refuses (googleapis/google-cloud-java#14635).
        fields.put("preEpochDate", new Date(-1_500));
        // Half a second before the epoch, with nanoseconds a Date cannot hold. The client library's
        // Timestamp.of(java.sql.Timestamp) reads it as half a second after the epoch
        // (googleapis/google-cloud-java#14636).
        fields.put("sqlTimestamp", sqlTimestamp);
        fields.put("instant", Instant.ofEpochSecond(-1, 999_999_999));
        fields.put("nested", List.of(Map.of("integer", 7)));

        FirestoreWrite write = FirestoreWrite.set("c/a", fields);
        date.setTime(0);

        assertThat(write.getFields().get("integer")).isEqualTo((long) Integer.MIN_VALUE);
        assertThat(write.getFields().get("float")).isEqualTo((double) 0.1f);
        assertThat(write.getFields().get("date"))
                .as("a copy, which the Date's later change does not reach")
                .isEqualTo(Timestamp.ofTimeSecondsAndNanos(1, 500_000_000));
        assertThat(write.getFields().get("preEpochDate"))
                .isEqualTo(Timestamp.ofTimeSecondsAndNanos(-2, 500_000_000));
        assertThat(write.getFields().get("sqlTimestamp"))
                .isEqualTo(Timestamp.ofTimeSecondsAndNanos(-1, 500_000_123));
        assertThat(write.getFields().get("instant"))
                .isEqualTo(Timestamp.ofTimeSecondsAndNanos(-1, 999_999_999));
        assertThat(write.getFields().get("nested")).isEqualTo(List.of(Map.of("integer", 7L)));
        assertThat(InstantiationUtil.clone(write)).isEqualTo(write);
    }

    @Test
    void aTimeATimestampCannotHoldIsRejectedAndNamed() {
        assertThatThrownBy(
                        () ->
                                FirestoreWrite.set(
                                        "c/a", Map.of("m", Map.of("d", new Date(Long.MAX_VALUE)))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'m.d'")
                .hasMessageContaining("java.util.Date")
                .hasMessageContaining("years 1 to 9999");
        assertThatThrownBy(() -> FirestoreWrite.set("c/a", Map.of("l", List.of(Instant.MIN))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'l[0]'")
                .hasMessageContaining("java.time.Instant")
                .hasMessageContaining("years 1 to 9999");
        assertThatThrownBy(
                        () ->
                                FirestoreWrite.set(
                                        "c/a",
                                        Map.of(
                                                "s",
                                                new java.sql.Timestamp(-70_000_000_000_000_000L))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'s'")
                .hasMessageContaining("java.sql.Timestamp")
                .hasMessageContaining("years 1 to 9999");
    }

    @Test
    void aValueOutsideTheVocabularyIsRejectedAndNamed() {
        // The client library itself refuses a Short, so it stays outside the vocabulary.
        assertThatThrownBy(() -> FirestoreWrite.set("c/a", Map.of("n", Map.of("s", (short) 1))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'n.s'")
                .hasMessageContaining("java.lang.Short")
                .hasMessageContaining("a short is written as a Long");
        // The library would store these as strings, a type other than the one written.
        assertThatThrownBy(() -> FirestoreWrite.set("c/a", Map.of("d", new BigDecimal("1.5"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'d'")
                .hasMessageContaining("java.math.BigDecimal")
                .hasMessageContaining("toString() or name()");
        assertThatThrownBy(
                        () -> FirestoreWrite.set("c/a", Map.of("e", FirestoreWrite.Operation.SET)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'e'")
                .hasMessageContaining(FirestoreWrite.Operation.class.getName());
        assertThatThrownBy(() -> FirestoreWrite.set("c/a", Map.of("l", List.of(new byte[0]))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'l[0]'");
        assertThatThrownBy(() -> FirestoreWrite.set("c/a", Map.of("s", Set.of("x"))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aClientLibraryDocumentReferenceIsRejectedWithTheValueThatReplacesIt() throws Exception {
        try (Firestore firestore =
                FirestoreOptions.newBuilder()
                        .setProjectId("p")
                        .setEmulatorHost("localhost:1")
                        .build()
                        .getService()) {
            DocumentReference reference = firestore.document("users/alice");

            assertThatThrownBy(() -> FirestoreWrite.set("c/a", Map.of("r", reference)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("'r'")
                    .hasMessageContaining("FirestoreDocumentReference.of(path)");
        }
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
        // and #1589 kept them outside the accepted values.
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
        // A value the library would refuse is rejected when the write is built, so that no accepted
        // value makes the library throw after it has queued an operation (ADR-0171). WriteBatch
        // converts synchronously through the same
        // encoder BulkWriter uses and sends nothing until commit, so an unreachable emulator
        // endpoint is enough. A FirestoreDocumentReference is the one accepted value the library
        // never sees: the writer replaces it with the library's DocumentReference first, which
        // FirestoreWriterITCase and DocumentSizeEstimatorITCase send.
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
        // An Integer, a Float, a Date and an Instant are not listed: the library sees only the
        // Long, Double and Timestamp they are converted to, which are.
        fields.put("vector", FieldValue.vector(new double[] {1}));
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
