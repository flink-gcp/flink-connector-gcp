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

package io.github.flink.gcp.connector.firestore.source.batch;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.Blob;
import com.google.cloud.firestore.BsonObjectId;
import com.google.cloud.firestore.BsonTimestamp;
import com.google.cloud.firestore.Decimal128Value;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.FieldPath;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Int32Value;
import com.google.cloud.firestore.MaxKey;
import com.google.cloud.firestore.MinKey;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.RegexValue;
import com.google.cloud.firestore.VectorValue;
import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.Cursor;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import io.github.flink.gcp.connector.firestore.TestDocuments;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Continuing a query after a document: the cursor is the client library's, and the {@code limit}
 * and {@code offset} are the connector's to rewrite. Offline — the client is never connected to.
 */
class QueryCursorsTest {

    private static final Timestamp READ_TIME = Timestamp.ofTimeSecondsAndNanos(1_700_000_000L, 0);

    private static Firestore client;

    @BeforeAll
    static void openClient() {
        client = TestDocuments.offlineClient("p");
    }

    @AfterAll
    static void closeClient() throws Exception {
        client.close();
    }

    private static DocumentSnapshot document(String path, Map<String, Object> fields) {
        return TestDocuments.document(client, path, fields, READ_TIME);
    }

    private static StructuredQuery structured(Query query) {
        return query.toProto().getStructuredQuery();
    }

    @Test
    void continuesAfterADocumentWithoutTouchingTheLimit() {
        Query base = client.collection("c").offset(2).limit(10);

        StructuredQuery continued = structured(QueryCursors.after(base, document("c/x", Map.of())));

        assertThat(continued.getLimit().getValue()).isEqualTo(10);
        assertThat(continued.getOffset()).isZero();
        assertThat(continued.getStartAt().getBefore()).isFalse();
    }

    @Test
    void leavesAQueryWithNothingPassedAsItIs() {
        Query base = client.collection("c").whereEqualTo("k", 1L).offset(3).limit(5);

        assertThat(QueryCursors.continueAfter(base, null, 0)).isEqualTo(base);
    }

    @Test
    void continuesAPartitionAfterTheDocumentAndKeepsItsEnd() {
        Query partition =
                client.collectionGroup("orders")
                        .orderBy(FieldPath.documentId())
                        .startAt(client.document("orders/a"))
                        .endBefore(client.document("orders/m"));

        StructuredQuery continued =
                structured(
                        QueryCursors.continueAfter(partition, document("orders/c", Map.of()), 1));

        Cursor start = continued.getStartAt();
        assertThat(start.getBefore()).as("after the document, not at it").isFalse();
        assertThat(start.getValuesList())
                .singleElement()
                .satisfies(
                        value ->
                                assertThat(value.getReferenceValue())
                                        .endsWith("/documents/orders/c"));
        assertThat(continued.getEndAt()).isEqualTo(structured(partition).getEndAt());
    }

    @Test
    void takesTheCursorInTheQuerysOwnOrderWithTheDocumentNameLast() {
        Query base = client.collection("c").orderBy("k", Query.Direction.DESCENDING);

        StructuredQuery continued =
                structured(QueryCursors.continueAfter(base, document("c/x", Map.of("k", 7L)), 1));

        assertThat(continued.getOrderByList())
                .extracting(order -> order.getField().getFieldPath())
                .containsExactly("k", "__name__");
        assertThat(continued.getStartAt().getValuesList()).hasSize(2);
        assertThat(continued.getStartAt().getValues(0).getIntegerValue()).isEqualTo(7L);
        assertThat(continued.getStartAt().getValues(1).getReferenceValue())
                .endsWith("/documents/c/x");
    }

    private static final Value NULL = Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();

    /**
     * The reserved single-key maps that google-cloud-firestore decodes into a value type of its own
     * rather than a {@code Map}, each with that type: the seven BSON types and a BSON binary {@code
     * Blob} since 3.49.0, and a vector before that.
     */
    static Stream<Arguments> reservedMapValues() {
        return Stream.of(
                Arguments.of(reserved("__min__", NULL), MinKey.class),
                Arguments.of(reserved("__max__", NULL), MaxKey.class),
                Arguments.of(
                        reserved(
                                "__regex__",
                                map(Map.of("pattern", string("^a.*"), "options", string("i")))),
                        RegexValue.class),
                Arguments.of(reserved("__int__", integer(-7L)), Int32Value.class),
                Arguments.of(reserved("__decimal128__", string("1.50")), Decimal128Value.class),
                Arguments.of(reserved("__binary__", bytes(0x80, 1, 2)), Blob.class),
                Arguments.of(
                        reserved("__oid__", string("507f1f77bcf86cd799439011")),
                        BsonObjectId.class),
                Arguments.of(
                        reserved(
                                "__request_timestamp__",
                                map(
                                        Map.of(
                                                "seconds",
                                                integer(1_700_000_000L),
                                                "increment",
                                                integer(3L)))),
                        BsonTimestamp.class),
                Arguments.of(
                        map(
                                Map.of(
                                        "__type__",
                                        string("__vector__"),
                                        "value",
                                        Value.newBuilder()
                                                .setArrayValue(
                                                        ArrayValue.newBuilder()
                                                                .addValues(doubleValue(1.5))
                                                                .addValues(doubleValue(-2.0)))
                                                .build())),
                        VectorValue.class));
    }

    @ParameterizedTest
    @MethodSource("reservedMapValues")
    void aCursorOnAValueTheLibraryDecodesToItsOwnTypeIsTheStoredValue(
            Value stored, Class<?> decodedType) {
        // startAfter decodes the ordered field into a Java value and encodes that value again, so
        // the cursor is the stored value only if the library's round trip is exact (#1589). An
        // unrecognised map would round-trip as a plain map, hence the type check.
        DocumentSnapshot document = document("c/x", Map.of("k", stored));
        assertThat(document.get("k")).isInstanceOf(decodedType);

        StructuredQuery continued =
                structured(
                        QueryCursors.continueAfter(
                                client.collection("c").orderBy("k"), document, 1));

        assertThat(continued.getStartAt().getValues(0)).isEqualTo(stored);
    }

    @Test
    void theLibraryRoundTripsTwoStoredFormsToAnotherValue() {
        // The limits ADR-0173 records: the library narrows an __int__ to 32 bits, and re-encodes a
        // subtype-0 __binary__ as plain bytes, so a cursor on either names another position.
        Query base = client.collection("c").orderBy("k");

        assertThat(cursorAfter(base, reserved("__int__", integer((1L << 40) + 5))))
                .isEqualTo(reserved("__int__", integer(5L)));
        assertThat(cursorAfter(base, reserved("__binary__", bytes(0, 1, 2))))
                .isEqualTo(bytes(1, 2));
    }

    @Test
    void aStoredValueTheLibraryCannotDecodeFailsWithoutTheProjectionHint() {
        Query base = client.collection("c").orderBy("k");
        Value outOfRange =
                reserved(
                        "__request_timestamp__",
                        map(Map.of("seconds", integer(-1L), "increment", integer(0L))));

        assertThatThrownBy(() -> cursorAfter(base, outOfRange))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("BsonTimestamp")
                .hasMessageNotContaining("projection");
        assertThatThrownBy(() -> cursorAfter(base, reserved("__binary__", bytes())))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    private static Value cursorAfter(Query base, Value stored) {
        return structured(QueryCursors.after(base, document("c/x", Map.of("k", stored))))
                .getStartAt()
                .getValues(0);
    }

    private static Value reserved(String key, Value value) {
        return map(Map.of(key, value));
    }

    private static Value map(Map<String, Value> fields) {
        return Value.newBuilder().setMapValue(MapValue.newBuilder().putAllFields(fields)).build();
    }

    private static Value string(String value) {
        return Value.newBuilder().setStringValue(value).build();
    }

    private static Value integer(long value) {
        return Value.newBuilder().setIntegerValue(value).build();
    }

    private static Value doubleValue(double value) {
        return Value.newBuilder().setDoubleValue(value).build();
    }

    /** A bytes value, each argument one byte. */
    private static Value bytes(int... values) {
        byte[] bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return Value.newBuilder().setBytesValue(ByteString.copyFrom(bytes)).build();
    }

    @Test
    void shrinksTheLimitByWhatWasPassedAndDropsTheOffset() {
        Query base = client.collection("c").offset(4).limit(10);

        StructuredQuery continued =
                structured(QueryCursors.continueAfter(base, document("c/x", Map.of()), 3));

        assertThat(continued.getLimit().getValue()).isEqualTo(7);
        assertThat(continued.getOffset()).isZero();
    }

    @Test
    void aLimitUsedUpBecomesZeroRatherThanNegative() {
        Query base = client.collection("c").limit(2);

        assertThat(
                        structured(QueryCursors.continueAfter(base, document("c/x", Map.of()), 5))
                                .getLimit()
                                .getValue())
                .isZero();
    }

    @Test
    void aQueryWithoutALimitGainsNone() {
        Query base = client.collection("c");

        assertThat(
                        structured(QueryCursors.continueAfter(base, document("c/x", Map.of()), 3))
                                .hasLimit())
                .isFalse();
        assertThat(QueryCursors.limitOf(base)).isNull();
    }

    @Test
    void refusesAProjectionThatLeavesOutAnOrderedField() {
        Query base = client.collection("c").orderBy("k").select("other");

        assertThatThrownBy(
                        () ->
                                QueryCursors.continueAfter(
                                        base, document("c/x", Map.of("other", "v")), 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("c/x")
                .hasMessageContaining("Add the field to the projection");
    }

    @Test
    void refusesACountThatDisagreesWithTheDocument() {
        Query base = client.collection("c");

        assertThatThrownBy(() -> QueryCursors.continueAfter(base, null, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> QueryCursors.continueAfter(base, document("c/x", Map.of()), -1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
