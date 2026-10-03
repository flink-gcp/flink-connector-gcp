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
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.FieldPath;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;
import com.google.firestore.v1.Cursor;
import com.google.firestore.v1.StructuredQuery;
import io.github.flink.gcp.connector.firestore.TestDocuments;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

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
