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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import com.google.api.core.SettableApiFuture;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.Timestamp;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import io.github.flink.gcp.connector.firestore.TestDocuments;
import io.github.flink.gcp.connector.firestore.table.FirestoreTableSchema;
import io.github.flink.gcp.connector.firestore.table.TypeMismatchPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Drives both lookup functions over a scripted {@link DocumentLookup}. */
class RowDataLookupFunctionTest {

    private static final Timestamp READ_TIME = Timestamp.ofTimeSecondsAndNanos(1_700_000_000L, 0);

    private static final RowType TYPE =
            (RowType)
                    DataTypes.ROW(
                                    DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                                    DataTypes.FIELD("n", DataTypes.BIGINT()))
                            .getLogicalType();

    private static final FirestoreTableSchema SCHEMA =
            FirestoreTableSchema.of(TYPE, new int[] {0}, List.of(), List.of());

    private Firestore client;

    @BeforeEach
    void openClient() {
        client = TestDocuments.offlineClient("p");
    }

    @AfterEach
    void closeClient() throws Exception {
        client.close();
    }

    private static RowDataDeserializationSchema deserializer(TypeMismatchPolicy policy) {
        return new RowDataDeserializationSchema(
                SCHEMA, new int[] {0, 1}, List.of(), policy, InternalTypeInfo.of(TYPE));
    }

    private DocumentSnapshot document(String id, Object n) {
        return TestDocuments.document(client, "c/" + id, Map.of("n", n), READ_TIME);
    }

    private static RowData key(String id) {
        return GenericRowData.of(id == null ? null : StringData.fromString(id));
    }

    private static RowDataLookupFunction sync(int maxRetries, FakeDocumentLookup lookup)
            throws Exception {
        RowDataLookupFunction function =
                new RowDataLookupFunction(
                        deserializer(TypeMismatchPolicy.FAIL), maxRetries, lookup);
        function.open(null);
        return function;
    }

    private static RowDataAsyncLookupFunction async(int maxRetries, FakeDocumentLookup lookup)
            throws Exception {
        RowDataAsyncLookupFunction function =
                new RowDataAsyncLookupFunction(
                        deserializer(TypeMismatchPolicy.FAIL), maxRetries, lookup);
        function.open(null);
        return function;
    }

    @Test
    void theDocumentIsReadAsItsRow() throws Exception {
        FakeDocumentLookup lookup = new FakeDocumentLookup().then(document("a", 7L));

        assertThat(sync(0, lookup).lookup(key("a")))
                .containsExactly(GenericRowData.of(StringData.fromString("a"), 7L));
        assertThat(lookup.reads()).containsExactly("a");
        assertThat(lookup.opened()).isTrue();
    }

    @Test
    void aKeyThatNamesNoDocumentOfTheCollectionIsNeverRead() throws Exception {
        FakeDocumentLookup lookup = new FakeDocumentLookup();
        RowDataLookupFunction sync = sync(0, lookup);
        RowDataAsyncLookupFunction async = async(0, lookup);

        String tooLong = "x".repeat(1501);
        for (String id :
                new String[] {
                    null, "", "a/sub/b", "a/", "/a", "a/b", ".", "..", "____", "__x__", tooLong
                }) {
            assertThat(sync.lookup(key(id))).as("%s", id).isEmpty();
            assertThat(async.asyncLookup(key(id)).get(10, SECONDS)).as("%s", id).isEmpty();
        }
        assertThat(lookup.reads()).isEmpty();
    }

    @Test
    void anIdTheServiceCanStoreIsRead() throws Exception {
        String longest = "\u00e9".repeat(750); // 1,500 UTF-8 bytes
        FakeDocumentLookup lookup = new FakeDocumentLookup();
        for (String id : new String[] {"a.b", "...", "__x", "x__", "__", "___", longest}) {
            lookup.then(document("d", 1L));
        }
        RowDataLookupFunction sync = sync(0, lookup);

        for (String id : new String[] {"a.b", "...", "__x", "x__", "__", "___", longest}) {
            sync.lookup(key(id));
        }
        assertThat(lookup.reads()).hasSize(7);
    }

    @ParameterizedTest
    @EnumSource(
            value = StatusCode.Code.class,
            names = {"UNAVAILABLE", "INTERNAL", "DEADLINE_EXCEEDED"})
    void aTransientFailureIsReadAgainWithinTheBudget(StatusCode.Code code) throws Exception {
        FakeDocumentLookup syncLookup =
                new FakeDocumentLookup()
                        .then(FakeDocumentLookup.failure(code))
                        .then(FakeDocumentLookup.failure(code))
                        .then(document("a", 1L));
        FakeDocumentLookup asyncLookup =
                new FakeDocumentLookup()
                        .then(FakeDocumentLookup.failure(code))
                        .then(FakeDocumentLookup.failure(code))
                        .then(document("a", 1L));

        assertThat(sync(2, syncLookup).lookup(key("a"))).hasSize(1);
        assertThat(async(2, asyncLookup).asyncLookup(key("a")).get(10, SECONDS)).hasSize(1);
        assertThat(syncLookup.reads()).hasSize(3);
        assertThat(asyncLookup.reads()).hasSize(3);
    }

    @Test
    void anExhaustedBudgetSurfacesTheLastFailure() throws Exception {
        FakeDocumentLookup syncLookup =
                new FakeDocumentLookup()
                        .then(FakeDocumentLookup.failure(StatusCode.Code.UNAVAILABLE))
                        .then(FakeDocumentLookup.failure(StatusCode.Code.DEADLINE_EXCEEDED));
        FakeDocumentLookup asyncLookup =
                new FakeDocumentLookup()
                        .then(FakeDocumentLookup.failure(StatusCode.Code.UNAVAILABLE))
                        .then(FakeDocumentLookup.failure(StatusCode.Code.DEADLINE_EXCEEDED));

        // The blocking read rethrows the library's exception itself, not an ExecutionException.
        assertThatThrownBy(() -> sync(1, syncLookup).lookup(key("a")))
                .isInstanceOf(ApiException.class)
                .hasMessageContaining("DEADLINE_EXCEEDED");
        assertThatThrownBy(() -> async(1, asyncLookup).asyncLookup(key("a")).get(10, SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("DEADLINE_EXCEEDED");
        assertThat(syncLookup.reads()).hasSize(2);
        assertThat(asyncLookup.reads()).hasSize(2);
    }

    @ParameterizedTest
    @EnumSource(
            value = StatusCode.Code.class,
            names = {"RESOURCE_EXHAUSTED", "PERMISSION_DENIED", "INVALID_ARGUMENT", "ABORTED"})
    void aPermanentFailureIsNotReadAgain(StatusCode.Code code) throws Exception {
        FakeDocumentLookup syncLookup =
                new FakeDocumentLookup().then(FakeDocumentLookup.failure(code));
        FakeDocumentLookup asyncLookup =
                new FakeDocumentLookup().then(FakeDocumentLookup.failure(code));

        assertThatThrownBy(() -> sync(3, syncLookup).lookup(key("a")))
                .hasMessageContaining(code.name());
        assertThatThrownBy(() -> async(3, asyncLookup).asyncLookup(key("a")).get(10, SECONDS))
                .hasMessageContaining(code.name());
        assertThat(syncLookup.reads()).hasSize(1);
        assertThat(asyncLookup.reads()).hasSize(1);
    }

    @Test
    void aMismatchFailsTheLookupNamingTheDocument() throws Exception {
        FakeDocumentLookup syncLookup = new FakeDocumentLookup().then(document("bad", "text"));
        FakeDocumentLookup asyncLookup = new FakeDocumentLookup().then(document("bad", "text"));

        assertThatThrownBy(() -> sync(3, syncLookup).lookup(key("bad")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Document 'c/bad' cannot be read into the table");
        assertThatThrownBy(() -> async(3, asyncLookup).asyncLookup(key("bad")).get(10, SECONDS))
                .hasCauseInstanceOf(IOException.class)
                .hasMessageContaining("Document 'c/bad' cannot be read into the table");
        assertThat(syncLookup.reads()).hasSize(1);
    }

    @Test
    void anAsyncLookupWaitsForALaterCallbackAndRetriesALaterFailure() throws Exception {
        SettableApiFuture<DocumentSnapshot> first = SettableApiFuture.create();
        SettableApiFuture<DocumentSnapshot> second = SettableApiFuture.create();
        FakeDocumentLookup lookup = new FakeDocumentLookup().then(first).then(second);

        CompletableFuture<Collection<RowData>> result = async(1, lookup).asyncLookup(key("a"));
        assertThat(result).isNotDone();

        first.setException(FakeDocumentLookup.failure(StatusCode.Code.UNAVAILABLE));
        assertThat(result).isNotDone();
        assertThat(lookup.reads()).containsExactly("a", "a");

        second.set(document("a", 5L));
        assertThat(result.get(10, SECONDS))
                .containsExactly(GenericRowData.of(StringData.fromString("a"), 5L));
    }

    @Test
    void aReadThatThrowsAtOnceIsRetriedLikeAFailedOne() throws Exception {
        FakeDocumentLookup lookup =
                new FakeDocumentLookup()
                        .then(
                                new FakeDocumentLookup.Thrown(
                                        FakeDocumentLookup.failure(StatusCode.Code.UNAVAILABLE)))
                        .then(document("a", 1L));

        assertThat(async(1, lookup).asyncLookup(key("a")).get(10, SECONDS)).hasSize(1);
        assertThat(lookup.reads()).hasSize(2);
    }

    @Test
    void immediateFailuresDoNotGrowTheTaskThreadStack() throws Exception {
        int budget = 5_000;
        FakeDocumentLookup lookup = new FakeDocumentLookup();
        for (int i = 0; i < budget; i++) {
            lookup.then(FakeDocumentLookup.failure(StatusCode.Code.UNAVAILABLE));
        }
        lookup.then(document("a", 1L));

        RowDataAsyncLookupFunction function = async(budget, lookup);
        // A thread with a small stack, so recursion overflows it whatever the default is.
        CompletableFuture<Collection<RowData>> result = new CompletableFuture<>();
        Thread caller =
                new Thread(
                        null,
                        () -> {
                            try {
                                function.asyncLookup(key("a"))
                                        .whenComplete(
                                                (rows, failure) -> {
                                                    if (failure != null) {
                                                        result.completeExceptionally(failure);
                                                    } else {
                                                        result.complete(rows);
                                                    }
                                                });
                            } catch (Throwable failure) {
                                result.completeExceptionally(failure);
                            }
                        },
                        "small-stack-lookup",
                        256 * 1024);
        caller.start();
        caller.join(10_000);

        assertThat(result.get(10, SECONDS)).hasSize(1);
        assertThat(lookup.reads()).hasSize(budget + 1);
    }

    @Test
    void closingEitherFunctionClosesItsLookup() throws Exception {
        FakeDocumentLookup syncLookup = new FakeDocumentLookup();
        FakeDocumentLookup asyncLookup = new FakeDocumentLookup();
        sync(0, syncLookup).close();
        async(0, asyncLookup).close();

        assertThat(syncLookup.closed()).isTrue();
        assertThat(asyncLookup.closed()).isTrue();
    }
}
