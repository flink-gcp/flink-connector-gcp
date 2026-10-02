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

package io.github.flink.gcp.connector.firestore.source.batch.reader;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.util.Collector;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;
import io.github.flink.gcp.connector.firestore.FirestoreMetricNames;
import io.github.flink.gcp.connector.firestore.TestDocuments;
import io.github.flink.gcp.connector.firestore.source.batch.FetchedDocument;
import io.github.flink.gcp.connector.firestore.source.batch.QuerySplit;
import io.github.flink.gcp.connector.firestore.source.batch.QuerySplitState;
import io.github.flink.gcp.connector.firestore.source.serializer.FirestoreDocumentDeserializationSchema;
import io.github.flink.gcp.connector.testutils.CollectingSourceOutput;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FirestoreRecordEmitter}. */
@Timeout(30)
class FirestoreRecordEmitterTest {

    private static final Timestamp READ_TIME = Timestamp.ofTimeSecondsAndNanos(1_700_000_000L, 0);

    private static Firestore client;

    private final TestReaderMetrics metrics = new TestReaderMetrics();

    @BeforeAll
    static void openClient() {
        client = TestDocuments.offlineClient("p");
    }

    @AfterAll
    static void closeClient() throws Exception {
        client.close();
    }

    private static Query query() {
        return client.collection("c");
    }

    private static QuerySplitState state() {
        return new QuerySplitState(new QuerySplit("0", query().toProto(), READ_TIME));
    }

    private static FetchedDocument document(String path) {
        return new FetchedDocument(
                TestDocuments.document(client, path, Map.of(), READ_TIME), query());
    }

    private FirestoreRecordEmitter<String> emitter(Deserializer deserializer) {
        return new FirestoreRecordEmitter<>(deserializer, metrics.metrics());
    }

    @Test
    void emitsTheRecordsADocumentProducedAndAdvancesTheSplit() throws Exception {
        CollectingSourceOutput<String> output = new CollectingSourceOutput<>();
        QuerySplitState state = state();

        emitter(
                        (document, out) -> {
                            out.collect(document.getId() + "#0");
                            out.collect(document.getId() + "#1");
                        })
                .emitRecord(document("c/a"), output, state);

        assertThat(output.records()).containsExactly("a#0", "a#1");
        assertThat(state.getEmitted()).isOne();
        assertThat(metrics.counter(FirestoreMetricNames.RECORDS_SKIPPED)).isZero();
    }

    @Test
    void aDocumentThatProducedNothingIsSkippedAndStillPassed() throws Exception {
        QuerySplitState state = state();

        emitter((document, out) -> {})
                .emitRecord(document("c/a"), new CollectingSourceOutput<>(), state);

        assertThat(metrics.counter(FirestoreMetricNames.RECORDS_SKIPPED)).isOne();
        assertThat(state.getEmitted())
                .as("the resume point moves past a skipped document too")
                .isOne();
        assertThat(state.toSplit().getQuery().getStructuredQuery().getStartAt().getBefore())
                .isFalse();
    }

    @Test
    void leavesTheSplitWhereItWasWhenTheDeserializerThrows() {
        QuerySplitState state = state();

        assertThatThrownBy(
                        () ->
                                emitter(
                                                (document, out) -> {
                                                    throw new IOException("bad document");
                                                })
                                        .emitRecord(
                                                document("c/a"),
                                                new CollectingSourceOutput<>(),
                                                state))
                .isInstanceOf(IOException.class);
        assertThat(state.getEmitted()).isZero();
    }

    @Test
    void refusesACollectorUsedOutsideTheCallItWasHandedTo() throws Exception {
        AtomicReference<Collector<String>> retained = new AtomicReference<>();
        FirestoreRecordEmitter<String> emitter =
                emitter(
                        (document, out) -> {
                            if (retained.get() == null) {
                                retained.set(out);
                            } else {
                                retained.get().collect("late");
                            }
                        });
        emitter.emitRecord(document("c/a"), new CollectingSourceOutput<>(), state());

        assertThatThrownBy(
                        () ->
                                emitter.emitRecord(
                                        document("c/b"), new CollectingSourceOutput<>(), state()))
                .isInstanceOf(IllegalStateException.class);
    }

    /** A deserializer written as a lambda. */
    @FunctionalInterface
    private interface Deserializer extends FirestoreDocumentDeserializationSchema<String> {

        @Override
        default TypeInformation<String> getProducedType() {
            return Types.STRING;
        }

        @Override
        void deserialize(DocumentSnapshot document, Collector<String> out) throws IOException;
    }
}
