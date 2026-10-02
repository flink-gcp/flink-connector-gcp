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
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;
import com.google.firestore.v1.RunQueryRequest;
import io.github.flink.gcp.connector.firestore.TestDocuments;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** The checkpointed form of a split being read: what is left of it. */
class QuerySplitStateTest {

    private static final Timestamp READ_TIME = Timestamp.ofTimeSecondsAndNanos(1_700_000_000L, 5);

    private static Firestore client;

    @BeforeAll
    static void openClient() {
        client = TestDocuments.offlineClient("p");
    }

    @AfterAll
    static void closeClient() throws Exception {
        client.close();
    }

    private static FetchedDocument fetched(Query query, String path) {
        return new FetchedDocument(
                TestDocuments.document(client, path, Map.of(), READ_TIME), query);
    }

    @Test
    void aSplitWithNothingEmittedIsCheckpointedUnchanged() {
        QuerySplit split =
                new QuerySplit("3", client.collection("c").limit(5).toProto(), READ_TIME);

        assertThat(new QuerySplitState(split).toSplit()).isSameAs(split);
    }

    @Test
    void aSplitIsCheckpointedAfterItsLastEmittedDocument() {
        Query query = client.collection("c").offset(1).limit(5);
        QuerySplit split = new QuerySplit("3", query.toProto(), READ_TIME);
        QuerySplitState state = new QuerySplitState(split);

        state.recordEmitted(fetched(query, "c/a"));
        state.recordEmitted(fetched(query, "c/b"));
        QuerySplit checkpointed = state.toSplit();

        assertThat(checkpointed.splitId()).isEqualTo("3");
        assertThat(checkpointed.getReadTime()).isEqualTo(READ_TIME);
        RunQueryRequest expected =
                query.startAfter(TestDocuments.document(client, "c/b", Map.of(), READ_TIME))
                        .offset(0)
                        .limit(3)
                        .toProto();
        assertThat(checkpointed.getQuery()).isEqualTo(expected);
        assertThat(state.getEmitted()).isEqualTo(2);
    }

    @Test
    void aSplitWhoseLimitWasUsedUpIsCheckpointedExhausted() {
        Query query = client.collection("c").limit(1);
        QuerySplitState state =
                new QuerySplitState(new QuerySplit("0", query.toProto(), READ_TIME));

        state.recordEmitted(fetched(query, "c/a"));

        assertThat(state.toSplit().isExhausted()).isTrue();
    }
}
