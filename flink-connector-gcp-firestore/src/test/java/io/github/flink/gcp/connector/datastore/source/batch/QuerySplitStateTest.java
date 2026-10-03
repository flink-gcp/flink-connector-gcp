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

package io.github.flink.gcp.connector.datastore.source.batch;

import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.RunQueryRequest;
import com.google.protobuf.ByteString;
import com.google.protobuf.Int32Value;
import io.github.flink.gcp.connector.datastore.source.batch.enumerator.ScriptedQueryPlanner;
import org.junit.jupiter.api.Test;

import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;

/** The checkpointed form of a split being read: what is left of it. */
class QuerySplitStateTest {

    private static final Timestamp READ_TIME = Timestamp.ofTimeSecondsAndNanos(1_700_000_000L, 5);
    private static final ByteString END = ByteString.copyFromUtf8("end");

    private static QuerySplit split(UnaryOperator<Query.Builder> query) {
        RunQueryRequest request = ScriptedQueryPlanner.request("K");
        return new QuerySplit(
                "3",
                request.toBuilder().setQuery(query.apply(request.getQuery().toBuilder())).build(),
                READ_TIME);
    }

    private static FetchedEntity fetched(String name) {
        return new FetchedEntity(
                Entity.newBuilder(Key.newBuilder("p", "K", name).build()).build(),
                ByteString.copyFromUtf8("after-" + name));
    }

    @Test
    void aSplitWithNothingEmittedIsCheckpointedUnchanged() {
        QuerySplit split = split(q -> q.setLimit(Int32Value.of(5)));

        assertThat(new QuerySplitState(split).toSplit()).isSameAs(split);
    }

    @Test
    void aSplitIsCheckpointedAtTheCursorAfterItsLastEmittedEntity() {
        QuerySplit split = split(q -> q.setOffset(1).setLimit(Int32Value.of(5)).setEndCursor(END));
        QuerySplitState state = new QuerySplitState(split);

        state.recordEmitted(fetched("a"));
        state.recordEmitted(fetched("b"));
        QuerySplit checkpointed = state.toSplit();

        assertThat(checkpointed.splitId()).isEqualTo("3");
        assertThat(checkpointed.getReadTime()).isEqualTo(READ_TIME);
        assertThat(checkpointed.getRequest())
                .isEqualTo(
                        split.getRequest().toBuilder()
                                .setQuery(
                                        split.getRequest().getQuery().toBuilder()
                                                .setStartCursor(ByteString.copyFromUtf8("after-b"))
                                                .setOffset(0)
                                                .setLimit(Int32Value.of(3)))
                                .build());
        assertThat(checkpointed.getRequest().getQuery().getEndCursor())
                .as("the query's own end stays")
                .isEqualTo(END);
        assertThat(state.getEmitted()).isEqualTo(2);
    }

    @Test
    void aQueryWithoutALimitIsCheckpointedWithoutOne() {
        QuerySplitState state = new QuerySplitState(split(UnaryOperator.identity()));

        state.recordEmitted(fetched("a"));

        assertThat(state.toSplit().getRequest().getQuery().hasLimit()).isFalse();
    }

    @Test
    void aSplitWhoseLimitWasUsedUpIsCheckpointedExhausted() {
        QuerySplitState state = new QuerySplitState(split(q -> q.setLimit(Int32Value.of(1))));

        state.recordEmitted(fetched("a"));

        assertThat(state.toSplit().isExhausted()).isTrue();
    }
}
