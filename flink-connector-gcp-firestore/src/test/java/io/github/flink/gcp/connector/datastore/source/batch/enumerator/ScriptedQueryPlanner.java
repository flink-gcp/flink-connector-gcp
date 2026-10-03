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

package io.github.flink.gcp.connector.datastore.source.batch.enumerator;

import com.google.auth.Credentials;
import com.google.cloud.Timestamp;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.RunQueryRequest;
import io.github.flink.gcp.connector.datastore.source.DatastoreSourceConfig;
import io.github.flink.gcp.connector.datastore.source.batch.SplittableQueries;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A {@link QueryPlanner} that answers with a plan the test chose, and records what it was asked.
 */
public final class ScriptedQueryPlanner implements QueryPlanner {

    /** The read time every scripted plan carries. */
    public static final Timestamp READ_TIME = Timestamp.ofTimeSecondsAndNanos(1_700_000_000L, 0);

    @Nullable private final QueryPlan plan;
    @Nullable private final RuntimeException failure;
    private final List<Integer> parallelisms = new ArrayList<>();
    private int closeCalls;

    private ScriptedQueryPlanner(@Nullable QueryPlan plan, @Nullable RuntimeException failure) {
        this.plan = plan;
        this.failure = failure;
    }

    /** Returns a planner answering with one query per kind given, as a split query would be. */
    public static ScriptedQueryPlanner answering(String... kinds) {
        List<RunQueryRequest> requests = new ArrayList<>();
        for (String kind : kinds) {
            requests.add(request(kind));
        }
        return new ScriptedQueryPlanner(new QueryPlan(READ_TIME, requests, null), null);
    }

    /** Returns a planner answering with one query it says could not be split. */
    public static ScriptedQueryPlanner answeringUnsplittable(String reason) {
        return new ScriptedQueryPlanner(
                new QueryPlan(READ_TIME, List.of(request("Task")), reason), null);
    }

    /** Returns a planner whose every plan fails. */
    public static ScriptedQueryPlanner failingWith(RuntimeException failure) {
        return new ScriptedQueryPlanner(null, failure);
    }

    /** Returns a request reading one kind of the default database of project {@code p}. */
    public static RunQueryRequest request(String kind) {
        return RunQueryRequest.newBuilder()
                .setProjectId("p")
                .setPartitionId(PartitionId.newBuilder().setProjectId("p"))
                .setQuery(SplittableQueries.ofKind(kind))
                .build();
    }

    @Override
    public synchronized QueryPlan plan(DatastoreSourceConfig<?> config, int parallelism)
            throws IOException {
        parallelisms.add(parallelism);
        if (failure != null) {
            throw failure;
        }
        return plan;
    }

    @Override
    public void useCredentials(@Nullable Credentials credentials) {}

    @Override
    public synchronized void close() {
        closeCalls++;
    }

    /** Returns the parallelism each planning call was given. */
    public synchronized List<Integer> parallelisms() {
        return new ArrayList<>(parallelisms);
    }

    /** Returns how many times this planner was closed. */
    public synchronized int closeCalls() {
        return closeCalls;
    }

    /**
     * Mints scripted planners and keeps every one it minted. Static records, because the factory is
     * serialized into the source; one test uses a factory at a time.
     */
    public static final class Factory implements QueryPlannerFactory {

        private static final long serialVersionUID = 1L;

        private static final List<ScriptedQueryPlanner> MINTED = new CopyOnWriteArrayList<>();

        private final String[] kinds;

        private Factory(String[] kinds) {
            this.kinds = kinds;
        }

        /** Returns a factory whose planners answer with one query per kind given. */
        public static Factory answering(String... kinds) {
            MINTED.clear();
            return new Factory(kinds);
        }

        @Override
        public QueryPlanner create() {
            ScriptedQueryPlanner planner = ScriptedQueryPlanner.answering(kinds);
            MINTED.add(planner);
            return planner;
        }

        /** Returns every planner minted since the factory was created. */
        public List<ScriptedQueryPlanner> minted() {
            return new ArrayList<>(MINTED);
        }
    }
}
