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

package io.github.flink.gcp.connector.datastore.source;

import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.Query;
import io.github.flink.gcp.connector.datastore.source.batch.enumerator.ClientQueryPlanner;
import io.github.flink.gcp.connector.datastore.source.batch.enumerator.QueryPlanner;
import io.github.flink.gcp.connector.datastore.source.batch.enumerator.QueryPlannerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * The production planner over application-default credentials, recording how many ranges the
 * splitter answered each split call with, so a gated test can tell a read cut into key ranges from
 * one read whole.
 */
public final class RecordingPlannerFactory implements QueryPlannerFactory {

    private static final long serialVersionUID = 1L;

    /** The number of ranges each split call answered, in this JVM. */
    static final List<Integer> SPLITS = new CopyOnWriteArrayList<>();

    @Override
    public QueryPlanner create() {
        return new ClientQueryPlanner(null) {
            @Override
            protected List<Query> split(Query query, PartitionId partition, int splitCount)
                    throws IOException {
                List<Query> ranges = super.split(query, partition, splitCount);
                SPLITS.add(ranges.size());
                return ranges;
            }
        };
    }
}
