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

import com.google.datastore.v1.Filter;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.client.DatastoreHelper;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.datastore.source.batch.enumerator.ClientQueryPlanner;
import io.github.flink.gcp.connector.datastore.source.batch.enumerator.QueryPlanner;
import io.github.flink.gcp.connector.datastore.source.batch.enumerator.QueryPlannerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Mints the production planner with one call replaced: the key ranges are cut at key names the test
 * chooses, in the shape the client library's {@code QuerySplitter} gives them — {@code __key__ >=}
 * the range's start and {@code <} its end, ANDed onto the query — because the emulator answers the
 * splitter's {@code __scatter__} sampling with no keys. Everything else — the probe, the snapshot
 * time, the split count, the queries' wire form — is the production path's, against the emulator.
 */
public final class FixedSplitsPlannerFactory implements QueryPlannerFactory {

    private static final long serialVersionUID = 1L;

    /** The split count each replaced call was asked for, in this JVM. */
    static final List<Integer> ASKED = new CopyOnWriteArrayList<>();

    private final String emulatorEndpoint;
    private final List<String> boundaries;

    /**
     * Creates the factory.
     *
     * @param emulatorEndpoint the emulator as {@code host:port}
     * @param boundaries the key names, of the query's kind and in key order, at which one range
     *     ends and the next begins
     */
    public FixedSplitsPlannerFactory(String emulatorEndpoint, List<String> boundaries) {
        this.emulatorEndpoint = emulatorEndpoint;
        this.boundaries = List.copyOf(boundaries);
    }

    @Override
    public QueryPlanner create() {
        return new ClientQueryPlanner(
                EmulatorEndpoint.parse(emulatorEndpoint, "emulatorEndpoint")) {
            @Override
            protected List<Query> split(Query query, PartitionId partition, int splitCount) {
                ASKED.add(splitCount);
                String kind = query.getKind(0).getName();
                List<Query> ranges = new ArrayList<>();
                Key start = null;
                for (String boundary : boundaries) {
                    Key end =
                            DatastoreHelper.makeKey(kind, boundary)
                                    .setPartitionId(partition)
                                    .build();
                    ranges.add(range(query, start, end));
                    start = end;
                }
                ranges.add(range(query, start, null));
                return ranges;
            }
        };
    }

    private static Query range(Query query, @Nullable Key start, @Nullable Key end) {
        List<Filter> filters = new ArrayList<>();
        if (query.hasFilter()) {
            filters.add(query.getFilter());
        }
        if (start != null) {
            filters.add(
                    DatastoreHelper.makeFilter(
                                    "__key__",
                                    PropertyFilter.Operator.GREATER_THAN_OR_EQUAL,
                                    DatastoreHelper.makeValue(start))
                            .build());
        }
        if (end != null) {
            filters.add(
                    DatastoreHelper.makeFilter(
                                    "__key__",
                                    PropertyFilter.Operator.LESS_THAN,
                                    DatastoreHelper.makeValue(end))
                            .build());
        }
        return query.toBuilder().setFilter(DatastoreHelper.makeAndFilter(filters)).build();
    }
}
