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

package io.github.flink.gcp.connector.firestore.source;

import com.google.cloud.firestore.CollectionGroup;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.FieldPath;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryPartition;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.firestore.source.batch.enumerator.ClientQueryPlanner;
import io.github.flink.gcp.connector.firestore.source.batch.enumerator.QueryPlanner;
import io.github.flink.gcp.connector.firestore.source.batch.enumerator.QueryPlannerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Mints the production planner with one call replaced: the partitions are cut at document paths the
 * test chooses, because the emulator answers {@code PartitionQuery} with {@code UNIMPLEMENTED}.
 * Everything else — the probe, the snapshot time, the projection, the queries' wire form — is the
 * production path's, against the emulator.
 */
public final class FixedPartitionsPlannerFactory implements QueryPlannerFactory {

    private static final long serialVersionUID = 1L;

    private final String emulatorEndpoint;
    private final List<String> boundaries;

    /**
     * Creates the factory.
     *
     * @param emulatorEndpoint the emulator as {@code host:port}
     * @param boundaries the document paths, relative to the database's documents root and in
     *     document-name order, at which one partition ends and the next begins
     */
    public FixedPartitionsPlannerFactory(String emulatorEndpoint, List<String> boundaries) {
        this.emulatorEndpoint = emulatorEndpoint;
        this.boundaries = List.copyOf(boundaries);
    }

    @Override
    public QueryPlanner create() {
        return new ClientQueryPlanner(
                EmulatorEndpoint.parse(emulatorEndpoint, "emulatorEndpoint")) {
            @Override
            protected List<QueryPartition> partitions(CollectionGroup group, int partitionCount) {
                Query ordered = group.orderBy(FieldPath.documentId());
                List<QueryPartition> partitions = new ArrayList<>();
                Object[] start = null;
                for (String boundary : boundaries) {
                    DocumentReference at = group.getFirestore().document(boundary);
                    Object[] end = new Object[] {at};
                    partitions.add(new QueryPartition(ordered, start, end));
                    start = end;
                }
                partitions.add(new QueryPartition(ordered, start, null));
                return partitions;
            }
        };
    }
}
