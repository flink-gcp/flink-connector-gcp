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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;
import org.apache.flink.util.InstantiationUtil;

import io.github.flink.gcp.connector.base.lineage.PhysicalResourceFacet;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The source's lineage, read before and after a serialization round trip, from a source whose key
 * file does not exist: extraction must open nothing.
 */
class FirestoreSourceLineageTest {

    private static Source<String, ?, ?> source(FirestoreSourceBuilder<String> builder) {
        return builder.database(DatabaseDestination.of("p", "db"))
                .deserializer(new TestSources.DocumentPathDeserializer())
                .serviceAccountKeyFile("/lineage-test/must-not-read-credentials.json")
                .build();
    }

    @Test
    void aScanReportsItsCollectionGroup() throws Exception {
        Source<String, ?, ?> source =
                source(FirestoreSource.<String>builder().collectionGroup("orders"));

        for (Source<String, ?, ?> candidate : List.of(source, InstantiationUtil.clone(source))) {
            SourceLineageVertex vertex =
                    (SourceLineageVertex) ((LineageVertexProvider) candidate).getLineageVertex();
            assertThat(vertex.boundedness()).isEqualTo(Boundedness.BOUNDED);
            assertThat(vertex.datasets())
                    .singleElement()
                    .satisfies(
                            dataset -> {
                                assertThat(dataset.namespace()).isEqualTo("firestore://p/db");
                                assertThat(dataset.name()).isEqualTo("orders");
                                assertThat(
                                                ((PhysicalResourceFacet)
                                                                dataset.facets().get("gcp"))
                                                        .resources())
                                        .singleElement()
                                        .satisfies(
                                                resource ->
                                                        assertThat(resource.identity())
                                                                .isEqualTo(
                                                                        Map.of(
                                                                                "project",
                                                                                "p",
                                                                                "database",
                                                                                "db",
                                                                                "collectionGroup",
                                                                                "orders")));
                            });
        }
    }

    @Test
    void aQueryReportsNoDataset() throws Exception {
        Source<String, ?, ?> source =
                source(
                        FirestoreSource.<String>builder()
                                .query(firestore -> firestore.collection("c")));

        for (Source<String, ?, ?> candidate : List.of(source, InstantiationUtil.clone(source))) {
            assertThat(((LineageVertexProvider) candidate).getLineageVertex().datasets()).isEmpty();
        }
    }
}
