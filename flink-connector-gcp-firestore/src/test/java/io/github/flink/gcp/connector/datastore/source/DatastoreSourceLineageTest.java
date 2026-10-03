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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;
import org.apache.flink.util.InstantiationUtil;

import com.google.datastore.v1.KindExpression;
import com.google.datastore.v1.Query;
import com.google.protobuf.Int32Value;
import io.github.flink.gcp.connector.base.lineage.PhysicalResourceFacet;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.source.batch.SplittableQueries;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The source's lineage, read before and after a serialization round trip, from a source whose key
 * file does not exist: extraction must open nothing.
 */
class DatastoreSourceLineageTest {

    private static Source<String, ?, ?> source(
            DatabaseDestination database, DatastoreSourceBuilder<String> builder) {
        return builder.database(database)
                .deserializer(new TestSources.KeyNameDeserializer())
                .serviceAccountKeyFile("/lineage-test/must-not-read-credentials.json")
                .build();
    }

    private static List<SourceLineageVertex> vertices(Source<String, ?, ?> source)
            throws Exception {
        return List.of(
                (SourceLineageVertex) ((LineageVertexProvider) source).getLineageVertex(),
                (SourceLineageVertex)
                        ((LineageVertexProvider) InstantiationUtil.clone(source))
                                .getLineageVertex());
    }

    @Test
    void aKindReportsItself() throws Exception {
        Source<String, ?, ?> source =
                source(
                        DatabaseDestination.of("p", "db"),
                        DatastoreSource.<String>builder().kind("Task"));

        for (SourceLineageVertex vertex : vertices(source)) {
            assertThat(vertex.boundedness()).isEqualTo(Boundedness.BOUNDED);
            assertThat(vertex.datasets())
                    .singleElement()
                    .satisfies(
                            dataset -> {
                                assertThat(dataset.namespace()).isEqualTo("datastore://p/db");
                                assertThat(dataset.name()).isEqualTo("Task");
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
                                                                                "kind",
                                                                                "Task")));
                            });
        }
    }

    @Test
    void aQueryOfOneKindInANamespaceOfTheDefaultDatabaseReportsTheKind() throws Exception {
        Source<String, ?, ?> source =
                source(
                        DatabaseDestination.of("p"),
                        DatastoreSource.<String>builder()
                                .query(
                                        SplittableQueries.ofKind("Task").toBuilder()
                                                .setLimit(Int32Value.of(5))
                                                .build())
                                .namespace("tenant"));

        for (SourceLineageVertex vertex : vertices(source)) {
            assertThat(vertex.datasets())
                    .singleElement()
                    .satisfies(
                            dataset -> {
                                assertThat(dataset.namespace())
                                        .isEqualTo("datastore://p/(default)/tenant");
                                assertThat(dataset.name()).isEqualTo("Task");
                            });
        }
    }

    @Test
    void aGqlQueryAndAQueryOfNoSingleKindReportNoDataset() throws Exception {
        Query twoKinds =
                SplittableQueries.ofKind("A").toBuilder()
                        .addKind(KindExpression.newBuilder().setName("B"))
                        .build();
        for (DatastoreSourceBuilder<String> builder :
                List.of(
                        DatastoreSource.<String>builder().gqlQuery("SELECT * FROM Task"),
                        DatastoreSource.<String>builder().query(Query.getDefaultInstance()),
                        DatastoreSource.<String>builder().query(twoKinds))) {
            for (SourceLineageVertex vertex :
                    vertices(source(DatabaseDestination.of("p"), builder))) {
                assertThat(vertex.datasets()).isEmpty();
            }
        }
    }
}
