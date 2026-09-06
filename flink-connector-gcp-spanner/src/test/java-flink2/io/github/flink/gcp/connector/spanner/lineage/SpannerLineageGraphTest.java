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

package io.github.flink.gcp.connector.spanner.lineage;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageGraph;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;

import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Statement;
import io.github.flink.gcp.connector.base.lineage.PhysicalResourceFacet;
import io.github.flink.gcp.connector.spanner.source.SpannerLineageTest;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperation;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.github.flink.gcp.connector.spanner.source.SpannerLineageTest.roundTrip;
import static org.assertj.core.api.Assertions.assertThat;

/** Flink 2.x extraction from real Spanner runtime objects, without starting clients. */
class SpannerLineageGraphTest {
    @Test
    void dataStreamGraphExtractsEveryBuilderPathAndItsActualBound() throws Exception {
        for (Source<Long, ?, ?> source :
                List.of(
                        SpannerLineageTest.batch(
                                SpannerReadOperation.read("People", KeySet.all(), List.of("id"))),
                        SpannerLineageTest.batch(
                                SpannerReadOperation.readUsingIndex(
                                        "People", "by_id", KeySet.all(), List.of("id"))),
                        SpannerLineageTest.batch(
                                SpannerReadOperation.query(Statement.of("SELECT id FROM secret"))),
                        SpannerLineageTest.changes(false),
                        SpannerLineageTest.changes(true))) {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.fromSource(roundTrip(source), WatermarkStrategy.noWatermarks(), "input", Types.LONG)
                    .sinkTo(roundTrip(SpannerLineageTest.sink()));
            LineageGraph graph = env.getStreamGraph().getLineageGraph();
            assertThat(graph.sources())
                    .singleElement()
                    .satisfies(
                            vertex -> {
                                assertThat(vertex.boundedness()).isEqualTo(source.getBoundedness());
                                assertThat(vertex.datasets())
                                        .hasSameSizeAs(
                                                SpannerLineageTest.vertex(source).datasets());
                                if (!vertex.datasets().isEmpty()) {
                                    assertThat(vertex.datasets().get(0).namespace())
                                            .isEqualTo("spanner://p:i");
                                    assertThat(vertex.datasets().get(0).name())
                                            .isIn("db.People", "db/changeStreams/Changes");
                                    assertThat(facet(vertex.datasets().get(0)).resources())
                                            .isEqualTo(
                                                    facet(
                                                                    SpannerLineageTest.vertex(
                                                                                    source)
                                                                            .datasets()
                                                                            .get(0))
                                                            .resources());
                                }
                            });
            assertThat(graph.sinks())
                    .singleElement()
                    .satisfies(vertex -> assertThat(vertex.datasets()).isEmpty());
            assertThat(graph.relations()).hasSize(1);
        }
    }

    @Test
    void plannerRetainsQuotedPhysicalNamesAfterDeferredScanPushdown() {
        LineageGraph graph = plan(false, false);
        assertTableGraph(graph, false);
    }

    @Test
    void plannerRetainsTheCdcTableAndStreamProvenance() {
        assertTableGraph(plan(true, false), true);
    }

    @Test
    void lookupTableIsNotExtractedAsAnotherPhysicalSource() {
        assertTableGraph(plan(false, true), false);
    }

    private static LineageGraph plan(boolean cdc, boolean lookup) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment table = StreamTableEnvironment.create(env);
        table.registerCatalog("catalog", new GenericInMemoryCatalog("catalog", "logical"));
        table.useCatalog("catalog");
        String options =
                "'connector'='spanner', 'project'='p', 'instance'='i', 'database'='db', "
                        + "'dialect'='POSTGRESQL', 'schema'='\"Analytics\"', "
                        + "'service-account-key-file'='/lineage-test/must-not-read-credentials.json'";
        table.executeSql(
                "CREATE TABLE input (id BIGINT, name STRING, pt AS PROCTIME(), PRIMARY KEY(id) NOT ENFORCED) WITH ("
                        + options
                        + ", 'table'='\"People\"', "
                        + (cdc
                                ? "'scan.mode'='change-stream', 'scan.change-stream.name'='Changes', 'scan.change-stream.changelog-mode'='upsert'"
                                : "'scan.index'='by_id'")
                        + ")");
        table.executeSql(
                "CREATE TABLE output (id BIGINT, name STRING, PRIMARY KEY(id) NOT ENFORCED) WITH ("
                        + options
                        + ", 'table'='\"Output\"')");
        if (lookup) {
            table.executeSql(
                    "CREATE TABLE dimension (id BIGINT, name STRING, PRIMARY KEY(id) NOT ENFORCED) WITH ("
                            + options
                            + ", 'table'='\"Dimension\"')");
        }
        table.createStatementSet()
                .addInsertSql(
                        lookup
                                ? "INSERT INTO output SELECT i.id, d.name FROM input i LEFT JOIN dimension FOR SYSTEM_TIME AS OF i.pt AS d ON i.id = d.id"
                                : "INSERT INTO output SELECT id, name FROM input WHERE id > 7")
                .attachAsDataStream();
        return env.getStreamGraph().getLineageGraph();
    }

    private static void assertTableGraph(LineageGraph graph, boolean cdc) {
        assertThat(graph.sources())
                .singleElement()
                .satisfies(
                        source -> {
                            assertThat(source.boundedness())
                                    .isEqualTo(
                                            cdc
                                                    ? Boundedness.CONTINUOUS_UNBOUNDED
                                                    : Boundedness.BOUNDED);
                            assertThat(source.datasets())
                                    .singleElement()
                                    .satisfies(
                                            dataset -> {
                                                assertThat(dataset.name())
                                                        .isEqualTo("catalog.logical.input");
                                                assertThat(dataset.namespace())
                                                        .isEqualTo("spanner://p:i");
                                                assertThat(facet(dataset).resources())
                                                        .hasSize(cdc ? 2 : 1);
                                                assertTable(dataset, "People");
                                                if (cdc) {
                                                    assertThat(facet(dataset).resources())
                                                            .filteredOn(
                                                                    r ->
                                                                            r.kind()
                                                                                    .equals(
                                                                                            "spanner-change-stream"))
                                                            .singleElement()
                                                            .satisfies(
                                                                    resource -> {
                                                                        assertThat(
                                                                                        resource
                                                                                                .namespace())
                                                                                .isEqualTo(
                                                                                        "spanner://p:i");
                                                                        assertThat(resource.name())
                                                                                .isEqualTo(
                                                                                        "db/changeStreams/Changes");
                                                                        assertThat(
                                                                                        resource
                                                                                                .identity())
                                                                                .isEqualTo(
                                                                                        Map.of(
                                                                                                "project",
                                                                                                "p",
                                                                                                "instance",
                                                                                                "i",
                                                                                                "database",
                                                                                                "db",
                                                                                                "stream",
                                                                                                "Changes"));
                                                                    });
                                                }
                                            });
                        });
        assertThat(graph.sinks())
                .singleElement()
                .satisfies(
                        sink ->
                                assertThat(sink.datasets())
                                        .singleElement()
                                        .satisfies(
                                                dataset -> {
                                                    assertThat(dataset.name())
                                                            .isEqualTo("catalog.logical.output");
                                                    assertThat(dataset.namespace())
                                                            .isEqualTo("spanner://p:i");
                                                    assertThat(facet(dataset).resources())
                                                            .hasSize(1);
                                                    assertTable(dataset, "Output");
                                                }));
        assertThat(graph.relations()).hasSize(1);
    }

    private static void assertTable(LineageDataset dataset, String table) {
        assertThat(facet(dataset).resources())
                .filteredOn(r -> r.kind().equals("spanner-table"))
                .singleElement()
                .satisfies(
                        resource -> {
                            assertThat(resource.namespace()).isEqualTo("spanner://p:i");
                            assertThat(resource.name()).isEqualTo("db.Analytics." + table);
                            assertThat(resource.identity())
                                    .isEqualTo(
                                            Map.of(
                                                    "project",
                                                    "p",
                                                    "instance",
                                                    "i",
                                                    "database",
                                                    "db",
                                                    "schema",
                                                    "\"Analytics\"",
                                                    "table",
                                                    "\"" + table + "\""));
                        });
    }

    private static PhysicalResourceFacet facet(LineageDataset dataset) {
        return (PhysicalResourceFacet) dataset.facets().get("gcp");
    }
}
