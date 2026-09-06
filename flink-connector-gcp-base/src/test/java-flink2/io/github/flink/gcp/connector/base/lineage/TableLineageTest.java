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

package io.github.flink.gcp.connector.base.lineage;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageGraph;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.functions.LookupFunction;

import io.github.flink.gcp.connector.base.lineage.internal.Lineage;
import io.github.flink.gcp.connector.base.lineage.internal.LineageIdentifiers;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class TableLineageTest {
    private static final List<ResourceIdentifier> INPUTS =
            List.of(
                    LineageIdentifiers.bigQueryTable("p", "d", "a"),
                    LineageIdentifiers.bigQueryTable("p", "d", "b"));
    private static final ResourceIdentifier OUTPUT =
            LineageIdentifiers.bigQueryTable("p", "d", "out");
    private static final AtomicInteger LOOKUP_EXTRACTIONS = new AtomicInteger();

    @Test
    void plannerReplacesLogicalNamesAndRetainsAllPhysicalResources() {
        LineageGraph graph = plan(true, INPUTS, false);
        assertThat(graph.sources())
                .singleElement()
                .satisfies(
                        source -> {
                            assertThat(source.boundedness()).isEqualTo(Boundedness.BOUNDED);
                            assertThat(source.datasets())
                                    .singleElement()
                                    .satisfies(
                                            dataset -> {
                                                assertThat(dataset.name())
                                                        .isEqualTo("catalog.db.input");
                                                assertThat(dataset.namespace())
                                                        .isEqualTo("bigquery");
                                                assertThat(facet(dataset).resources())
                                                        .containsExactlyElementsOf(INPUTS);
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
                                                            .isEqualTo("catalog.db.output");
                                                    assertThat(facet(dataset).resources())
                                                            .containsExactly(OUTPUT);
                                                }));
        assertThat(graph.relations()).hasSize(1);
    }

    @Test
    void unadaptedMultipleDatasetsLoseTheFacetInThePlanner() {
        LineageGraph graph = plan(false, INPUTS, false);
        assertThat(graph.sources())
                .singleElement()
                .satisfies(
                        source ->
                                assertThat(source.datasets())
                                        .singleElement()
                                        .satisfies(
                                                dataset -> {
                                                    assertThat(dataset.name())
                                                            .isEqualTo("catalog.db.input");
                                                    assertThat(dataset.namespace()).isEmpty();
                                                    assertThat(dataset.facets())
                                                            .doesNotContainKey("gcp");
                                                }));
    }

    @Test
    void knownLogicalTableWithUnknownResourcesHasNoPhysicalPlaceholder() {
        LineageGraph graph = plan(true, List.of(), false);
        assertThat(graph.sources())
                .singleElement()
                .satisfies(
                        source ->
                                assertThat(source.datasets())
                                        .singleElement()
                                        .satisfies(
                                                dataset -> {
                                                    assertThat(dataset.name())
                                                            .isEqualTo("catalog.db.input");
                                                    assertThat(facet(dataset).resources())
                                                            .isEmpty();
                                                }));
    }

    @Test
    void lookupFunctionProviderIsNotExtractedAsASourceVertex() {
        LOOKUP_EXTRACTIONS.set(0);
        LineageGraph graph = plan(true, INPUTS, true);
        assertThat(LOOKUP_EXTRACTIONS).hasValue(0);
        assertThat(graph.sources())
                .singleElement()
                .satisfies(
                        source ->
                                assertThat(source.datasets())
                                        .singleElement()
                                        .satisfies(
                                                dataset -> {
                                                    assertThat(dataset.name())
                                                            .isEqualTo("catalog.db.input");
                                                    assertThat(facet(dataset).resources())
                                                            .containsExactlyElementsOf(INPUTS);
                                                }));
        assertThat(graph.relations()).hasSize(1);
    }

    private static LineageGraph plan(
            boolean adapt, List<ResourceIdentifier> resources, boolean lookup) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment table = StreamTableEnvironment.create(env);
        Factory factory = new FakeFactory(adapt, resources);
        table.registerCatalog(
                "catalog",
                new GenericInMemoryCatalog("catalog", "db") {
                    @Override
                    public Optional<Factory> getFactory() {
                        return Optional.of(factory);
                    }
                });
        table.useCatalog("catalog");
        table.executeSql(
                "CREATE TABLE input (id INT, pt AS PROCTIME()) WITH ('connector' ="
                        + " 'lineage-test')");
        table.executeSql("CREATE TABLE output (id INT) WITH ('connector' = 'lineage-test')");
        if (lookup) {
            table.executeSql("CREATE TABLE dimension (id INT) WITH ('connector' = 'lineage-test')");
        }
        table.createStatementSet()
                .addInsertSql(
                        lookup
                                ? "INSERT INTO output SELECT d.id FROM input AS f LEFT JOIN"
                                      + " dimension FOR SYSTEM_TIME AS OF f.pt AS d ON f.id = d.id"
                                : "INSERT INTO output SELECT id FROM input")
                .attachAsDataStream();
        return env.getStreamGraph().getLineageGraph();
    }

    private static PhysicalResourceFacet facet(LineageDataset dataset) {
        return (PhysicalResourceFacet) dataset.facets().get("gcp");
    }

    private static final class FakeFactory
            implements DynamicTableSourceFactory, DynamicTableSinkFactory {
        private final boolean adapt;
        private final List<ResourceIdentifier> resources;

        private FakeFactory(boolean adapt, List<ResourceIdentifier> resources) {
            this.adapt = adapt;
            this.resources = resources;
        }

        @Override
        public DynamicTableSource createDynamicTableSource(DynamicTableFactory.Context context) {
            return new FakeTableSource(adapt, resources);
        }

        @Override
        public DynamicTableSink createDynamicTableSink(DynamicTableFactory.Context context) {
            return new FakeTableSink();
        }

        @Override
        public String factoryIdentifier() {
            return "lineage-test";
        }

        @Override
        public Set<ConfigOption<?>> requiredOptions() {
            return Set.of();
        }

        @Override
        public Set<ConfigOption<?>> optionalOptions() {
            return Set.of();
        }
    }

    private static final class FakeTableSource implements ScanTableSource, LookupTableSource {
        private final boolean adapt;
        private final List<ResourceIdentifier> resources;

        private FakeTableSource(boolean adapt, List<ResourceIdentifier> resources) {
            this.adapt = adapt;
            this.resources = resources;
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return ChangelogMode.insertOnly();
        }

        @Override
        public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
            return SourceProvider.of(
                    new LineageTestFixtures.FakeSource(resources, Boundedness.BOUNDED, adapt));
        }

        @Override
        public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
            return LookupFunctionProvider.of(new FakeLookup());
        }

        @Override
        public DynamicTableSource copy() {
            return new FakeTableSource(adapt, resources);
        }

        @Override
        public String asSummaryString() {
            return "lineage test source";
        }
    }

    private static final class FakeTableSink implements DynamicTableSink {
        @Override
        public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
            return ChangelogMode.insertOnly();
        }

        @Override
        public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
            return SinkV2Provider.of(new LineageTestFixtures.FakeSink(List.of(OUTPUT), true));
        }

        @Override
        public DynamicTableSink copy() {
            return new FakeTableSink();
        }

        @Override
        public String asSummaryString() {
            return "lineage test sink";
        }
    }

    public static final class FakeLookup extends LookupFunction implements LineageVertexProvider {
        private static final long serialVersionUID = 1L;

        @Override
        public Collection<RowData> lookup(RowData keyRow) {
            throw new AssertionError("planning must not execute a lookup");
        }

        @Override
        public LineageVertex getLineageVertex() {
            LOOKUP_EXTRACTIONS.incrementAndGet();
            return Lineage.source(
                    Boundedness.BOUNDED,
                    List.of(LineageIdentifiers.bigQueryTable("p", "d", "dimension")));
        }
    }
}
