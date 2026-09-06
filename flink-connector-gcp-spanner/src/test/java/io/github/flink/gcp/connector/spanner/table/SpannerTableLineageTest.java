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

package io.github.flink.gcp.connector.spanner.table;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;

import io.github.flink.gcp.connector.base.lineage.PhysicalResourceFacet;
import io.github.flink.gcp.connector.spanner.source.batch.SpannerBatchReadSource;
import io.github.flink.gcp.connector.spanner.table.source.SpannerDynamicSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.github.flink.gcp.connector.spanner.source.SpannerLineageTest.roundTrip;
import static io.github.flink.gcp.connector.spanner.source.SpannerLineageTest.vertex;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpannerTableLineageTest {
    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("id", DataTypes.BIGINT().notNull()),
                    Column.physical("name", DataTypes.STRING()));

    @ParameterizedTest
    @CsvSource(
            delimiter = '|',
            value = {
                "GOOGLE_STANDARD_SQL||People|db.People",
                "POSTGRESQL||People|db.public.People",
                "GOOGLE_STANDARD_SQL||Analytics.People|db.Analytics.People",
                "POSTGRESQL||Analytics.People|db.Analytics.People",
                "GOOGLE_STANDARD_SQL|Analytics|People|db.Analytics.People",
                "POSTGRESQL|Analytics|People|db.analytics.people",
                "POSTGRESQL|\"Analytics\"|\"People\"|db.Analytics.People",
                "POSTGRESQL|\"A.B\"|\"C.D\"|db.A.B.C.D",
                "GOOGLE_STANDARD_SQL|`Analytics`|`People`|db.Analytics.People"
            })
    void factoryRuntimeObjectsPreserveExactPhysicalIdentities(
            String dialect, String schema, String table, String name) throws Exception {
        Map<String, String> options = options();
        options.put("dialect", dialect);
        options.put("table", table);
        if (schema != null) {
            options.put("schema", schema);
        }
        Map<String, String> identity =
                new HashMap<>(
                        Map.of("project", "p", "instance", "i", "database", "db", "table", table));
        if (schema != null) {
            identity.put("schema", schema);
        }
        DynamicTableSink sink = FactoryMocks.createTableSink(SCHEMA, options);
        ScanTableSource scan = (ScanTableSource) FactoryMocks.createTableSource(SCHEMA, options);
        for (Object runtime :
                List.of(
                        sink(sink),
                        sink(sink.copy()),
                        source(scan),
                        source((ScanTableSource) scan.copy()))) {
            check(runtime, name, identity, false);
            check(roundTrip((java.io.Serializable) runtime), name, identity, false);
        }
        options.put("scan.mode", "change-stream");
        options.put("scan.change-stream.name", "Changes");
        options.put("scan.change-stream.changelog-mode", "full");
        ScanTableSource cdc = (ScanTableSource) FactoryMocks.createTableSource(SCHEMA, options);
        for (Source<RowData, ?, ?> runtime :
                List.of(source(cdc), source((ScanTableSource) cdc.copy()))) {
            check(runtime, name, identity, true);
            check(roundTrip(runtime), name, identity, true);
        }
    }

    @Test
    void deferredIndexAndFilterReadsKeepTheKnownTableThroughProjectionAndCopy() throws Exception {
        Map<String, String> options = options();
        options.put("schema", "analytics");
        options.put("scan.index", "by_id");
        SpannerDynamicSource source =
                (SpannerDynamicSource) FactoryMocks.createTableSource(SCHEMA, options);
        source.applyFilters(
                List.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.EQUALS,
                                List.of(
                                        new FieldReferenceExpression(
                                                "id", DataTypes.BIGINT().notNull(), 0, 0),
                                        new ValueLiteralExpression(7L)),
                                DataTypes.BOOLEAN())));
        source.applyProjection(new int[0][], DataTypes.ROW());
        Source<RowData, ?, ?> runtime = source((ScanTableSource) source.copy());
        SpannerBatchReadSource<?> delegate =
                (SpannerBatchReadSource<?>)
                        ((SpannerTableLineage.TableSource<?, ?, ?>) runtime).delegate;
        assertThat(delegate.getConfig().getReadOperation().getTable()).isNull();
        assertThat(delegate.getConfig().getReadOperation().toString()).contains("deferred read");
        Map<String, String> identity =
                Map.of(
                        "project",
                        "p",
                        "instance",
                        "i",
                        "database",
                        "db",
                        "schema",
                        "analytics",
                        "table",
                        "People");
        check(runtime, "db.analytics.People", identity, false);
        check(roundTrip(runtime), "db.analytics.People", identity, false);
    }

    @Test
    void originalQuotingParticipatesInPlanIdentityEvenWhenCanonicalNamesAgree() {
        Map<String, String> plain = options();
        plain.put("dialect", "POSTGRESQL");
        plain.put("schema", "analytics");
        plain.put("table", "people");
        Map<String, String> quoted = new HashMap<>(plain);
        quoted.put("table", "\"people\"");
        assertThat(FactoryMocks.createTableSource(SCHEMA, plain))
                .isNotEqualTo(FactoryMocks.createTableSource(SCHEMA, quoted));
        assertThat(FactoryMocks.createTableSink(SCHEMA, plain))
                .isNotEqualTo(FactoryMocks.createTableSink(SCHEMA, quoted));
    }

    @Test
    void extractedValuesCannotMutateLaterInspections() {
        Source<RowData, ?, ?> runtime =
                source((ScanTableSource) FactoryMocks.createTableSource(SCHEMA, options()));
        LineageVertex first = vertex(runtime);
        PhysicalResourceFacet facet =
                (PhysicalResourceFacet) first.datasets().get(0).facets().get("gcp");
        assertThatThrownBy(() -> facet.resources().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> facet.resources().get(0).identity().put("table", "other"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(
                        ((PhysicalResourceFacet)
                                        vertex(runtime).datasets().get(0).facets().get("gcp"))
                                .resources())
                .isEqualTo(facet.resources());
    }

    private static Map<String, String> options() {
        return new HashMap<>(
                Map.of(
                        "connector",
                        "spanner",
                        "project",
                        "p",
                        "instance",
                        "i",
                        "database",
                        "db",
                        "table",
                        "People",
                        "service-account-key-file",
                        "/lineage-test/must-not-read-credentials.json"));
    }

    private static Source<RowData, ?, ?> source(ScanTableSource source) {
        return ((SourceProvider) source.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE))
                .createSource();
    }

    private static Sink<RowData> sink(DynamicTableSink sink) {
        return (Sink<RowData>)
                ((SinkV2Provider)
                                sink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false)))
                        .createSink();
    }

    private static void check(
            Object runtime, String name, Map<String, String> identity, boolean cdc) {
        LineageVertex vertex = vertex(runtime);
        if (runtime instanceof Source) {
            assertThat(((SourceLineageVertex) vertex).boundedness())
                    .isEqualTo(((Source<?, ?, ?>) runtime).getBoundedness())
                    .isEqualTo(cdc ? Boundedness.CONTINUOUS_UNBOUNDED : Boundedness.BOUNDED);
        } else {
            assertThat(vertex).isNotInstanceOf(SourceLineageVertex.class);
        }
        assertThat(vertex.datasets())
                .singleElement()
                .satisfies(
                        dataset -> {
                            assertThat(dataset.name()).isEqualTo("default.default.t1");
                            assertThat(dataset.namespace()).isEqualTo("spanner://p:i");
                            assertThat(dataset.facets()).containsOnlyKeys("gcp");
                            PhysicalResourceFacet facet =
                                    (PhysicalResourceFacet) dataset.facets().get("gcp");
                            assertThat(facet.resources()).hasSize(cdc ? 2 : 1);
                            assertThat(facet.resources())
                                    .filteredOn(r -> r.kind().equals("spanner-table"))
                                    .singleElement()
                                    .satisfies(
                                            resource -> {
                                                assertThat(resource.namespace())
                                                        .isEqualTo("spanner://p:i");
                                                assertThat(resource.name()).isEqualTo(name);
                                                assertThat(resource.identity()).isEqualTo(identity);
                                            });
                            if (cdc) {
                                assertThat(facet.resources())
                                        .filteredOn(r -> r.kind().equals("spanner-change-stream"))
                                        .singleElement()
                                        .satisfies(
                                                resource -> {
                                                    assertThat(resource.namespace())
                                                            .isEqualTo("spanner://p:i");
                                                    assertThat(resource.name())
                                                            .isEqualTo("db/changeStreams/Changes");
                                                    assertThat(resource.identity())
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
    }
}
