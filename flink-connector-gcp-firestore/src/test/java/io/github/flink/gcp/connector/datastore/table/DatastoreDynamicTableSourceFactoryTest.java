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

package io.github.flink.gcp.connector.datastore.table;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;

import io.github.flink.gcp.connector.base.lineage.PhysicalResourceFacet;
import io.github.flink.gcp.connector.base.lineage.ResourceIdentifier;
import io.github.flink.gcp.connector.base.lineage.internal.TableLineageSource;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.source.DatastoreSourceConfig;
import io.github.flink.gcp.connector.datastore.source.batch.DatastoreBatchSource;
import io.github.flink.gcp.connector.datastore.source.batch.SplittableQueries;
import io.github.flink.gcp.connector.datastore.table.source.DatastoreDynamicSource;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Drives the factory's scan path without a planner. */
class DatastoreDynamicTableSourceFactoryTest {

    private static final ResolvedSchema KEYED =
            new ResolvedSchema(
                    List.of(
                            Column.physical("id", DataTypes.STRING().notNull()),
                            Column.physical("name", DataTypes.STRING())),
                    List.of(),
                    UniqueConstraint.primaryKey("pk", List.of("id")));

    private static Map<String, String> options(String... pairs) {
        Map<String, String> options = new HashMap<>();
        options.put("connector", DatastoreDynamicTableFactory.IDENTIFIER);
        options.put("project", "my-project");
        options.put("kind", "Order");
        for (int i = 0; i < pairs.length; i += 2) {
            options.put(pairs[i], pairs[i + 1]);
        }
        return options;
    }

    private static Source<?, ?, ?> runtimeSource(DynamicTableSource source) {
        return ((SourceProvider)
                        ((ScanTableSource) source)
                                .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE))
                .createSource();
    }

    private static DatastoreSourceConfig<?> config(Source<?, ?, ?> source) {
        Object delegate = ((TableLineageSource<?, ?, ?>) source).delegate();
        return ((DatastoreBatchSource<?>) delegate).getConfig();
    }

    @Test
    void theKindIsReadWithTheScanOptions() {
        DatastoreSourceConfig<?> config =
                config(
                        runtimeSource(
                                FactoryMocks.createTableSource(
                                        KEYED,
                                        options(
                                                "database", "db",
                                                "namespace", "tenant-a",
                                                "scan.partition.max-partitions", "16",
                                                "scan.read-time", "2026-10-04T00:00:00Z",
                                                "scan.max-rows-per-fetch", "50"))));

        assertThat(config.getDatabase()).isEqualTo(DatabaseDestination.of("my-project", "db"));
        assertThat(config.getQuery()).isEqualTo(SplittableQueries.ofKind("Order"));
        assertThat(config.getNamespace()).isEqualTo("tenant-a");
        assertThat(config.getSplitCount()).isEqualTo(16);
        assertThat(config.getReadTime()).isEqualTo(Instant.parse("2026-10-04T00:00:00Z"));
        assertThat(config.getPageSize()).isEqualTo(50);
    }

    @Test
    void withoutScanOptionsTheSourceKeepsItsDefaults() {
        DatastoreSourceConfig<?> config =
                config(runtimeSource(FactoryMocks.createTableSource(KEYED, options())));

        assertThat(config.getNamespace()).isEmpty();
        assertThat(config.getSplitCount()).isNull();
        assertThat(config.getReadTime()).isNull();
        assertThat(config.getServiceAccountKeyFile()).isNull();
    }

    @Test
    void theScanIsInsertOnlyAndReportsTheKindUnderTheTablesName() {
        DynamicTableSource dynamic =
                FactoryMocks.createTableSource(KEYED, options("namespace", "tenant-a"));
        assertThat(((ScanTableSource) dynamic).getChangelogMode())
                .isEqualTo(ChangelogMode.insertOnly());

        Source<?, ?, ?> source = runtimeSource(dynamic);
        SourceLineageVertex vertex =
                (SourceLineageVertex) ((LineageVertexProvider) source).getLineageVertex();
        assertThat(vertex.boundedness()).isEqualTo(Boundedness.BOUNDED);
        LineageDataset dataset = vertex.datasets().get(0);
        assertThat(dataset.name()).isEqualTo("default.default.t1");
        ResourceIdentifier resource =
                ((PhysicalResourceFacet) dataset.facets().get("gcp")).resources().get(0);
        assertThat(resource.kind()).isEqualTo("datastore-kind");
        assertThat(resource.namespace()).isEqualTo("datastore://my-project/(default)/tenant-a");
        assertThat(resource.name()).isEqualTo("Order");
        // The DataStream source names the same kind itself.
        assertThat(resource)
                .isEqualTo(
                        ((PhysicalResourceFacet)
                                        ((LineageVertexProvider)
                                                        ((TableLineageSource<?, ?, ?>) source)
                                                                .delegate())
                                                .getLineageVertex()
                                                .datasets()
                                                .get(0)
                                                .facets()
                                                .get("gcp"))
                                .resources()
                                .get(0));
    }

    @Test
    void theSourceListsTheSixMetadataKeysInOrder() {
        assertThat(
                        ((DatastoreDynamicSource) FactoryMocks.createTableSource(KEYED, options()))
                                .listReadableMetadata()
                                .keySet())
                .containsExactly(
                        "key-name", "key-id", "version", "create-time", "update-time", "read-time");
    }

    @Test
    void copyIsEqualToTheOriginalAndKeepsTheProjectionAndMetadata() {
        Map<String, String> options =
                options("type-mismatch-policy", "null", "scan.max-rows-per-fetch", "9");
        DatastoreDynamicSource dynamic =
                (DatastoreDynamicSource) FactoryMocks.createTableSource(KEYED, options);
        DatastoreDynamicSource unprojected =
                (DatastoreDynamicSource) FactoryMocks.createTableSource(KEYED, options);
        dynamic.applyProjection(
                new int[][] {{1}}, DataTypes.ROW(DataTypes.FIELD("name", DataTypes.STRING())));
        dynamic.applyReadableMetadata(
                List.of("version"),
                DataTypes.ROW(
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("version", DataTypes.BIGINT().notNull())));

        assertThat(dynamic.copy()).isEqualTo(dynamic).hasSameHashCodeAs(dynamic);
        // The same options: only the projection and the metadata differ.
        assertThat(dynamic.copy()).isNotEqualTo(unprojected);
        for (String[] differing :
                new String[][] {
                    {"type-mismatch-policy", "null"},
                    {"namespace", "other"},
                    {"kind", "Other"},
                    {"database", "other"},
                    {"scan.read-time", "2026-10-04T00:00:00Z"}
                }) {
            assertThat(FactoryMocks.createTableSource(KEYED, options(differing)))
                    .as(differing[0])
                    .isNotEqualTo(FactoryMocks.createTableSource(KEYED, options()));
        }
    }

    @Test
    void aReadTimeThatIsNotAnInstantIsRefusedUnderItsKey() {
        assertThatThrownBy(
                        () ->
                                FactoryMocks.createTableSource(
                                        KEYED, options("scan.read-time", "yesterday")))
                .hasStackTraceContaining("Option 'scan.read-time' is invalid: 'yesterday'");
    }

    @Test
    void aScanValueTheBuilderRefusesIsRefusedUnderItsKey() {
        assertThatThrownBy(
                        () ->
                                FactoryMocks.createTableSource(
                                        KEYED, options("scan.partition.max-partitions", "0")))
                .hasStackTraceContaining("Option 'scan.partition.max-partitions' is invalid")
                .hasStackTraceContaining("splitCount");
        assertThatThrownBy(
                        () ->
                                FactoryMocks.createTableSource(
                                        KEYED, options("scan.max-rows-per-fetch", "0")))
                .hasStackTraceContaining("Option 'scan.max-rows-per-fetch' is invalid")
                .hasStackTraceContaining("pageSize");
    }

    @Test
    void theSharedChecksRefuseOnTheScanPathToo() {
        assertThatThrownBy(() -> FactoryMocks.createTableSource(KEYED, options("kind", " ")))
                .hasStackTraceContaining("Option 'kind' is invalid:");
        assertThatThrownBy(
                        () ->
                                FactoryMocks.createTableSource(
                                        KEYED, options("kind", "__Stat_Kind__")))
                .hasStackTraceContaining("Option 'kind' is invalid:")
                .hasStackTraceContaining("neither writes nor reads one");
        assertThatThrownBy(() -> FactoryMocks.createTableSource(KEYED, options("namespace", "a/b")))
                .hasStackTraceContaining("Option 'namespace' is invalid:");
        assertThatThrownBy(
                        () ->
                                FactoryMocks.createTableSource(
                                        KEYED, options("namespace", "__reserved__")))
                .hasStackTraceContaining("Option 'namespace' is invalid:")
                .hasStackTraceContaining("which Datastore reserves");
        ResolvedSchema intColumn = ResolvedSchema.of(Column.physical("n", DataTypes.INT()));
        assertThatThrownBy(() -> FactoryMocks.createTableSource(intColumn, options()))
                .hasStackTraceContaining("write an INT as a BIGINT");
    }

    @Test
    void theKeyFileAndTheParallelismReachTheRuntimeSource() {
        assertThat(
                        config(
                                        runtimeSource(
                                                FactoryMocks.createTableSource(
                                                        KEYED,
                                                        options(
                                                                "service-account-key-file",
                                                                "/k.json"))))
                                .getServiceAccountKeyFile())
                .isEqualTo("/k.json");
        assertThat(
                        ((SourceProvider)
                                        ((ScanTableSource)
                                                        FactoryMocks.createTableSource(
                                                                KEYED,
                                                                options("scan.parallelism", "3")))
                                                .getScanRuntimeProvider(
                                                        ScanRuntimeProviderContext.INSTANCE))
                                .getParallelism())
                .contains(3);
    }
}
