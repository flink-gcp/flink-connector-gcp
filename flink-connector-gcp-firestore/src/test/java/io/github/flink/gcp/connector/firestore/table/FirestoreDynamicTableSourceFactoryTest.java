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

package io.github.flink.gcp.connector.firestore.table;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
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

import com.google.cloud.firestore.FieldPath;
import com.google.cloud.firestore.Firestore;
import io.github.flink.gcp.connector.base.lineage.PhysicalResourceFacet;
import io.github.flink.gcp.connector.base.lineage.ResourceIdentifier;
import io.github.flink.gcp.connector.base.lineage.internal.TableLineageSource;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.TestDocuments;
import io.github.flink.gcp.connector.firestore.source.FirestoreSourceConfig;
import io.github.flink.gcp.connector.firestore.source.batch.FirestoreBatchSource;
import io.github.flink.gcp.connector.firestore.table.source.FirestoreDynamicSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Drives the factory's scan path without a planner. Refusals are asserted on phrases only the
 * connector's sentence carries: {@code FactoryUtil} echoes every option into its own message.
 */
class FirestoreDynamicTableSourceFactoryTest {

    private static final ResolvedSchema KEYED =
            new ResolvedSchema(
                    List.of(
                            Column.physical("id", DataTypes.STRING().notNull()),
                            Column.physical("name", DataTypes.STRING()),
                            Column.physical("a.b", DataTypes.STRING())),
                    List.of(),
                    UniqueConstraint.primaryKey("pk", List.of("id")));

    private static final ResolvedSchema UNKEYED =
            ResolvedSchema.of(
                    Column.physical("name", DataTypes.STRING()),
                    Column.physical("a.b", DataTypes.STRING()));

    private static Map<String, String> options(String... pairs) {
        Map<String, String> options = new HashMap<>();
        options.put("connector", FirestoreDynamicTableFactory.IDENTIFIER);
        options.put("project", "my-project");
        options.put("collection", "users/alice/orders");
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

    @SuppressWarnings("unchecked")
    private static FirestoreSourceConfig<?> config(Source<?, ?, ?> source) {
        Object delegate = ((TableLineageSource<?, ?, ?>) source).delegate();
        return ((FirestoreBatchSource<?>) delegate).getConfig();
    }

    @Test
    void aCollectionIsReadAsOneQueryWithTheScanConfig() throws Exception {
        DynamicTableSource dynamic =
                FactoryMocks.createTableSource(
                        KEYED,
                        options(
                                "database", "db",
                                "scan.read-time", "2026-10-04T00:00:00Z",
                                "scan.max-rows-per-fetch", "50"));
        FirestoreSourceConfig<?> config = config(runtimeSource(dynamic));

        assertThat(((ScanTableSource) dynamic).getChangelogMode())
                .isEqualTo(ChangelogMode.insertOnly());
        assertThat(config.getDatabase()).isEqualTo(DatabaseDestination.of("my-project", "db"));
        assertThat(config.getCollectionGroup()).isNull();
        assertThat(config.getReadTime()).isEqualTo(Instant.parse("2026-10-04T00:00:00Z"));
        assertThat(config.getPageSize()).isEqualTo(50);
        try (Firestore client = TestDocuments.offlineClient("my-project")) {
            // Only the declared fields are read, each a literal name; the key is the id.
            assertThat(config.getQueryFactory().create(client))
                    .isEqualTo(
                            client.collection("users/alice/orders")
                                    .select(FieldPath.of("name"), FieldPath.of("a.b")));
        }
    }

    @Test
    void aProjectionSelectsLiteralFieldNamesAndTheKeyIsNeverAField() throws Exception {
        FirestoreDynamicSource dynamic =
                (FirestoreDynamicSource) FactoryMocks.createTableSource(KEYED, options());
        dynamic.applyProjection(
                new int[][] {{0}, {2}},
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("a.b", DataTypes.STRING())));

        try (Firestore client = TestDocuments.offlineClient("my-project")) {
            assertThat(config(runtimeSource(dynamic)).getQueryFactory().create(client))
                    .isEqualTo(client.collection("users/alice/orders").select(FieldPath.of("a.b")));
        }
    }

    @Test
    void aCollectionGroupScanPartitionsAndSelectsLiteralNames() {
        FirestoreDynamicSource dynamic =
                (FirestoreDynamicSource)
                        FactoryMocks.createTableSource(
                                UNKEYED,
                                options(
                                        "scan.collection-group", "true",
                                        "scan.partition.max-partitions", "8"));
        FirestoreSourceConfig<?> all = config(runtimeSource(dynamic));

        assertThat(all.getCollectionGroup()).isEqualTo("orders");
        assertThat(all.getPartitionCount()).isEqualTo(8);
        // The encoded field paths: a dotted name is one field, in backticks.
        assertThat(all.getFieldMask()).containsExactly("name", "`a.b`");

        dynamic.applyProjection(
                new int[][] {{1}}, DataTypes.ROW(DataTypes.FIELD("a.b", DataTypes.STRING())));
        assertThat(config(runtimeSource(dynamic)).getFieldMask()).containsExactly("`a.b`");

        dynamic.applyProjection(new int[0][], DataTypes.ROW());
        assertThat(config(runtimeSource(dynamic)).getFieldMask()).containsExactly("__name__");
    }

    @Test
    void aCollectionGroupTableCannotDeclareAPrimaryKey() {
        assertThatThrownBy(
                        () ->
                                FactoryMocks.createTableSource(
                                        KEYED, options("scan.collection-group", "true")))
                .hasStackTraceContaining("whose ids repeat across them");
    }

    @Test
    void theScanReportsTheCollectionOrTheGroupUnderTheTablesName() {
        ResourceIdentifier collection = resource(KEYED, options("database", "db"));
        assertThat(collection.kind()).isEqualTo("firestore-collection");
        assertThat(collection.namespace()).isEqualTo("firestore://my-project/db");
        assertThat(collection.name()).isEqualTo("users/alice/orders");
        assertThat(collection.identity())
                .isEqualTo(
                        Map.of(
                                "project", "my-project",
                                "database", "db",
                                "collection", "users/alice/orders"));

        Source<?, ?, ?> group =
                runtimeSource(
                        FactoryMocks.createTableSource(
                                UNKEYED,
                                options("database", "db", "scan.collection-group", "true")));
        ResourceIdentifier groupResource = resource(group);
        assertThat(groupResource.kind()).isEqualTo("firestore-collection-group");
        assertThat(groupResource.namespace()).isEqualTo("firestore://my-project/db");
        assertThat(groupResource.name()).isEqualTo("orders");
        // The DataStream source names the same group itself.
        assertThat(groupResource)
                .isEqualTo(
                        ((PhysicalResourceFacet)
                                        ((LineageVertexProvider)
                                                        ((TableLineageSource<?, ?, ?>) group)
                                                                .delegate())
                                                .getLineageVertex()
                                                .datasets()
                                                .get(0)
                                                .facets()
                                                .get("gcp"))
                                .resources()
                                .get(0));

        SourceLineageVertex vertex =
                (SourceLineageVertex)
                        ((LineageVertexProvider)
                                        runtimeSource(
                                                FactoryMocks.createTableSource(KEYED, options())))
                                .getLineageVertex();
        assertThat(vertex.boundedness()).isEqualTo(Boundedness.BOUNDED);
        assertThat(vertex.datasets())
                .singleElement()
                .satisfies(d -> assertThat(d.name()).isEqualTo("default.default.t1"));
    }

    private static ResourceIdentifier resource(ResolvedSchema schema, Map<String, String> options) {
        return resource(runtimeSource(FactoryMocks.createTableSource(schema, options)));
    }

    private static ResourceIdentifier resource(Source<?, ?, ?> source) {
        return ((PhysicalResourceFacet)
                        ((LineageVertexProvider) source)
                                .getLineageVertex()
                                .datasets()
                                .get(0)
                                .facets()
                                .get("gcp"))
                .resources()
                .get(0);
    }

    @Test
    void theKeyFileAndTheParallelismReachTheRuntimeSource() {
        DynamicTableSource dynamic =
                FactoryMocks.createTableSource(
                        KEYED,
                        options(
                                "service-account-key-file", "/secrets/key.json",
                                "scan.parallelism", "3"));
        SourceProvider provider =
                (SourceProvider)
                        ((ScanTableSource) dynamic)
                                .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

        assertThat(provider.getParallelism()).contains(3);
        assertThat(config(provider.createSource()).getServiceAccountKeyFile())
                .isEqualTo("/secrets/key.json");
    }

    @Test
    void theSharedChecksRefuseOnTheScanPathToo() {
        assertThatThrownBy(
                        () ->
                                FactoryMocks.createTableSource(
                                        KEYED,
                                        options(
                                                "emulator-endpoint", "localhost:8085",
                                                "service-account-key-file", "/k.json")))
                .hasStackTraceContaining("an emulator connects without credentials");
        assertThatThrownBy(
                        () ->
                                FactoryMocks.createTableSource(
                                        KEYED, options("emulator-endpoint", "no-port")))
                .hasStackTraceContaining("emulator-endpoint must be host:port");
    }

    @Test
    void theSourceReadsTheFourMetadataKeysInOrder() {
        FirestoreDynamicSource dynamic =
                (FirestoreDynamicSource) FactoryMocks.createTableSource(KEYED, options());

        assertThat(dynamic.listReadableMetadata().keySet())
                .containsExactly("document-path", "create-time", "update-time", "read-time");
    }

    @Test
    void copyIsEqualToTheOriginalAndKeepsTheProjectionAndMetadata() {
        FirestoreDynamicSource dynamic =
                (FirestoreDynamicSource)
                        FactoryMocks.createTableSource(
                                KEYED,
                                options(
                                        "type-mismatch-policy",
                                        "null",
                                        "scan.max-rows-per-fetch",
                                        "9"));
        dynamic.applyProjection(
                new int[][] {{1}}, DataTypes.ROW(DataTypes.FIELD("name", DataTypes.STRING())));
        dynamic.applyReadableMetadata(
                List.of("read-time"),
                DataTypes.ROW(
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("read-time", DataTypes.TIMESTAMP_LTZ(6).notNull())));

        assertThat(dynamic.copy()).isEqualTo(dynamic);
        assertThat(dynamic.copy()).isNotEqualTo(FactoryMocks.createTableSource(KEYED, options()));
        // One field apart: the policy alone.
        assertThat(FactoryMocks.createTableSource(KEYED, options("type-mismatch-policy", "null")))
                .isNotEqualTo(FactoryMocks.createTableSource(KEYED, options()));
        // And the scan configuration alone.
        assertThat(FactoryMocks.createTableSource(KEYED, options("scan.max-rows-per-fetch", "9")))
                .isNotEqualTo(FactoryMocks.createTableSource(KEYED, options()));
    }

    @Test
    void partitionsWithoutACollectionGroupAreRefused() {
        assertThatThrownBy(
                        () ->
                                FactoryMocks.createTableSource(
                                        KEYED, options("scan.partition.max-partitions", "4")))
                .hasStackTraceContaining("partitions a collection-group scan");
    }

    @Test
    void aReadTimeFinerThanAMicrosecondReachesTheSourceTruncated() {
        FirestoreSourceConfig<?> config =
                config(
                        runtimeSource(
                                FactoryMocks.createTableSource(
                                        KEYED,
                                        options(
                                                "scan.read-time",
                                                "2026-10-10T00:00:00.123456789Z"))));

        assertThat(config.getReadTime()).isEqualTo(Instant.parse("2026-10-10T00:00:00.123456Z"));
    }

    @Test
    void aReadTimeThatIsNotAnInstantIsRefusedUnderItsKey() {
        assertThatThrownBy(
                        () ->
                                FactoryMocks.createTableSource(
                                        KEYED, options("scan.read-time", "yesterday")))
                .hasStackTraceContaining(
                        "Option 'scan.read-time' is invalid: 'yesterday' is not an ISO-8601"
                                + " instant");
    }

    @ParameterizedTest
    @ValueSource(strings = {"scan.max-rows-per-fetch", "scan.partition.max-partitions"})
    void aValueTheBuilderRefusesIsRenamedToItsKey(String key) {
        Map<String, String> options = options(key, "0");
        options.put("scan.collection-group", "true");

        assertThatThrownBy(() -> FactoryMocks.createTableSource(UNKEYED, options))
                .hasStackTraceContaining("Option '" + key + "' is invalid:");
    }
}
