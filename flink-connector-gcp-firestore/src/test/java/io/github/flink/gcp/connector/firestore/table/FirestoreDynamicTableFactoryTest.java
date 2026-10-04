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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.types.RowKind;

import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.lineage.PhysicalResourceFacet;
import io.github.flink.gcp.connector.base.lineage.internal.TableLineageSink;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.sink.FirestoreBulkWriterSink;
import io.github.flink.gcp.connector.firestore.sink.FirestoreSinkConfig;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWriterOptions;
import io.github.flink.gcp.connector.firestore.sink.PreconditionFailurePolicy;
import io.github.flink.gcp.connector.firestore.table.sink.RowDataSerializationSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Drives the factory without a planner. A refusal is asserted with {@code hasStackTraceContaining}
 * on a phrase only the connector's sentence carries: {@code FactoryUtil} wraps it in a generic
 * message that echoes every {@code WITH} option.
 */
class FirestoreDynamicTableFactoryTest {

    private static final ResolvedSchema KEYED =
            new ResolvedSchema(
                    List.of(
                            Column.physical("id", DataTypes.STRING().notNull()),
                            Column.physical("name", DataTypes.STRING())),
                    List.of(),
                    UniqueConstraint.primaryKey("pk", List.of("id")));

    private static final ResolvedSchema UNKEYED =
            ResolvedSchema.of(
                    Column.physical("id", DataTypes.STRING()),
                    Column.physical("name", DataTypes.STRING()));

    private static Map<String, String> options() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", FirestoreDynamicTableFactory.IDENTIFIER);
        options.put("project", "my-project");
        options.put("collection", "users/alice/orders");
        return options;
    }

    private static Map<String, String> options(String key, String value) {
        Map<String, String> options = options();
        options.put(key, value);
        return options;
    }

    private static Sink<?> runtimeSink(ResolvedSchema schema, Map<String, String> options) {
        return ((SinkV2Provider)
                        FactoryMocks.createTableSink(schema, options)
                                .getSinkRuntimeProvider(new SinkRuntimeProviderContext(false)))
                .createSink();
    }

    private static FirestoreSinkConfig<?> config(
            ResolvedSchema schema, Map<String, String> options) {
        Sink<?> sink = runtimeSink(schema, options);
        return ((FirestoreBulkWriterSink<?>) ((TableLineageSink<?>) sink).delegate()).getConfig();
    }

    @Test
    void buildsTheConnectorsOwnSinkWithItsDefaultsForWhatADdlCannotSay() {
        FirestoreSinkConfig<?> config = config(KEYED, options());

        assertThat(config.getDatabase()).isEqualTo(DatabaseDestination.of("my-project"));
        assertThat(config.getWriterOptions()).isEqualTo(FirestoreWriterOptions.builder().build());
        assertThat(config.getFailedWriteHandler()).isEqualTo(FailureHandler.failJob());
        assertThat(config.getPreconditionFailurePolicy())
                .isEqualTo(PreconditionFailurePolicy.FAIL_JOB);
        assertThat(config.getSerializer())
                .isInstanceOf(RowDataSerializationSchema.class)
                .isEqualTo(
                        new RowDataSerializationSchema(
                                FirestoreTableSchema.of(
                                        (org.apache.flink.table.types.logical.RowType)
                                                KEYED.toPhysicalRowDataType().getLogicalType(),
                                        new int[] {0},
                                        List.of(),
                                        List.of()),
                                "users/alice/orders",
                                WriteMode.SET));
        assertThat(config.getEmulatorEndpoint()).isNull();
        assertThat(config.getServiceAccountKeyFile()).isNull();
    }

    @Test
    void theNamedDatabaseTheWriteModeAndTheEmulatorReachTheSink() {
        Map<String, String> options = options();
        options.put("database", "orders-db");
        options.put("sink.write-mode", "merge");
        options.put("emulator-endpoint", "localhost:8080");
        FirestoreSinkConfig<?> config = config(KEYED, options);

        assertThat(config.getDatabase())
                .isEqualTo(DatabaseDestination.of("my-project", "orders-db"));
        assertThat(config.getEmulatorEndpoint()).isNotNull();
        assertThat(((RowDataSerializationSchema) config.getSerializer()))
                .isEqualTo(
                        new RowDataSerializationSchema(
                                FirestoreTableSchema.of(
                                        (org.apache.flink.table.types.logical.RowType)
                                                KEYED.toPhysicalRowDataType().getLogicalType(),
                                        new int[] {0},
                                        List.of(),
                                        List.of()),
                                "users/alice/orders",
                                WriteMode.MERGE));
    }

    @Test
    void aKeyedTableTakesUpsertsAndAnUnkeyedOneInsertsOnly() {
        DynamicTableSink keyed = FactoryMocks.createTableSink(KEYED, options());
        DynamicTableSink unkeyed = FactoryMocks.createTableSink(UNKEYED, options());

        assertThat(keyed.getChangelogMode(ChangelogMode.all()).contains(RowKind.DELETE)).isTrue();
        assertThat(keyed.getChangelogMode(ChangelogMode.all()).contains(RowKind.UPDATE_BEFORE))
                .isFalse();
        assertThat(keyed.getChangelogMode(ChangelogMode.insertOnly()))
                .isEqualTo(ChangelogMode.insertOnly());
        assertThat(unkeyed.getChangelogMode(ChangelogMode.all()))
                .isEqualTo(ChangelogMode.insertOnly());
    }

    @Test
    void theSinkReportsTheCollectionUnderTheTablesName() {
        Sink<?> sink = runtimeSink(KEYED, options("database", "db"));

        assertThat(((LineageVertexProvider) sink).getLineageVertex().datasets())
                .singleElement()
                .satisfies(
                        dataset -> {
                            assertThat(dataset.name()).isEqualTo("default.default.t1");
                            assertThat(dataset.namespace()).isEqualTo("firestore://my-project/db");
                            assertThat(dataset.facets()).containsOnlyKeys("gcp");
                            assertThat(
                                            ((PhysicalResourceFacet) dataset.facets().get("gcp"))
                                                    .resources())
                                    .singleElement()
                                    .satisfies(
                                            resource -> {
                                                assertThat(resource.kind())
                                                        .isEqualTo("firestore-collection");
                                                assertThat(resource.namespace())
                                                        .isEqualTo("firestore://my-project/db");
                                                assertThat(resource.name())
                                                        .isEqualTo("users/alice/orders");
                                                assertThat(resource.identity())
                                                        .isEqualTo(
                                                                Map.of(
                                                                        "project",
                                                                        "my-project",
                                                                        "database",
                                                                        "db",
                                                                        "collection",
                                                                        "users/alice/orders"));
                                            });
                        });
    }

    @Test
    void theKeyFileTheEndpointAndTheParallelismReachTheRuntimeSink() {
        Map<String, String> withKeyFile = options("service-account-key-file", "/secrets/key.json");
        withKeyFile.put("sink.parallelism", "3");
        assertThat(config(KEYED, withKeyFile).getServiceAccountKeyFile())
                .isEqualTo("/secrets/key.json");
        assertThat(
                        ((SinkV2Provider)
                                        FactoryMocks.createTableSink(KEYED, withKeyFile)
                                                .getSinkRuntimeProvider(
                                                        new SinkRuntimeProviderContext(false)))
                                .getParallelism())
                .contains(3);

        assertThat(
                        config(KEYED, options("emulator-endpoint", "localhost:8085"))
                                .getEmulatorEndpoint())
                .isEqualTo(EmulatorEndpoint.parse("localhost:8085", "emulator-endpoint"));
    }

    @Test
    void aMalformedEmulatorEndpointIsRefusedWhenPlannedUnderItsKey() {
        assertThatThrownBy(
                        () ->
                                FactoryMocks.createTableSink(
                                        KEYED, options("emulator-endpoint", "no-port")))
                .hasStackTraceContaining("emulator-endpoint must be host:port");
    }

    @Test
    void copyIsEqualToTheOriginalAndKeepsEveryField() {
        Map<String, String> everything = options("emulator-endpoint", "localhost:8085");
        everything.put("database", "db");
        everything.put("sink.write-mode", "merge");
        everything.put("sink.parallelism", "2");
        everything.put("sink.in-flight.max-writes", "17");
        DynamicTableSink sink = FactoryMocks.createTableSink(KEYED, everything);

        assertThat(sink.copy())
                .isEqualTo(sink)
                .isNotEqualTo(FactoryMocks.createTableSink(KEYED, options()));
        Map<String, String> withKeyFile = options("service-account-key-file", "/k.json");
        DynamicTableSink keyed = FactoryMocks.createTableSink(KEYED, withKeyFile);
        assertThat(keyed.copy())
                .isEqualTo(keyed)
                .isNotEqualTo(FactoryMocks.createTableSink(KEYED, options()));
    }

    @Test
    void anUpdateTableTakesNoDeletes() {
        DynamicTableSink update =
                FactoryMocks.createTableSink(KEYED, options("sink.write-mode", "update"));

        ChangelogMode mode = update.getChangelogMode(ChangelogMode.all());
        assertThat(mode.contains(RowKind.INSERT)).isTrue();
        assertThat(mode.contains(RowKind.UPDATE_AFTER)).isTrue();
        assertThat(mode.contains(RowKind.DELETE)).isFalse();
        assertThat(mode.contains(RowKind.UPDATE_BEFORE)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"users/alice", "users//orders", "/users", "users/", " users"})
    void aCollectionPathThatIsNotACollectionIsRefused(String collection) {
        assertThatThrownBy(
                        () ->
                                FactoryMocks.createTableSink(
                                        KEYED, options("collection", collection)))
                .hasStackTraceContaining("Option 'collection' is invalid:");
    }

    @Test
    void aDocumentPathIsRefusedSayingWhatACollectionPathIs() {
        assertThatThrownBy(
                        () ->
                                FactoryMocks.createTableSink(
                                        KEYED, options("collection", "users/alice")))
                .hasStackTraceContaining("has 2 segments, which names a document");
    }

    @Test
    void aWriteModeWithoutAKeyIsRefused() {
        assertThatThrownBy(
                        () ->
                                FactoryMocks.createTableSink(
                                        UNKEYED, options("sink.write-mode", "set")))
                .hasStackTraceContaining("needs a PRIMARY KEY, the document id");
    }

    @Test
    void anUpdateOfATableWithNoFieldIsRefused() {
        ResolvedSchema keyOnly =
                new ResolvedSchema(
                        List.of(Column.physical("id", DataTypes.STRING().notNull())),
                        List.of(),
                        UniqueConstraint.primaryKey("pk", List.of("id")));

        assertThatThrownBy(
                        () ->
                                FactoryMocks.createTableSink(
                                        keyOnly, options("sink.write-mode", "update")))
                .hasStackTraceContaining("there is no field to replace");
    }

    @Test
    void credentialsAndTheEmulatorAreRefusedTogether() {
        Map<String, String> both = options("emulator-endpoint", "localhost:8080");
        both.put("service-account-key-file", "/key.json");

        assertThatThrownBy(() -> FactoryMocks.createTableSink(KEYED, both))
                .hasStackTraceContaining("an emulator connects without credentials");
        assertThatThrownBy(
                        () ->
                                FactoryMocks.createTableSink(
                                        KEYED, options("service-account-key-file", " ")))
                .hasStackTraceContaining("service-account-key-file must not be blank.");
    }

    @Test
    void aProjectOrDatabaseThatIsNotAPathComponentIsRefusedUnderItsKey() {
        assertThatThrownBy(() -> FactoryMocks.createTableSink(KEYED, options("project", "a/b")))
                .hasStackTraceContaining(
                        "Option 'project' is invalid: project must not contain '/'");
        assertThatThrownBy(() -> FactoryMocks.createTableSink(KEYED, options("database", "a/b")))
                .hasStackTraceContaining(
                        "Option 'database' is invalid: database must not contain '/'");
    }

    @Test
    void aSchemaTheConnectorCannotStoreIsRefusedWhenPlanned() {
        ResolvedSchema intColumn = ResolvedSchema.of(Column.physical("n", DataTypes.INT()));

        assertThatThrownBy(() -> FactoryMocks.createTableSink(intColumn, options()))
                .hasStackTraceContaining("write an INT as a BIGINT");
    }
}
