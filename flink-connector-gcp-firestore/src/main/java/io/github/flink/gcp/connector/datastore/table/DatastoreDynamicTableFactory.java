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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.datastore.Key;
import io.github.flink.gcp.connector.base.options.ResourceNames;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.table.OptionSetters;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.table.sink.DatastoreDynamicSink;
import io.github.flink.gcp.connector.datastore.table.sink.WriterOptionsMapper;
import io.github.flink.gcp.connector.datastore.table.source.DatastoreDynamicSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Creates the {@code datastore} table source and sink from a SQL DDL: the entities of one kind in
 * one namespace of a database in Datastore mode, a row per entity, the PRIMARY KEY column as the
 * key's name or id.
 *
 * <p>Every check that needs only the {@code WITH} clause and the schema runs here, so a mistake is
 * reported when the statement is planned, in the option keys the DDL spells.
 */
@Internal
public final class DatastoreDynamicTableFactory
        implements DynamicTableSinkFactory, DynamicTableSourceFactory {

    /** The {@code connector} value that selects this factory. */
    public static final String IDENTIFIER = "datastore";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>(
                Arrays.asList(DatastoreConnectorOptions.PROJECT, DatastoreConnectorOptions.KIND));
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>(
                Arrays.asList(
                        DatastoreConnectorOptions.DATABASE,
                        DatastoreConnectorOptions.NAMESPACE,
                        DatastoreConnectorOptions.EMULATOR_ENDPOINT,
                        DatastoreConnectorOptions.SERVICE_ACCOUNT_KEY_FILE,
                        DatastoreConnectorOptions.TYPE_MISMATCH_POLICY,
                        DatastoreConnectorOptions.SCAN_PARTITION_MAX_PARTITIONS,
                        DatastoreConnectorOptions.SCAN_READ_TIME,
                        DatastoreConnectorOptions.SCAN_MAX_ROWS_PER_FETCH,
                        DatastoreConnectorOptions.LOOKUP_ASYNC,
                        LookupOptions.CACHE_TYPE,
                        LookupOptions.MAX_RETRIES,
                        LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS,
                        LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE,
                        LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY,
                        LookupOptions.PARTIAL_CACHE_MAX_ROWS,
                        DatastoreConnectorOptions.SINK_UNINDEXED_COLUMNS,
                        DatastoreConnectorOptions.SINK_BUFFER_FLUSH_MAX_MUTATIONS,
                        DatastoreConnectorOptions.SINK_BUFFER_FLUSH_MAX_SIZE,
                        DatastoreConnectorOptions.SINK_REQUEST_TIMEOUT,
                        DatastoreConnectorOptions.SINK_RECOVERY_INITIAL_BACKOFF,
                        DatastoreConnectorOptions.SINK_RECOVERY_MAX_BACKOFF,
                        DatastoreConnectorOptions.SINK_RECOVERY_MAX_ATTEMPTS,
                        DatastoreConnectorOptions.SINK_THROTTLING_ENABLED,
                        DatastoreConnectorOptions.SINK_THROTTLING_PARALLELISM,
                        DatastoreConnectorOptions.SINK_ID_ALLOCATION_BATCH_SIZE,
                        FactoryUtil.SINK_PARALLELISM,
                        FactoryUtil.SOURCE_PARALLELISM));
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        ReadableConfig config = validatedOptions(context);
        DatabaseDestination database = database(config);
        String namespace = namespace(config);
        String kind = kind(config);

        return DatastoreDynamicSink.builder()
                .schema(schema(context, config))
                .database(database)
                .namespace(namespace)
                .kind(kind)
                .writerOptions(WriterOptionsMapper.map(config))
                .emulatorEndpoint(
                        config.getOptional(DatastoreConnectorOptions.EMULATOR_ENDPOINT)
                                .orElse(null))
                .serviceAccountKeyFile(
                        config.getOptional(DatastoreConnectorOptions.SERVICE_ACCOUNT_KEY_FILE)
                                .orElse(null))
                .parallelism(config.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null))
                .build(lineage(context, database, namespace, kind));
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        ReadableConfig config = validatedOptions(context);
        DatabaseDestination database = database(config);
        String namespace = namespace(config);
        String kind = kind(config);
        return new DatastoreDynamicSource(
                schema(context, config),
                database,
                namespace,
                kind,
                DatastoreScanConfig.from(config),
                DatastoreLookupConfig.from(config),
                config.get(DatastoreConnectorOptions.TYPE_MISMATCH_POLICY),
                context.getPhysicalRowDataType(),
                config.getOptional(DatastoreConnectorOptions.EMULATOR_ENDPOINT).orElse(null),
                config.getOptional(DatastoreConnectorOptions.SERVICE_ACCOUNT_KEY_FILE).orElse(null),
                config.getOptional(FactoryUtil.SOURCE_PARALLELISM).orElse(null),
                lineage(context, database, namespace, kind));
    }

    /** The table's schema, checked once for either direction. */
    private static DatastoreTableSchema schema(Context context, ReadableConfig config) {
        return DatastoreTableSchema.of(
                (RowType) context.getPhysicalRowDataType().getLogicalType(),
                context.getPrimaryKeyIndexes(),
                config.getOptional(DatastoreConnectorOptions.SINK_UNINDEXED_COLUMNS)
                        .orElse(Collections.emptyList()));
    }

    private static DatastoreTableLineage lineage(
            Context context, DatabaseDestination database, String namespace, String kind) {
        return DatastoreTableLineage.of(
                context.getObjectIdentifier().asSummaryString(), database, namespace, kind);
    }

    /**
     * Validates the options and the checks every direction shares: the credentials mode, and the
     * emulator endpoint's grammar under its key.
     */
    private ReadableConfig validatedOptions(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig config = helper.getOptions();
        validateCredentialsMode(config);
        config.getOptional(DatastoreConnectorOptions.EMULATOR_ENDPOINT)
                .ifPresent(
                        value ->
                                EmulatorEndpoint.parse(
                                        value, DatastoreConnectorOptions.EMULATOR_ENDPOINT.key()));
        return config;
    }

    /**
     * Refuses a blank key file, and a key file beside an emulator endpoint, which connects without
     * credentials; the sink's builder refuses both too, but in its setters' names.
     */
    private static void validateCredentialsMode(ReadableConfig config) {
        String keyFile =
                config.getOptional(DatastoreConnectorOptions.SERVICE_ACCOUNT_KEY_FILE).orElse(null);
        if (keyFile != null && keyFile.isBlank()) {
            throw new ValidationException(
                    DatastoreConnectorOptions.SERVICE_ACCOUNT_KEY_FILE.key()
                            + " must not be blank.");
        }
        if (keyFile != null
                && config.getOptional(DatastoreConnectorOptions.EMULATOR_ENDPOINT).isPresent()) {
            throw new ValidationException(
                    DatastoreConnectorOptions.SERVICE_ACCOUNT_KEY_FILE.key()
                            + " cannot be combined with "
                            + DatastoreConnectorOptions.EMULATOR_ENDPOINT.key()
                            + ": an emulator connects without credentials.");
        }
    }

    /**
     * The database, each component checked under the option key that supplied it. The project is
     * also checked against the client library's grammar, which every key the sink builds is held
     * to: a project it refuses would fail every record. The scan applies the same check, so one DDL
     * is accepted or refused alike in either direction.
     */
    private static DatabaseDestination database(ReadableConfig config) {
        String project = config.get(DatastoreConnectorOptions.PROJECT);
        checkComponent(DatastoreConnectorOptions.PROJECT, project);
        OptionSetters.accept(
                DatastoreConnectorOptions.PROJECT.key(),
                project,
                value -> Key.newBuilder(value, "kind", "name").build());
        String databaseId = config.getOptional(DatastoreConnectorOptions.DATABASE).orElse(null);
        if (databaseId == null) {
            return DatabaseDestination.of(project);
        }
        checkComponent(DatastoreConnectorOptions.DATABASE, databaseId);
        return DatabaseDestination.of(project, databaseId);
    }

    /**
     * The namespace, empty for the default one. A configured namespace is held to the client
     * library's grammar, which every key the sink builds is checked against, and may not be one
     * Datastore reserves: either would fail every record. The scan applies the same checks, as
     * {@link #database(ReadableConfig)} does.
     */
    private static String namespace(ReadableConfig config) {
        String namespace = config.getOptional(DatastoreConnectorOptions.NAMESPACE).orElse(null);
        if (namespace == null) {
            return "";
        }
        OptionSetters.accept(
                DatastoreConnectorOptions.NAMESPACE.key(),
                namespace,
                value -> {
                    if (value.isBlank()) {
                        throw new IllegalArgumentException(
                                "it is blank; leave it out for the default namespace.");
                    }
                    checkNotReserved(value, "namespace");
                    Key.newBuilder("project", "kind", "name").setNamespace(value).build();
                });
        return namespace;
    }

    /**
     * The kind, which may not be blank, one Datastore reserves, or longer than it stores. A write
     * to any of them would fail every record, and the scan applies the same checks, so one DDL is
     * accepted or refused alike in either direction. A reserved kind is Datastore's own statistics
     * or metadata: the emulator answers the {@code __kind__} metadata query without the cursor
     * every entity the scan reads must carry, and no statistics kind was measured, so the scan
     * refuses them too. The client library checks only that a kind is not empty, so these are the
     * service's rules, stated once in {@link DatastoreTableSchema}.
     */
    private static String kind(ReadableConfig config) {
        String kind = config.get(DatastoreConnectorOptions.KIND);
        OptionSetters.accept(
                DatastoreConnectorOptions.KIND.key(),
                kind,
                value -> {
                    if (value.isBlank()) {
                        throw new IllegalArgumentException("it is blank.");
                    }
                    checkNotReserved(value, "kind");
                    if (DatastoreTableSchema.isTooLong(value)) {
                        throw new IllegalArgumentException(
                                "it is longer than "
                                        + DatastoreTableSchema.MAX_NAME_BYTES
                                        + " bytes, which Datastore refuses for a kind.");
                    }
                });
        return kind;
    }

    private static void checkNotReserved(String value, String what) {
        if (DatastoreTableSchema.isReserved(value)) {
            throw new IllegalArgumentException(
                    "'"
                            + value
                            + "' has the form '__…__', with at least one character between the"
                            + " underscores, which Datastore reserves for a "
                            + what
                            + " of its own; the table connector neither writes nor reads one.");
        }
    }

    private static void checkComponent(ConfigOption<String> option, String value) {
        OptionSetters.accept(
                option.key(), value, v -> ResourceNames.checkComponent(v, option.key()));
    }
}
