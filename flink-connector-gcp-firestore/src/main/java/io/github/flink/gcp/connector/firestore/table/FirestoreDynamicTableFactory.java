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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.RowType;

import io.github.flink.gcp.connector.base.options.ResourceNames;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.table.OptionSetters;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.table.sink.FirestoreDynamicSink;
import io.github.flink.gcp.connector.firestore.table.sink.WriterOptionsMapper;
import io.github.flink.gcp.connector.firestore.table.source.FirestoreDynamicSource;
import io.github.flink.gcp.connector.firestore.table.source.ScanConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Creates the {@code firestore} table source and sink from a SQL DDL: the documents of one
 * collection, or a scan of every collection with its id, a row per document, the PRIMARY KEY column
 * as the document id.
 *
 * <p>Every check that needs only the {@code WITH} clause and the schema runs here, so a mistake is
 * reported when the statement is planned, in the option keys the DDL spells.
 */
@Internal
public final class FirestoreDynamicTableFactory
        implements DynamicTableSinkFactory, DynamicTableSourceFactory {

    /** The {@code connector} value that selects this factory. */
    public static final String IDENTIFIER = "firestore";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>(
                Arrays.asList(
                        FirestoreConnectorOptions.PROJECT, FirestoreConnectorOptions.COLLECTION));
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>(
                Arrays.asList(
                        FirestoreConnectorOptions.DATABASE,
                        FirestoreConnectorOptions.EMULATOR_ENDPOINT,
                        FirestoreConnectorOptions.SERVICE_ACCOUNT_KEY_FILE,
                        FirestoreConnectorOptions.GEO_POINT_FIELD_PATHS,
                        FirestoreConnectorOptions.REFERENCE_FIELD_PATHS,
                        FirestoreConnectorOptions.TYPE_MISMATCH_POLICY,
                        FirestoreConnectorOptions.SCAN_COLLECTION_GROUP,
                        FirestoreConnectorOptions.SCAN_PARTITION_MAX_PARTITIONS,
                        FirestoreConnectorOptions.SCAN_READ_TIME,
                        FirestoreConnectorOptions.SCAN_MAX_ROWS_PER_FETCH,
                        FirestoreConnectorOptions.SINK_WRITE_MODE,
                        FirestoreConnectorOptions.SINK_THROTTLING_ENABLED,
                        FirestoreConnectorOptions.SINK_THROTTLING_INITIAL_OPS_PER_SECOND,
                        FirestoreConnectorOptions.SINK_THROTTLING_MAX_OPS_PER_SECOND,
                        FirestoreConnectorOptions.SINK_WRITE_MAX_ATTEMPTS,
                        FirestoreConnectorOptions.SINK_RETRY_TOTAL_TIMEOUT,
                        FirestoreConnectorOptions.SINK_RETRY_INITIAL_DELAY,
                        FirestoreConnectorOptions.SINK_RETRY_DELAY_MULTIPLIER,
                        FirestoreConnectorOptions.SINK_RETRY_MAX_DELAY,
                        FirestoreConnectorOptions.SINK_RETRY_INITIAL_RPC_TIMEOUT,
                        FirestoreConnectorOptions.SINK_RETRY_RPC_TIMEOUT_MULTIPLIER,
                        FirestoreConnectorOptions.SINK_RETRY_MAX_RPC_TIMEOUT,
                        FirestoreConnectorOptions.SINK_RETRY_MAX_ATTEMPTS,
                        FirestoreConnectorOptions.SINK_IN_FLIGHT_MAX_WRITES,
                        FirestoreConnectorOptions.SINK_IN_FLIGHT_MAX_BYTES,
                        FactoryUtil.SINK_PARALLELISM,
                        FactoryUtil.SOURCE_PARALLELISM));
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        ReadableConfig config = validatedOptions(context);
        DatabaseDestination database = database(config);
        String collection = collection(config);
        FirestoreTableSchema schema = schema(context, config);
        WriteMode writeMode = config.get(FirestoreConnectorOptions.SINK_WRITE_MODE);
        validateWriteMode(context, schema, writeMode);

        return FirestoreDynamicSink.builder()
                .schema(schema)
                .database(database)
                .collection(collection)
                .writeMode(writeMode)
                .writerOptions(WriterOptionsMapper.map(config))
                .emulatorEndpoint(
                        config.getOptional(FirestoreConnectorOptions.EMULATOR_ENDPOINT)
                                .orElse(null))
                .serviceAccountKeyFile(
                        config.getOptional(FirestoreConnectorOptions.SERVICE_ACCOUNT_KEY_FILE)
                                .orElse(null))
                .parallelism(config.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null))
                .build(
                        FirestoreTableLineage.of(
                                context.getObjectIdentifier().asSummaryString(),
                                database,
                                collection));
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        ReadableConfig config = validatedOptions(context);
        DatabaseDestination database = database(config);
        String collection = collection(config);
        FirestoreTableSchema schema = schema(context, config);
        ScanConfig scanConfig = ScanConfig.from(config);
        if (scanConfig.isCollectionGroup() && schema.hasPrimaryKey()) {
            // A document id is unique only within its collection, and the planner trusts a
            // declared key to be unique: it would drop a GROUP BY or a DISTINCT over it.
            throw new ValidationException(
                    "A scan with "
                            + FirestoreConnectorOptions.SCAN_COLLECTION_GROUP.key()
                            + " = 'true' reads documents of many collections, whose ids repeat"
                            + " across them, so the table cannot declare a PRIMARY KEY. Read"
                            + " the 'document-path' metadata column, which is unique, instead.");
        }
        String logicalName = context.getObjectIdentifier().asSummaryString();
        return new FirestoreDynamicSource(
                schema,
                database,
                collection,
                scanConfig,
                config.get(FirestoreConnectorOptions.TYPE_MISMATCH_POLICY),
                context.getPhysicalRowDataType(),
                config.getOptional(FirestoreConnectorOptions.EMULATOR_ENDPOINT).orElse(null),
                config.getOptional(FirestoreConnectorOptions.SERVICE_ACCOUNT_KEY_FILE).orElse(null),
                config.getOptional(FactoryUtil.SOURCE_PARALLELISM).orElse(null),
                scanConfig.isCollectionGroup()
                        ? FirestoreTableLineage.ofCollectionGroup(
                                logicalName,
                                database,
                                FirestoreDynamicSource.collectionId(collection))
                        : FirestoreTableLineage.of(logicalName, database, collection));
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
        config.getOptional(FirestoreConnectorOptions.EMULATOR_ENDPOINT)
                .ifPresent(
                        value ->
                                EmulatorEndpoint.parse(
                                        value, FirestoreConnectorOptions.EMULATOR_ENDPOINT.key()));
        return config;
    }

    private static FirestoreTableSchema schema(Context context, ReadableConfig config) {
        return FirestoreTableSchema.of(
                (RowType) context.getPhysicalRowDataType().getLogicalType(),
                context.getPrimaryKeyIndexes(),
                config.getOptional(FirestoreConnectorOptions.GEO_POINT_FIELD_PATHS)
                        .orElse(Collections.emptyList()),
                config.getOptional(FirestoreConnectorOptions.REFERENCE_FIELD_PATHS)
                        .orElse(Collections.emptyList()));
    }

    /**
     * Refuses a blank key file, and a key file beside an emulator endpoint, which connects without
     * credentials; the sink's builder refuses both too, but in its setters' names.
     */
    private static void validateCredentialsMode(ReadableConfig config) {
        String keyFile =
                config.getOptional(FirestoreConnectorOptions.SERVICE_ACCOUNT_KEY_FILE).orElse(null);
        if (keyFile != null && keyFile.isBlank()) {
            throw new ValidationException(
                    FirestoreConnectorOptions.SERVICE_ACCOUNT_KEY_FILE.key()
                            + " must not be blank.");
        }
        if (keyFile != null
                && config.getOptional(FirestoreConnectorOptions.EMULATOR_ENDPOINT).isPresent()) {
            throw new ValidationException(
                    FirestoreConnectorOptions.SERVICE_ACCOUNT_KEY_FILE.key()
                            + " cannot be combined with "
                            + FirestoreConnectorOptions.EMULATOR_ENDPOINT.key()
                            + ": an emulator connects without credentials.");
        }
    }

    /** The database, each component checked under the option key that supplied it. */
    private static DatabaseDestination database(ReadableConfig config) {
        String project = config.get(FirestoreConnectorOptions.PROJECT);
        checkComponent(FirestoreConnectorOptions.PROJECT, project);
        String databaseId = config.getOptional(FirestoreConnectorOptions.DATABASE).orElse(null);
        if (databaseId == null) {
            return DatabaseDestination.of(project);
        }
        checkComponent(FirestoreConnectorOptions.DATABASE, databaseId);
        return DatabaseDestination.of(project, databaseId);
    }

    /**
     * The collection path: an odd number of {@code '/'}-separated segments, each a component the
     * path is composed from (ADR-0127). An even number would name a document, and the sink would
     * then write into a collection the DDL never named.
     */
    private static String collection(ReadableConfig config) {
        String collection = config.get(FirestoreConnectorOptions.COLLECTION);
        String[] segments = collection.split("/", -1);
        for (String segment : segments) {
            OptionSetters.accept(
                    FirestoreConnectorOptions.COLLECTION.key(),
                    segment,
                    value ->
                            ResourceNames.checkComponent(
                                    value, "each segment of '" + collection + "'"));
        }
        OptionSetters.accept(
                FirestoreConnectorOptions.COLLECTION.key(),
                segments.length,
                count -> {
                    if (count % 2 == 0) {
                        throw new IllegalArgumentException(
                                "'"
                                        + collection
                                        + "' has "
                                        + count
                                        + " segments, which names a document. A collection path"
                                        + " alternates collection and document ids and ends with"
                                        + " a collection id, for example 'users' or"
                                        + " 'users/alice/orders'.");
                    }
                });
        return collection;
    }

    private static void checkComponent(ConfigOption<String> option, String value) {
        OptionSetters.accept(
                option.key(), value, v -> ResourceNames.checkComponent(v, option.key()));
    }

    /**
     * Refuses a {@code sink.write-mode} on a table without a PRIMARY KEY, whose rows are always
     * created under new ids, and an {@code update} that would name no field.
     */
    private static void validateWriteMode(
            Context context, FirestoreTableSchema schema, WriteMode writeMode) {
        String key = FirestoreConnectorOptions.SINK_WRITE_MODE.key();
        if (!schema.hasPrimaryKey() && context.getCatalogTable().getOptions().containsKey(key)) {
            throw new ValidationException(
                    key
                            + " needs a PRIMARY KEY, the document id: without one every row is"
                            + " created as a new document. Declare the id column PRIMARY KEY NOT"
                            + " ENFORCED, or remove "
                            + key
                            + ".");
        }
        if (writeMode == WriteMode.UPDATE && schema.getRowType().getFieldCount() == 1) {
            throw new ValidationException(
                    key
                            + " = 'update' replaces the table's fields, but the table declares only"
                            + " its PRIMARY KEY, so there is no field to replace.");
        }
    }
}
