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

package io.github.flink.gcp.connector.datastore.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupOptions.LookupCacheType;
import org.apache.flink.table.connector.source.lookup.PartialCachingAsyncLookupProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.table.LookupKeyFilter;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.source.DatastoreSource;
import io.github.flink.gcp.connector.datastore.source.DatastoreSourceBuilder;
import io.github.flink.gcp.connector.datastore.table.DatastoreLookupConfig;
import io.github.flink.gcp.connector.datastore.table.DatastoreScanConfig;
import io.github.flink.gcp.connector.datastore.table.DatastoreTableLineage;
import io.github.flink.gcp.connector.datastore.table.DatastoreTableSchema;
import io.github.flink.gcp.connector.datastore.table.TypeMismatchPolicy;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The {@code datastore} table's bounded scan, backed by the DataStream {@code DatastoreSource}:
 * every entity of the table's kind in its namespace, read in key ranges at one read time; and its
 * lookup of one entity by key.
 *
 * <p>The scan reads whole entities and converts only the columns it produces. A projection the
 * planner pushes down is not sent to the service: a Datastore projection query returns only the
 * entities that hold an indexed value of every projected property, and one result per value of an
 * array property, so it would drop or repeat rows. A lookup asks for the produced columns'
 * properties through the lookup's property mask instead, which keeps every entity whole in that
 * respect: it returns the entity whatever its indexes, and an array whole.
 */
@Internal
public final class DatastoreDynamicSource
        implements ScanTableSource,
                LookupTableSource,
                SupportsProjectionPushDown,
                SupportsReadingMetadata {

    private final DatastoreTableSchema schema;
    private final DatabaseDestination database;
    private final String namespace;
    private final String kind;
    private final DatastoreScanConfig scanConfig;
    private final DatastoreLookupConfig lookupConfig;
    private final TypeMismatchPolicy policy;
    @Nullable private final String emulatorEndpoint;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final Integer parallelism;
    private final DatastoreTableLineage lineage;

    private int[] columns;
    private List<String> metadataKeys = List.of();
    private DataType producedDataType;

    /**
     * Creates the source over every physical column and no metadata.
     *
     * @param schema the checked table schema
     * @param database the database
     * @param namespace the namespace, empty for the default one
     * @param kind the kind
     * @param scanConfig the scan's options
     * @param lookupConfig the lookup's options
     * @param policy what a mismatched value does
     * @param physicalDataType the physical row type
     * @param emulatorEndpoint the emulator endpoint, or {@code null} for the service
     * @param serviceAccountKeyFile the key file, or {@code null} for application default ones
     * @param parallelism the source parallelism, or {@code null} for the planner's
     * @param lineage the lineage to report
     */
    public DatastoreDynamicSource(
            DatastoreTableSchema schema,
            DatabaseDestination database,
            String namespace,
            String kind,
            DatastoreScanConfig scanConfig,
            DatastoreLookupConfig lookupConfig,
            TypeMismatchPolicy policy,
            DataType physicalDataType,
            @Nullable String emulatorEndpoint,
            @Nullable String serviceAccountKeyFile,
            @Nullable Integer parallelism,
            DatastoreTableLineage lineage) {
        this.schema = Preconditions.checkNotNull(schema, "schema must not be null");
        this.database = Preconditions.checkNotNull(database, "database must not be null");
        this.namespace = Preconditions.checkNotNull(namespace, "namespace must not be null");
        this.kind = Preconditions.checkNotNull(kind, "kind must not be null");
        this.scanConfig = Preconditions.checkNotNull(scanConfig, "scanConfig must not be null");
        this.lookupConfig =
                Preconditions.checkNotNull(lookupConfig, "lookupConfig must not be null");
        this.policy = Preconditions.checkNotNull(policy, "policy must not be null");
        this.emulatorEndpoint = emulatorEndpoint;
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.parallelism = parallelism;
        this.lineage = Preconditions.checkNotNull(lineage, "lineage must not be null");
        this.columns = new int[schema.getRowType().getFieldCount()];
        for (int i = 0; i < columns.length; i++) {
            columns[i] = i;
        }
        this.producedDataType = physicalDataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        TypeInformation<RowData> producedType = context.createTypeInformation(producedDataType);
        DatastoreSourceBuilder<RowData> builder =
                DatastoreSource.<RowData>builder()
                        .database(database)
                        .deserializer(
                                new RowDataDeserializationSchema(
                                        schema, columns, metadataKeys, policy, producedType))
                        .kind(kind);
        if (!namespace.isEmpty()) {
            builder.namespace(namespace);
        }
        scanConfig.applyTo(builder);
        if (emulatorEndpoint != null) {
            builder.emulatorEndpoint(emulatorEndpoint);
        }
        if (serviceAccountKeyFile != null) {
            builder.serviceAccountKeyFile(serviceAccountKeyFile);
        }
        Source<RowData, ?, ?> source = builder.build();
        return SourceProvider.of(lineage.source(source, producedType), parallelism);
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        LookupKeyFilter equalityKeys = lookupKeys(context.getKeys());
        RowDataDeserializationSchema deserializer =
                new RowDataDeserializationSchema(
                        schema,
                        columns,
                        metadataKeys,
                        policy,
                        context.createTypeInformation(producedDataType));
        DatastoreLookupKeys keys =
                new DatastoreLookupKeys(database, namespace, kind, schema.isKeyId());
        DatastoreEntityLookup lookup =
                new DatastoreKindEntityLookup(
                        database, propertiesRead(), emulatorEndpoint, serviceAccountKeyFile);
        int maxRetries = lookupConfig.getMaxRetries();
        boolean partial = lookupConfig.getCacheType() == LookupCacheType.PARTIAL;
        if (lookupConfig.isAsync()) {
            AsyncLookupFunction function =
                    equalityKeys.wrap(
                            new DatastoreRowDataAsyncLookupFunction(
                                    deserializer, keys, maxRetries, lookup));
            return partial
                    ? PartialCachingAsyncLookupProvider.of(
                            function, equalityKeys.wrap(lookupConfig.createPartialCache()))
                    : AsyncLookupFunctionProvider.of(function);
        }
        LookupFunction function =
                equalityKeys.wrap(
                        new DatastoreRowDataLookupFunction(deserializer, keys, maxRetries, lookup));
        return partial
                ? PartialCachingLookupProvider.of(
                        function, equalityKeys.wrap(lookupConfig.createPartialCache()))
                : LookupFunctionProvider.of(function);
    }

    private LookupKeyFilter lookupKeys(int[][] keys) {
        String requirement =
                schema.hasPrimaryKey()
                        ? "A Datastore lookup requires an equality predicate on the PRIMARY KEY column '"
                                + schema.getRowType().getFieldNames().get(schema.getKeyIndex())
                                + "', the entity key's name or id. Additional lookup keys must name top-level physical scalar columns; metadata and nested key paths are unsupported."
                        : "A Datastore lookup requires an equality predicate on the PRIMARY KEY column, the entity key's name or id. A table without a PRIMARY KEY cannot be looked up.";
        return LookupKeyFilter.of(
                (RowType) producedDataType.getLogicalType(),
                keys,
                columns,
                schema.hasPrimaryKey() ? new int[] {schema.getKeyIndex()} : new int[0],
                false,
                requirement);
    }

    /**
     * The names of the properties the produced columns read: every column but the key. An entity's
     * undeclared properties are never read.
     */
    private String[] propertiesRead() {
        List<String> properties = new ArrayList<>();
        for (int column : columns) {
            if (column != schema.getKeyIndex()) {
                properties.add(schema.getRowType().getFieldNames().get(column));
            }
        }
        return properties.toArray(new String[0]);
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    /** Keeps the projected columns; the scan still reads whole entities, as the class says. */
    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        int[] projected = new int[projectedFields.length];
        for (int i = 0; i < projected.length; i++) {
            projected[i] = projectedFields[i][0];
        }
        this.columns = projected;
        this.producedDataType = producedDataType;
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        return ReadableMetadata.listAll();
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        // The planner's list is its own plan state; keep a copy.
        this.metadataKeys = List.copyOf(metadataKeys);
        this.producedDataType = producedDataType;
    }

    @Override
    public DynamicTableSource copy() {
        DatastoreDynamicSource copy =
                new DatastoreDynamicSource(
                        schema,
                        database,
                        namespace,
                        kind,
                        scanConfig,
                        lookupConfig,
                        policy,
                        producedDataType,
                        emulatorEndpoint,
                        serviceAccountKeyFile,
                        parallelism,
                        lineage);
        copy.columns = columns.clone();
        copy.metadataKeys = metadataKeys;
        copy.producedDataType = producedDataType;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "Datastore table source";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DatastoreDynamicSource that = (DatastoreDynamicSource) o;
        return schema.equals(that.schema)
                && database.equals(that.database)
                && namespace.equals(that.namespace)
                && kind.equals(that.kind)
                && scanConfig.equals(that.scanConfig)
                && lookupConfig.equals(that.lookupConfig)
                && policy == that.policy
                && Objects.equals(emulatorEndpoint, that.emulatorEndpoint)
                && Objects.equals(serviceAccountKeyFile, that.serviceAccountKeyFile)
                && Objects.equals(parallelism, that.parallelism)
                && lineage.equals(that.lineage)
                && Arrays.equals(columns, that.columns)
                && metadataKeys.equals(that.metadataKeys)
                && producedDataType.equals(that.producedDataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                schema,
                database,
                namespace,
                kind,
                scanConfig,
                lookupConfig,
                policy,
                emulatorEndpoint,
                serviceAccountKeyFile,
                parallelism,
                lineage,
                Arrays.hashCode(columns),
                metadataKeys,
                producedDataType);
    }
}
