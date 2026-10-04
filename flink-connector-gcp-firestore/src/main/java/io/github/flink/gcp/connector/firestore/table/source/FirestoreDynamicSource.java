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

package io.github.flink.gcp.connector.firestore.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.table.api.ValidationException;
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
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import com.google.cloud.firestore.FieldPath;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.source.FirestoreSource;
import io.github.flink.gcp.connector.firestore.source.FirestoreSourceBuilder;
import io.github.flink.gcp.connector.firestore.table.FirestoreTableLineage;
import io.github.flink.gcp.connector.firestore.table.FirestoreTableSchema;
import io.github.flink.gcp.connector.firestore.table.TypeMismatchPolicy;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The {@code firestore} table's bounded scan, backed by the DataStream {@code FirestoreSource}, and
 * its lookup by document id.
 *
 * <p>The table's collection is read as one query, one split; under {@code scan.collection-group}
 * every collection with its id is read as a partitioned collection-group scan. A lookup reads one
 * document of the collection by its id. Each reads only the fields of the columns it produces, each
 * named as one literal field: the declared columns, or the projected ones when the planner pushed a
 * projection down.
 */
@Internal
public final class FirestoreDynamicSource
        implements ScanTableSource,
                LookupTableSource,
                SupportsProjectionPushDown,
                SupportsReadingMetadata {

    private final FirestoreTableSchema schema;
    private final DatabaseDestination database;
    private final String collection;
    private final ScanConfig scanConfig;
    private final LookupConfig lookupConfig;
    private final TypeMismatchPolicy policy;
    @Nullable private final String emulatorEndpoint;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final Integer parallelism;
    @Nullable private final FirestoreTableLineage lineage;

    private int[] columns;
    private List<String> metadataKeys = List.of();
    private DataType producedDataType;

    /**
     * Creates the source over every physical column and no metadata.
     *
     * @param schema the checked table schema
     * @param database the database
     * @param collection the collection path
     * @param scanConfig the scan's options
     * @param lookupConfig the lookup's options
     * @param policy what a mismatched value does
     * @param physicalDataType the physical row type
     * @param emulatorEndpoint the emulator endpoint, or {@code null} for the service
     * @param serviceAccountKeyFile the key file, or {@code null} for application default ones
     * @param parallelism the source parallelism, or {@code null} for the planner's
     * @param lineage the lineage to report, or {@code null} for none
     */
    public FirestoreDynamicSource(
            FirestoreTableSchema schema,
            DatabaseDestination database,
            String collection,
            ScanConfig scanConfig,
            LookupConfig lookupConfig,
            TypeMismatchPolicy policy,
            DataType physicalDataType,
            @Nullable String emulatorEndpoint,
            @Nullable String serviceAccountKeyFile,
            @Nullable Integer parallelism,
            @Nullable FirestoreTableLineage lineage) {
        this.schema = Preconditions.checkNotNull(schema, "schema must not be null");
        this.database = Preconditions.checkNotNull(database, "database must not be null");
        this.collection = Preconditions.checkNotNull(collection, "collection must not be null");
        this.scanConfig = Preconditions.checkNotNull(scanConfig, "scanConfig must not be null");
        this.lookupConfig =
                Preconditions.checkNotNull(lookupConfig, "lookupConfig must not be null");
        this.policy = Preconditions.checkNotNull(policy, "policy must not be null");
        this.emulatorEndpoint = emulatorEndpoint;
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.parallelism = parallelism;
        this.lineage = lineage;
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
        FirestoreSourceBuilder<RowData> builder =
                FirestoreSource.<RowData>builder()
                        .database(database)
                        .deserializer(
                                new RowDataDeserializationSchema(
                                        schema, columns, metadataKeys, policy, producedType));
        String[] fields = fieldsRead();
        if (scanConfig.isCollectionGroup()) {
            FieldPath[] paths = new FieldPath[fields.length];
            for (int i = 0; i < paths.length; i++) {
                paths[i] = FieldPath.of(fields[i]);
            }
            // No field read selects each document's name alone.
            builder.collectionGroup(collectionId(collection))
                    .select(paths.length == 0 ? new FieldPath[] {FieldPath.documentId()} : paths);
        } else {
            builder.query(new CollectionQueryFactory(collection, fields));
        }
        scanConfig.applyTo(builder);
        if (emulatorEndpoint != null) {
            builder.emulatorEndpoint(emulatorEndpoint);
        }
        if (serviceAccountKeyFile != null) {
            builder.serviceAccountKeyFile(serviceAccountKeyFile);
        }
        Source<RowData, ?, ?> source = builder.build();
        return SourceProvider.of(
                lineage == null ? source : lineage.source(source, producedType), parallelism);
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        checkLookupKey(context.getKeys());
        RowDataDeserializationSchema deserializer =
                new RowDataDeserializationSchema(
                        schema,
                        columns,
                        metadataKeys,
                        policy,
                        context.createTypeInformation(producedDataType));
        // The collection is always the one addressed: a collection-group table has no key.
        DocumentLookup lookup =
                new CollectionDocumentLookup(
                        database,
                        collection,
                        fieldsRead(),
                        emulatorEndpoint,
                        serviceAccountKeyFile);
        int maxRetries = lookupConfig.getMaxRetries();
        boolean partial = lookupConfig.getCacheType() == LookupCacheType.PARTIAL;
        if (lookupConfig.isAsync()) {
            RowDataAsyncLookupFunction function =
                    new RowDataAsyncLookupFunction(deserializer, maxRetries, lookup);
            return partial
                    ? PartialCachingAsyncLookupProvider.of(
                            function, lookupConfig.createPartialCache())
                    : AsyncLookupFunctionProvider.of(function);
        }
        RowDataLookupFunction function =
                new RowDataLookupFunction(deserializer, maxRetries, lookup);
        return partial
                ? PartialCachingLookupProvider.of(function, lookupConfig.createPartialCache())
                : LookupFunctionProvider.of(function);
    }

    /**
     * Accepts a lookup only on the document id: exactly one equality key, the PRIMARY KEY column.
     */
    private void checkLookupKey(int[][] keys) {
        if (!schema.hasPrimaryKey()
                || keys.length != 1
                || keys[0].length != 1
                || keys[0][0] < 0
                || keys[0][0] >= columns.length
                || columns[keys[0][0]] != schema.getKeyIndex()) {
            if (!schema.hasPrimaryKey()) {
                throw new ValidationException(
                        "A Firestore lookup reads one document by its id, so it requires an"
                                + " equality predicate on the PRIMARY KEY column. A table without a"
                                + " PRIMARY KEY, as a collection-group table is, cannot be looked"
                                + " up.");
            }
            throw new ValidationException(
                    "A Firestore lookup reads one document by its id, so it requires an equality"
                            + " predicate on the PRIMARY KEY column '"
                            + schema.getRowType().getFieldNames().get(schema.getKeyIndex())
                            + "', the document id, and on no other column. An equality between"
                            + " another column of the table and a constant, in ON or in WHERE,"
                            + " also becomes a lookup key, so write such a condition another"
                            + " way.");
        }
    }

    /**
     * The names of the fields the produced columns read: every column but the document id. A
     * document's undeclared fields are never read.
     */
    private String[] fieldsRead() {
        List<String> fields = new ArrayList<>();
        for (int column : columns) {
            if (column != schema.getKeyIndex()) {
                fields.add(schema.getRowType().getFieldNames().get(column));
            }
        }
        return fields.toArray(new String[0]);
    }

    /** The collection id of a collection path: its last segment. */
    public static String collectionId(String collection) {
        return collection.substring(collection.lastIndexOf('/') + 1);
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

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
        FirestoreDynamicSource copy =
                new FirestoreDynamicSource(
                        schema,
                        database,
                        collection,
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
        return "Firestore table source";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FirestoreDynamicSource that = (FirestoreDynamicSource) o;
        return schema.equals(that.schema)
                && database.equals(that.database)
                && collection.equals(that.collection)
                && scanConfig.equals(that.scanConfig)
                && lookupConfig.equals(that.lookupConfig)
                && policy == that.policy
                && Objects.equals(emulatorEndpoint, that.emulatorEndpoint)
                && Objects.equals(serviceAccountKeyFile, that.serviceAccountKeyFile)
                && Objects.equals(parallelism, that.parallelism)
                && Objects.equals(lineage, that.lineage)
                && Arrays.equals(columns, that.columns)
                && metadataKeys.equals(that.metadataKeys)
                && producedDataType.equals(that.producedDataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                schema,
                database,
                collection,
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
