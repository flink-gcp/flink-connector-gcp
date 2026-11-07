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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupOptions.LookupCacheType;
import org.apache.flink.table.connector.source.lookup.PartialCachingAsyncLookupProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import com.google.cloud.firestore.FieldPath;
import io.github.flink.gcp.connector.base.table.LookupKeyFilter;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.source.FirestoreSource;
import io.github.flink.gcp.connector.firestore.source.FirestoreSourceBuilder;
import io.github.flink.gcp.connector.firestore.table.FirestoreLookupConfig;
import io.github.flink.gcp.connector.firestore.table.FirestoreScanConfig;
import io.github.flink.gcp.connector.firestore.table.FirestoreTableLineage;
import io.github.flink.gcp.connector.firestore.table.FirestoreTableSchema;
import io.github.flink.gcp.connector.firestore.table.TypeMismatchPolicy;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The {@code firestore} table's bounded scan, backed by the DataStream {@code FirestoreSource}, and
 * its lookup by document id.
 *
 * <p>The table's collection is read as one query, one split; under {@code scan.collection-group}
 * every collection with its id is read as a partitioned collection-group scan. A lookup reads one
 * document of the collection by its id. Each reads only the fields of the columns it produces: the
 * declared columns, or the projected ones when the planner pushed a projection down, a projected
 * field of a ROW column read as its nested path where {@code fieldMask} says so. A
 * single-collection scan also adds the equality filters {@link FirestoreFilterPushDown} translates,
 * each a necessary condition that Flink evaluates again; a collection-group scan pushes none
 * ({@code docs/adr/0197}).
 */
@Internal
public final class FirestoreDynamicSource
        implements ScanTableSource,
                LookupTableSource,
                SupportsFilterPushDown,
                SupportsProjectionPushDown,
                SupportsReadingMetadata {

    private final FirestoreTableSchema schema;
    private final DatabaseDestination database;
    private final String collection;
    private final FirestoreScanConfig scanConfig;
    private final FirestoreLookupConfig lookupConfig;
    private final TypeMismatchPolicy policy;
    @Nullable private final String emulatorEndpoint;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final Integer parallelism;
    @Nullable private final FirestoreTableLineage lineage;

    /** The produced physical fields, by index path: a top-level column, then ROW fields. */
    private int[][] columns;

    private List<String> metadataKeys = List.of();
    private DataType producedDataType;
    private FirestoreFilterPushDown.State filterState = FirestoreFilterPushDown.State.empty();

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
            FirestoreScanConfig scanConfig,
            FirestoreLookupConfig lookupConfig,
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
        int[] all = new int[schema.getRowType().getFieldCount()];
        for (int i = 0; i < all.length; i++) {
            all[i] = i;
        }
        this.columns = RowDataDeserializationSchema.topLevel(all);
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
        String[] fields = fieldMask(schema, columns);
        if (scanConfig.isCollectionGroup()) {
            FieldPath[] paths = new FieldPath[fields.length];
            for (int i = 0; i < paths.length; i++) {
                paths[i] = FieldPath.fromServerFormat(fields[i]);
            }
            // No field read selects each document's name alone.
            builder.collectionGroup(collectionId(collection))
                    .select(paths.length == 0 ? new FieldPath[] {FieldPath.documentId()} : paths);
        } else {
            builder.query(
                    new FirestoreCollectionQueryFactory(
                            collection, fields, filterState.equalities()));
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
        LookupKeyFilter equalityKeys = lookupKeys(context.getKeys());
        RowDataDeserializationSchema deserializer =
                new RowDataDeserializationSchema(
                        schema,
                        columns,
                        metadataKeys,
                        policy,
                        context.createTypeInformation(producedDataType));
        // The collection is always the one addressed: a collection-group table has no key.
        FirestoreDocumentLookup lookup =
                new FirestoreCollectionDocumentLookup(
                        database,
                        collection,
                        fieldMask(schema, columns),
                        emulatorEndpoint,
                        serviceAccountKeyFile);
        int maxRetries = lookupConfig.getMaxRetries();
        boolean partial = lookupConfig.getCacheType() == LookupCacheType.PARTIAL;
        if (lookupConfig.isAsync()) {
            AsyncLookupFunction function =
                    equalityKeys.wrap(
                            new FirestoreRowDataAsyncLookupFunction(
                                    deserializer, maxRetries, lookup));
            return partial
                    ? PartialCachingAsyncLookupProvider.of(
                            function, equalityKeys.wrap(lookupConfig.createPartialCache()))
                    : AsyncLookupFunctionProvider.of(function);
        }
        LookupFunction function =
                equalityKeys.wrap(
                        new FirestoreRowDataLookupFunction(deserializer, maxRetries, lookup));
        return partial
                ? PartialCachingLookupProvider.of(
                        function, equalityKeys.wrap(lookupConfig.createPartialCache()))
                : LookupFunctionProvider.of(function);
    }

    private LookupKeyFilter lookupKeys(int[][] keys) {
        // The top-level column of each produced field: a nested path starts at a ROW column,
        // which is never the document id.
        int[] physical = new int[columns.length];
        for (int i = 0; i < physical.length; i++) {
            physical[i] = columns[i][0];
        }
        String requirement =
                schema.hasPrimaryKey()
                        ? "A Firestore lookup requires an equality predicate on the PRIMARY KEY column '"
                                + schema.getRowType().getFieldNames().get(schema.getKeyIndex())
                                + "', the document id. Additional lookup keys must name top-level physical scalar columns; metadata and nested key paths are unsupported."
                        : "A Firestore lookup requires an equality predicate on the PRIMARY KEY column, the document id. A table without a PRIMARY KEY, as a collection-group table is, cannot be looked up.";
        return LookupKeyFilter.of(
                (RowType) producedDataType.getLogicalType(),
                keys,
                physical,
                schema.hasPrimaryKey() ? new int[] {schema.getKeyIndex()} : new int[0],
                false,
                requirement);
    }

    /**
     * The field mask of the produced fields, each path in the client library's encoded form ({@code
     * FieldPath.toString()}, read back with {@code FieldPath.fromServerFormat}): every field but
     * the document id, and none a document does not declare. An equal path is selected once. A path
     * may extend another only if the planner asked for both, which it does not (it asks for the
     * whole ROW alone); the service accepts such a mask.
     *
     * <p>A projected field of a ROW column is read as its nested path only as deep as every field
     * on the path, itself included, is nullable, and not into a geographical point, which is no
     * map. The service returns a nested path only where each field on it is a map holding the next,
     * so a field that is missing, null, or no map reads alike, as missing; each of those reads as
     * NULL only when every field down to it is nullable. Past a NOT NULL field the mask stops, and
     * the converter reads the rest of the path from the value the mask returned. Below a nullable
     * top-level column no field is NOT NULL when nested projection is on ({@code
     * supportsNestedProjection}), so the stop matters for a NOT NULL column; the rule is kept whole
     * so that the mask stays right by itself.
     *
     * @param schema the checked table schema
     * @param columns the produced physical fields, by index path
     * @return the encoded paths to select, empty when no field is read
     */
    @VisibleForTesting
    static String[] fieldMask(FirestoreTableSchema schema, int[][] columns) {
        // Equal paths collapse here; the produced order is kept.
        Set<String> fields = new LinkedHashSet<>();
        for (int[] column : columns) {
            if (column.length == 1 && column[0] == schema.getKeyIndex()) {
                continue;
            }
            RowType row = schema.getRowType();
            String name = row.getFieldNames().get(column[0]);
            LogicalType type = row.getTypeAt(column[0]);
            List<String> segments = new ArrayList<>(List.of(name));
            String dotted = name;
            for (int i = 1; i < column.length; i++) {
                if (!type.isNullable() || schema.isGeoPoint(dotted)) {
                    break;
                }
                RowType parent = (RowType) type;
                type = parent.getTypeAt(column[i]);
                if (!type.isNullable()) {
                    break;
                }
                name = parent.getFieldNames().get(column[i]);
                segments.add(name);
                dotted = dotted + "." + name;
            }
            fields.add(FieldPath.of(segments.toArray(new String[0])).toString());
        }
        return fields.toArray(new String[0]);
    }

    /** The collection id of a collection path: its last segment. */
    public static String collectionId(String collection) {
        return collection.substring(collection.lastIndexOf('/') + 1);
    }

    @Override
    public boolean supportsNestedProjection() {
        // The planner types a projected nested field as the field is declared, so a NOT NULL
        // field of a nullable ROW would be produced NOT NULL although its ROW may be NULL, and a
        // NULL there fails Flink's NOT NULL enforcer, e.g. for COALESCE(r.a, -1). Such a table
        // reads whole columns.
        RowType row = schema.getRowType();
        for (int i = 0; i < row.getFieldCount(); i++) {
            if (hasNotNullFieldUnderNullableRow(row.getTypeAt(i), false)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether a ROW reachable through ROW fields alone holds a NOT NULL field below a nullable one.
     */
    private static boolean hasNotNullFieldUnderNullableRow(
            LogicalType type, boolean underNullable) {
        if (underNullable && !type.isNullable()) {
            return true;
        }
        if (!(type instanceof RowType)) {
            return false;
        }
        for (LogicalType field : ((RowType) type).getChildren()) {
            if (hasNotNullFieldUnderNullableRow(field, underNullable || type.isNullable())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        // The planner's paths are its own plan state; keep a copy.
        int[][] projected = new int[projectedFields.length][];
        for (int i = 0; i < projected.length; i++) {
            projected[i] = projectedFields[i].clone();
        }
        this.columns = projected;
        this.producedDataType = producedDataType;
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        // A collection-group query on a filtered field needs a collection-group index the service
        // does not create, and PartitionQuery partitions the unfiltered group (docs/adr/0197).
        filterState =
                scanConfig.isCollectionGroup()
                        ? FirestoreFilterPushDown.State.declined(filters)
                        : FirestoreFilterPushDown.translate(schema, filters);
        return filterState.result();
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
        copy.columns = columns;
        copy.metadataKeys = metadataKeys;
        copy.producedDataType = producedDataType;
        copy.filterState = filterState;
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
                && Arrays.deepEquals(columns, that.columns)
                && metadataKeys.equals(that.metadataKeys)
                && producedDataType.equals(that.producedDataType)
                && filterState.equals(that.filterState);
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
                Arrays.deepHashCode(columns),
                metadataKeys,
                producedDataType,
                filterState);
    }
}
