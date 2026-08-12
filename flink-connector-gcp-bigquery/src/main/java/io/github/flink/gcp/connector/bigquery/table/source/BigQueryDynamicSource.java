/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.bigquery.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.source.BigQuerySource;
import io.github.flink.gcp.connector.bigquery.source.BigQuerySourceBuilder;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** A bounded Table API source mapped onto {@link BigQuerySource}. */
@Internal
public final class BigQueryDynamicSource implements ScanTableSource, SupportsProjectionPushDown {

    private final RowType physicalRowType;
    private final DataType physicalDataType;
    @Nullable private final TableDestination table;
    @Nullable private final String query;
    private final String parentProject;
    private final boolean materializeViews;
    @Nullable private final String queryLocation;
    @Nullable private final String queryResultDataset;
    @Nullable private final Duration reuseQueryResultWithin;
    @Nullable private final String rowRestriction;
    @Nullable private final Instant snapshotTime;
    @Nullable private final Integer maxStreamCount;
    @Nullable private final Integer preferredMinStreamCount;
    @Nullable private final Integer maxRecordsPerFetch;
    @Nullable private final Integer retryMaxAttempts;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final String emulatorEndpoint;
    @Nullable private final String emulatorRestEndpoint;
    @Nullable private final Integer parallelism;

    private DataType producedDataType;
    @Nullable private int[] projectedFields;

    /** Creates a source from values already resolved by the table factory. */
    public BigQueryDynamicSource(
            DataType physicalDataType,
            @Nullable TableDestination table,
            @Nullable String query,
            String parentProject,
            boolean materializeViews,
            @Nullable String queryLocation,
            @Nullable String queryResultDataset,
            @Nullable Duration reuseQueryResultWithin,
            @Nullable String rowRestriction,
            @Nullable Instant snapshotTime,
            @Nullable Integer maxStreamCount,
            @Nullable Integer preferredMinStreamCount,
            @Nullable Integer maxRecordsPerFetch,
            @Nullable Integer retryMaxAttempts,
            @Nullable String serviceAccountKeyFile,
            @Nullable String emulatorEndpoint,
            @Nullable String emulatorRestEndpoint,
            @Nullable Integer parallelism) {
        this.physicalDataType =
                Preconditions.checkNotNull(physicalDataType, "physicalDataType must not be null");
        this.producedDataType = physicalDataType;
        this.physicalRowType = (RowType) physicalDataType.getLogicalType();
        Preconditions.checkArgument(
                (table == null) != (query == null), "exactly one of table and query must be set");
        this.table = table;
        this.query = query;
        this.parentProject =
                Preconditions.checkNotNull(parentProject, "parentProject must not be null");
        this.materializeViews = materializeViews;
        this.queryLocation = queryLocation;
        this.queryResultDataset = queryResultDataset;
        this.reuseQueryResultWithin = reuseQueryResultWithin;
        this.rowRestriction = rowRestriction;
        this.snapshotTime = snapshotTime;
        this.maxStreamCount = maxStreamCount;
        this.preferredMinStreamCount = preferredMinStreamCount;
        this.maxRecordsPerFetch = maxRecordsPerFetch;
        this.retryMaxAttempts = retryMaxAttempts;
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.emulatorEndpoint = emulatorEndpoint;
        this.emulatorRestEndpoint = emulatorRestEndpoint;
        this.parallelism = parallelism;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        int[] topLevel = new int[projectedFields.length];
        for (int i = 0; i < projectedFields.length; i++) {
            topLevel[i] = projectedFields[i][0];
        }
        this.projectedFields = topLevel;
        this.producedDataType = producedDataType;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        TypeInformation<RowData> producedType = context.createTypeInformation(producedDataType);
        BigQuerySourceBuilder<RowData> builder =
                BigQuerySource.<RowData>builder()
                        .deserializer(
                                new RowDataDeserializer(
                                        physicalRowType, projectedFields, producedType))
                        .selectedFields(selectedFields());
        if (table != null) {
            builder.table(table);
        } else {
            builder.query(query);
        }
        builder.parentProject(parentProject);
        if (materializeViews) {
            builder.materializeViews();
        }
        if (queryLocation != null) {
            builder.queryLocation(queryLocation);
        }
        if (queryResultDataset != null) {
            builder.queryResultDataset(queryResultDataset);
        }
        if (reuseQueryResultWithin != null) {
            builder.reuseQueryResultWithin(reuseQueryResultWithin);
        }
        if (rowRestriction != null) {
            builder.rowRestriction(rowRestriction);
        }
        if (snapshotTime != null) {
            builder.snapshotTime(snapshotTime);
        }
        if (maxStreamCount != null) {
            builder.maxStreamCount(maxStreamCount);
        }
        if (preferredMinStreamCount != null) {
            builder.preferredMinStreamCount(preferredMinStreamCount);
        }
        if (maxRecordsPerFetch != null) {
            builder.maxRecordsPerFetch(maxRecordsPerFetch);
        }
        if (retryMaxAttempts != null) {
            builder.retryMaxAttempts(retryMaxAttempts);
        }
        if (serviceAccountKeyFile != null) {
            builder.serviceAccountKeyFile(serviceAccountKeyFile);
        }
        if (emulatorEndpoint != null) {
            builder.emulatorEndpoint(emulatorEndpoint);
        }
        if (emulatorRestEndpoint != null) {
            builder.emulatorRestEndpoint(emulatorRestEndpoint);
        }
        Source<RowData, ?, ?> source = builder.build();
        return SourceProvider.of(source, parallelism);
    }

    private List<String> selectedFields() {
        List<String> names = physicalRowType.getFieldNames();
        if (projectedFields == null) {
            return names;
        }
        List<String> selected = new ArrayList<>();
        for (int field : projectedFields) {
            selected.add(names.get(field));
        }
        // An empty selected-fields list means "all fields" to BigQuery. Retain one carrier column
        // if a caller supplies an empty projection directly; Flink's planner normally performs
        // this rewrite itself and discards the carrier above the source.
        if (selected.isEmpty()) {
            selected.add(names.get(0));
        }
        return selected;
    }

    @Override
    public DynamicTableSource copy() {
        BigQueryDynamicSource copy =
                new BigQueryDynamicSource(
                        physicalDataType,
                        table,
                        query,
                        parentProject,
                        materializeViews,
                        queryLocation,
                        queryResultDataset,
                        reuseQueryResultWithin,
                        rowRestriction,
                        snapshotTime,
                        maxStreamCount,
                        preferredMinStreamCount,
                        maxRecordsPerFetch,
                        retryMaxAttempts,
                        serviceAccountKeyFile,
                        emulatorEndpoint,
                        emulatorRestEndpoint,
                        parallelism);
        copy.producedDataType = producedDataType;
        copy.projectedFields = projectedFields == null ? null : projectedFields.clone();
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "BigQuery table source";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BigQueryDynamicSource that = (BigQueryDynamicSource) o;
        return materializeViews == that.materializeViews
                && physicalRowType.equals(that.physicalRowType)
                && Objects.equals(table, that.table)
                && Objects.equals(query, that.query)
                && parentProject.equals(that.parentProject)
                && Objects.equals(queryLocation, that.queryLocation)
                && Objects.equals(queryResultDataset, that.queryResultDataset)
                && Objects.equals(reuseQueryResultWithin, that.reuseQueryResultWithin)
                && Objects.equals(rowRestriction, that.rowRestriction)
                && Objects.equals(snapshotTime, that.snapshotTime)
                && Objects.equals(maxStreamCount, that.maxStreamCount)
                && Objects.equals(preferredMinStreamCount, that.preferredMinStreamCount)
                && Objects.equals(maxRecordsPerFetch, that.maxRecordsPerFetch)
                && Objects.equals(retryMaxAttempts, that.retryMaxAttempts)
                && Objects.equals(serviceAccountKeyFile, that.serviceAccountKeyFile)
                && Objects.equals(emulatorEndpoint, that.emulatorEndpoint)
                && Objects.equals(emulatorRestEndpoint, that.emulatorRestEndpoint)
                && Objects.equals(parallelism, that.parallelism)
                && producedDataType.equals(that.producedDataType)
                && Arrays.equals(projectedFields, that.projectedFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                physicalRowType,
                table,
                query,
                parentProject,
                materializeViews,
                queryLocation,
                queryResultDataset,
                reuseQueryResultWithin,
                rowRestriction,
                snapshotTime,
                maxStreamCount,
                preferredMinStreamCount,
                maxRecordsPerFetch,
                retryMaxAttempts,
                serviceAccountKeyFile,
                emulatorEndpoint,
                emulatorRestEndpoint,
                parallelism,
                producedDataType,
                Arrays.hashCode(projectedFields));
    }
}
