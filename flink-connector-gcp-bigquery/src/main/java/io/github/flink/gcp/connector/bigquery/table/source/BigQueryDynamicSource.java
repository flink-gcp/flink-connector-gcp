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
import io.github.flink.gcp.connector.bigquery.table.BigQueryConnectorOptions;
import io.github.flink.gcp.connector.bigquery.table.OptionSetters;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A bounded Table API source mapped onto {@link BigQuerySource}.
 *
 * <p>Built through {@link #builder()} rather than a constructor, for the reason its sibling {@link
 * io.github.flink.gcp.connector.bigquery.table.sink.BigQueryDynamicSink the sink} states: the
 * resolved option families and the planner-applied projection would otherwise form a positional
 * list of eighteen mostly nullable values, repeated by construction, {@link #copy()} and the
 * factory's call site — and a transposition among the adjacent same-typed values compiles.
 */
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

    private BigQueryDynamicSource(Builder builder) {
        this.physicalDataType = builder.physicalDataType;
        this.producedDataType =
                builder.producedDataType == null ? physicalDataType : builder.producedDataType;
        this.physicalRowType = (RowType) physicalDataType.getLogicalType();
        this.table = builder.table;
        this.query = builder.query;
        this.parentProject = builder.parentProject;
        this.materializeViews = builder.materializeViews;
        this.queryLocation = builder.queryLocation;
        this.queryResultDataset = builder.queryResultDataset;
        this.reuseQueryResultWithin = builder.reuseQueryResultWithin;
        this.rowRestriction = builder.rowRestriction;
        this.snapshotTime = builder.snapshotTime;
        this.maxStreamCount = builder.maxStreamCount;
        this.preferredMinStreamCount = builder.preferredMinStreamCount;
        this.maxRecordsPerFetch = builder.maxRecordsPerFetch;
        this.retryMaxAttempts = builder.retryMaxAttempts;
        this.serviceAccountKeyFile = builder.serviceAccountKeyFile;
        this.emulatorEndpoint = builder.emulatorEndpoint;
        this.emulatorRestEndpoint = builder.emulatorRestEndpoint;
        this.parallelism = builder.parallelism;
        this.projectedFields = builder.projectedFields;
    }

    /**
     * Returns a builder for a source made of fully resolved values.
     *
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
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
                                new RowDataDeserializationSchema(
                                        physicalRowType, projectedFields, producedType))
                        .selectedFields(selectedFields());
        if (table != null) {
            builder.table(table);
        } else {
            OptionSetters.accept(BigQueryConnectorOptions.SCAN_QUERY.key(), query, builder::query);
        }
        builder.parentProject(parentProject);
        if (materializeViews) {
            builder.materializeViews();
        }
        OptionSetters.accept(
                BigQueryConnectorOptions.SCAN_QUERY_LOCATION.key(),
                queryLocation,
                builder::queryLocation);
        OptionSetters.accept(
                BigQueryConnectorOptions.SCAN_QUERY_RESULT_DATASET.key(),
                queryResultDataset,
                builder::queryResultDataset);
        OptionSetters.accept(
                BigQueryConnectorOptions.SCAN_REUSE_QUERY_RESULT_WITHIN.key(),
                reuseQueryResultWithin,
                builder::reuseQueryResultWithin);
        OptionSetters.accept(
                BigQueryConnectorOptions.SCAN_ROW_RESTRICTION.key(),
                rowRestriction,
                builder::rowRestriction);
        if (snapshotTime != null) {
            builder.snapshotTime(snapshotTime);
        }
        OptionSetters.accept(
                BigQueryConnectorOptions.SCAN_MAX_STREAM_COUNT.key(),
                maxStreamCount,
                builder::maxStreamCount);
        OptionSetters.accept(
                BigQueryConnectorOptions.SCAN_PREFERRED_MIN_STREAM_COUNT.key(),
                preferredMinStreamCount,
                builder::preferredMinStreamCount);
        OptionSetters.accept(
                BigQueryConnectorOptions.SCAN_MAX_RECORDS_PER_FETCH.key(),
                maxRecordsPerFetch,
                builder::maxRecordsPerFetch);
        OptionSetters.accept(
                BigQueryConnectorOptions.SCAN_RETRY_MAX_ATTEMPTS.key(),
                retryMaxAttempts,
                builder::retryMaxAttempts);
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
        return builder()
                .physicalDataType(physicalDataType)
                .table(table)
                .query(query)
                .parentProject(parentProject)
                .materializeViews(materializeViews)
                .queryLocation(queryLocation)
                .queryResultDataset(queryResultDataset)
                .reuseQueryResultWithin(reuseQueryResultWithin)
                .rowRestriction(rowRestriction)
                .snapshotTime(snapshotTime)
                .maxStreamCount(maxStreamCount)
                .preferredMinStreamCount(preferredMinStreamCount)
                .maxRecordsPerFetch(maxRecordsPerFetch)
                .retryMaxAttempts(retryMaxAttempts)
                .serviceAccountKeyFile(serviceAccountKeyFile)
                .emulatorEndpoint(emulatorEndpoint)
                .emulatorRestEndpoint(emulatorRestEndpoint)
                .parallelism(parallelism)
                .producedDataType(producedDataType)
                .projectedFields(projectedFields)
                .build();
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

    /**
     * Collects the source's fully resolved values.
     *
     * <p>Every <em>optional</em> setter takes {@code null} to mean "leave the connector's own
     * default alone", which is exactly what {@code config.getOptional(...).orElse(null)} hands the
     * factory, so an absent DDL option needs no branch on the way here.
     *
     * <p>Three values are not of that kind, and {@link #build()} is where each is checked rather
     * than the setter, so a caller may set them in any order. {@link #physicalDataType(DataType)}
     * and {@link #parentProject(String)} are required. {@link #table(TableDestination)} and {@link
     * #query(String)} are <b>alternatives</b>, not optional values: {@code null} there does not
     * select a default, it says the other one decides what is read, and exactly one of them must.
     */
    @Internal
    public static final class Builder {

        private DataType physicalDataType;
        @Nullable private TableDestination table;
        @Nullable private String query;
        private String parentProject;
        private boolean materializeViews;
        @Nullable private String queryLocation;
        @Nullable private String queryResultDataset;
        @Nullable private Duration reuseQueryResultWithin;
        @Nullable private String rowRestriction;
        @Nullable private Instant snapshotTime;
        @Nullable private Integer maxStreamCount;
        @Nullable private Integer preferredMinStreamCount;
        @Nullable private Integer maxRecordsPerFetch;
        @Nullable private Integer retryMaxAttempts;
        @Nullable private String serviceAccountKeyFile;
        @Nullable private String emulatorEndpoint;
        @Nullable private String emulatorRestEndpoint;
        @Nullable private Integer parallelism;
        @Nullable private DataType producedDataType;
        @Nullable private int[] projectedFields;

        private Builder() {}

        /**
         * Sets the physical columns of the table. Required.
         *
         * @param physicalDataType the physical row type
         * @return this builder
         */
        public Builder physicalDataType(DataType physicalDataType) {
            this.physicalDataType = physicalDataType;
            return this;
        }

        /**
         * Sets the table being read, or {@code null} when a query decides it.
         *
         * @param table the table, or {@code null}
         * @return this builder
         */
        public Builder table(@Nullable TableDestination table) {
            this.table = table;
            return this;
        }

        /**
         * Sets the query whose result is read, or {@code null} when a table is named directly.
         *
         * @param query the query, or {@code null}
         * @return this builder
         */
        public Builder query(@Nullable String query) {
            this.query = query;
            return this;
        }

        /**
         * Sets the project the query job and the read session are billed to. Required.
         *
         * @param parentProject the project
         * @return this builder
         */
        public Builder parentProject(String parentProject) {
            this.parentProject = parentProject;
            return this;
        }

        /** Sets whether a name that turns out to be a view is read by materializing it. */
        public Builder materializeViews(boolean materializeViews) {
            this.materializeViews = materializeViews;
            return this;
        }

        /**
         * Sets the location the query job runs in, or {@code null} to let BigQuery infer it.
         *
         * @param queryLocation the location, or {@code null}
         * @return this builder
         */
        public Builder queryLocation(@Nullable String queryLocation) {
            this.queryLocation = queryLocation;
            return this;
        }

        /**
         * Sets the dataset the query's result is written to, or {@code null} for BigQuery's own
         * anonymous dataset.
         *
         * @param queryResultDataset the dataset, or {@code null}
         * @return this builder
         */
        public Builder queryResultDataset(@Nullable String queryResultDataset) {
            this.queryResultDataset = queryResultDataset;
            return this;
        }

        /**
         * Sets how long a re-planned job may reuse a previous attempt's query job, or {@code null}
         * to reuse nothing.
         *
         * @param reuseQueryResultWithin the window, or {@code null}
         * @return this builder
         */
        public Builder reuseQueryResultWithin(@Nullable Duration reuseQueryResultWithin) {
            this.reuseQueryResultWithin = reuseQueryResultWithin;
            return this;
        }

        /**
         * Sets the server-side row filter, or {@code null} for no filter.
         *
         * @param rowRestriction the filter, or {@code null}
         * @return this builder
         */
        public Builder rowRestriction(@Nullable String rowRestriction) {
            this.rowRestriction = rowRestriction;
            return this;
        }

        /**
         * Sets the instant the table is read as of, or {@code null} for its current contents.
         *
         * @param snapshotTime the instant, or {@code null}
         * @return this builder
         */
        public Builder snapshotTime(@Nullable Instant snapshotTime) {
            this.snapshotTime = snapshotTime;
            return this;
        }

        /**
         * Sets the upper bound on read streams, or {@code null} to leave it at the connector's
         * default.
         *
         * @param maxStreamCount the bound, or {@code null}
         * @return this builder
         */
        public Builder maxStreamCount(@Nullable Integer maxStreamCount) {
            this.maxStreamCount = maxStreamCount;
            return this;
        }

        /**
         * Sets the preferred lower bound on read streams, or {@code null} to leave it at the
         * connector's default.
         *
         * @param preferredMinStreamCount the bound, or {@code null}
         * @return this builder
         */
        public Builder preferredMinStreamCount(@Nullable Integer preferredMinStreamCount) {
            this.preferredMinStreamCount = preferredMinStreamCount;
            return this;
        }

        /**
         * Sets the most rows one fetch hands to the task thread, or {@code null} to leave it at the
         * connector's default.
         *
         * @param maxRecordsPerFetch the bound, or {@code null}
         * @return this builder
         */
        public Builder maxRecordsPerFetch(@Nullable Integer maxRecordsPerFetch) {
            this.maxRecordsPerFetch = maxRecordsPerFetch;
            return this;
        }

        /**
         * Sets how many attempts a read stream makes, or {@code null} to leave it at the
         * connector's default.
         *
         * @param retryMaxAttempts the attempts, or {@code null}
         * @return this builder
         */
        public Builder retryMaxAttempts(@Nullable Integer retryMaxAttempts) {
            this.retryMaxAttempts = retryMaxAttempts;
            return this;
        }

        /** Sets the runtime service-account key-file path, or {@code null} for ADC. */
        public Builder serviceAccountKeyFile(@Nullable String serviceAccountKeyFile) {
            this.serviceAccountKeyFile = serviceAccountKeyFile;
            return this;
        }

        /**
         * Sets the emulator's gRPC endpoint, or {@code null} for the real service.
         *
         * @param emulatorEndpoint the endpoint, or {@code null}
         * @return this builder
         */
        public Builder emulatorEndpoint(@Nullable String emulatorEndpoint) {
            this.emulatorEndpoint = emulatorEndpoint;
            return this;
        }

        /**
         * Sets the emulator's REST endpoint, or {@code null} for the real service.
         *
         * @param emulatorRestEndpoint the endpoint, or {@code null}
         * @return this builder
         */
        public Builder emulatorRestEndpoint(@Nullable String emulatorRestEndpoint) {
            this.emulatorRestEndpoint = emulatorRestEndpoint;
            return this;
        }

        /**
         * Sets the source parallelism, or {@code null} for the planner's own.
         *
         * @param parallelism the parallelism, or {@code null}
         * @return this builder
         */
        public Builder parallelism(@Nullable Integer parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        /**
         * Restores the produced type when a source is copied, or {@code null} to derive it from the
         * physical one as an uncopied source does.
         */
        Builder producedDataType(@Nullable DataType producedDataType) {
            this.producedDataType = producedDataType;
            return this;
        }

        /** Restores the applied projection when a source is copied. */
        Builder projectedFields(@Nullable int[] projectedFields) {
            this.projectedFields = projectedFields == null ? null : projectedFields.clone();
            return this;
        }

        /**
         * Builds the source.
         *
         * @return the source
         */
        public BigQueryDynamicSource build() {
            Preconditions.checkNotNull(physicalDataType, "physicalDataType must not be null");
            Preconditions.checkArgument(
                    (table == null) != (query == null),
                    "exactly one of table and query must be set");
            Preconditions.checkNotNull(parentProject, "parentProject must not be null");
            return new BigQueryDynamicSource(this);
        }
    }
}
