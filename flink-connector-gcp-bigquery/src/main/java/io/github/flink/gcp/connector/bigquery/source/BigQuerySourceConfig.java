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

package io.github.flink.gcp.connector.bigquery.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.source.enumerator.ReadSessionCreatorFactory;
import io.github.flink.gcp.connector.bigquery.source.query.QueryRunner;
import io.github.flink.gcp.connector.bigquery.source.reader.RowStreamOpener;
import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializationSchema;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

/**
 * Everything the source's enumerator and readers were configured with, as one immutable object
 * shipped inside the job graph.
 *
 * @param <T> type of the records produced by the source
 */
@Internal
public final class BigQuerySourceConfig<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    @Nullable private final TableDestination table;
    @Nullable private final String query;
    @Nullable private final String queryLocation;
    @Nullable private final String queryResultDataset;
    @Nullable private final Duration reuseQueryResultWithin;
    private final boolean materializeViews;
    @Nullable private final QueryRunner queryRunner;
    private final String parentProject;
    private final BigQueryRowDeserializationSchema<T> deserializer;
    private final List<String> selectedFields;
    @Nullable private final String rowRestriction;
    @Nullable private final Instant snapshotTime;
    private final int maxStreamCount;
    private final int preferredMinStreamCount;
    private final int maxRecordsPerFetch;
    private final long maxBytesPerFetch;
    private final ReadSessionCreatorFactory sessionCreatorFactory;
    private final RowStreamOpener rowStreamOpener;

    private BigQuerySourceConfig(Builder<T> builder) {
        this.table = builder.table;
        this.query = builder.query;
        this.queryLocation = builder.queryLocation;
        this.queryResultDataset = builder.queryResultDataset;
        this.reuseQueryResultWithin = builder.reuseQueryResultWithin;
        this.materializeViews = builder.materializeViews;
        this.queryRunner = builder.queryRunner;
        this.parentProject = builder.parentProject;
        this.deserializer = builder.deserializer;
        this.selectedFields = builder.selectedFields;
        this.rowRestriction = builder.rowRestriction;
        this.snapshotTime = builder.snapshotTime;
        this.maxStreamCount = builder.maxStreamCount;
        this.preferredMinStreamCount = builder.preferredMinStreamCount;
        this.maxRecordsPerFetch = builder.maxRecordsPerFetch;
        this.maxBytesPerFetch = builder.maxBytesPerFetch;
        this.sessionCreatorFactory = builder.sessionCreatorFactory;
        this.rowStreamOpener = builder.rowStreamOpener;
    }

    /**
     * Returns a builder for a configuration made of values {@link BigQuerySourceBuilder} has
     * already validated.
     *
     * @param <T> type of the records produced by the source
     * @return the builder
     */
    static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /**
     * Returns the table being read, or {@code null} when a query decides it.
     *
     * <p>Exactly one of this and {@link #getQuery()} is set; which one is what the builder
     * enforced.
     */
    @Nullable
    public TableDestination getTable() {
        return table;
    }

    /** Returns the query whose result is read, or {@code null} when a table is named directly. */
    @Nullable
    public String getQuery() {
        return query;
    }

    /** Returns the location the query job runs in, or {@code null} to let BigQuery infer it. */
    @Nullable
    public String getQueryLocation() {
        return queryLocation;
    }

    /**
     * Returns the dataset the query's result is written to, or {@code null} for BigQuery's own
     * anonymous dataset.
     */
    @Nullable
    public String getQueryResultDataset() {
        return queryResultDataset;
    }

    /**
     * Returns how long a re-planned job may reuse a previous attempt's query job, or {@code null}
     * for the default: a random job id, under which nothing is ever reused.
     */
    @Nullable
    public Duration getReuseQueryResultWithin() {
        return reuseQueryResultWithin;
    }

    /**
     * Returns whether {@code materializeViews()} was asked for.
     *
     * <p>Named {@code ...Enabled} rather than after the field alone, which every other boolean
     * getter here is: {@code isMaterializeViews()} reads as a question about a <em>materialized
     * view</em> — a real BigQuery noun, and one this very class decides about — rather than as "the
     * option is on". A reader misread it that way, which is the whole argument.
     *
     * <p>Off unless asked for, because deciding it costs a metadata call the read path otherwise
     * never makes.
     */
    public boolean isMaterializeViewsEnabled() {
        return materializeViews;
    }

    /** Returns the seam running the query, or {@code null} when a table is named directly. */
    @Nullable
    public QueryRunner getQueryRunner() {
        return queryRunner;
    }

    /**
     * Returns what this source reads, as it reads mid-sentence, for the enumerator's log lines and
     * failure messages.
     *
     * <p>The query is not quoted in full: it can be arbitrarily long, and a message that buries its
     * own point under a page of SQL is worse than one that names the job to look up.
     */
    public String describeInput() {
        return table != null ? "table " + table : "the result of the configured query";
    }

    /** Returns the project the read session belongs to and is billed to. */
    public String getParentProject() {
        return parentProject;
    }

    /** Returns the deserializer converting rows into records. */
    public BigQueryRowDeserializationSchema<T> getDeserializer() {
        return deserializer;
    }

    /** Returns the columns read, or an empty list for every column. */
    public List<String> getSelectedFields() {
        return selectedFields;
    }

    /** Returns the server-side row filter, or {@code null} for no filter. */
    @Nullable
    public String getRowRestriction() {
        return rowRestriction;
    }

    /** Returns the instant the table is read as of, or {@code null} for its current contents. */
    @Nullable
    public Instant getSnapshotTime() {
        return snapshotTime;
    }

    /** Returns the upper bound on read streams, or {@code 0} to let BigQuery decide. */
    public int getMaxStreamCount() {
        return maxStreamCount;
    }

    /** Returns the preferred lower bound on read streams, or {@code 0} for none. */
    public int getPreferredMinStreamCount() {
        return preferredMinStreamCount;
    }

    /** Returns the most rows one fetch hands to the task thread. */
    public int getMaxRecordsPerFetch() {
        return maxRecordsPerFetch;
    }

    /** Returns the target serialized Avro bytes one fetch hands to the task thread. */
    public long getMaxBytesPerFetch() {
        return maxBytesPerFetch;
    }

    /**
     * Returns the factory the source mints one session creator per enumerator from.
     *
     * <p>A factory rather than a creator because the JobManager holds one source object for a job's
     * whole life, so a creator here would be shared by every enumerator a coordinator reset builds
     * and the first teardown would refuse every later one ({@code docs/adr/0128}).
     */
    public ReadSessionCreatorFactory getSessionCreatorFactory() {
        return sessionCreatorFactory;
    }

    /** Returns the seam opening read streams. */
    public RowStreamOpener getRowStreamOpener() {
        return rowStreamOpener;
    }

    /**
     * Collects what {@link BigQuerySourceBuilder#build()} resolved, so that its values reach the
     * configuration by name rather than by position.
     *
     * <p>Validation stays where the message belongs, in {@code BigQuerySourceBuilder}: every check
     * there names a <em>user-facing</em> setter, and restating one here would either duplicate that
     * message or invent a second wording for the same rule. What {@link #build()} does check is the
     * thing a positional list could not get wrong and a builder can — a value nobody set at all.
     *
     * @param <T> type of the records produced by the source
     */
    static final class Builder<T> {

        @Nullable private TableDestination table;
        @Nullable private String query;
        @Nullable private String queryLocation;
        @Nullable private String queryResultDataset;
        @Nullable private Duration reuseQueryResultWithin;
        private boolean materializeViews;
        @Nullable private QueryRunner queryRunner;
        private String parentProject;
        private BigQueryRowDeserializationSchema<T> deserializer;
        private List<String> selectedFields = Collections.emptyList();
        @Nullable private String rowRestriction;
        @Nullable private Instant snapshotTime;
        private int maxStreamCount;
        private int preferredMinStreamCount;
        private int maxRecordsPerFetch;
        private long maxBytesPerFetch;
        private ReadSessionCreatorFactory sessionCreatorFactory;
        private RowStreamOpener rowStreamOpener;

        private Builder() {}

        /** Sets the table being read, or {@code null} when a query decides it. */
        Builder<T> table(@Nullable TableDestination table) {
            this.table = table;
            return this;
        }

        /** Sets the query whose result is read, or {@code null} when a table is named directly. */
        Builder<T> query(@Nullable String query) {
            this.query = query;
            return this;
        }

        /** Sets the location the query job runs in, or {@code null} to let BigQuery infer it. */
        Builder<T> queryLocation(@Nullable String queryLocation) {
            this.queryLocation = queryLocation;
            return this;
        }

        /** Sets the dataset the query's result is written to, or {@code null} for the anonymous. */
        Builder<T> queryResultDataset(@Nullable String queryResultDataset) {
            this.queryResultDataset = queryResultDataset;
            return this;
        }

        /** Sets the window a previous attempt's query job may be reused in, or {@code null}. */
        Builder<T> reuseQueryResultWithin(@Nullable Duration reuseQueryResultWithin) {
            this.reuseQueryResultWithin = reuseQueryResultWithin;
            return this;
        }

        /** Sets whether a name that turns out to be a view is read by materializing it. */
        Builder<T> materializeViews(boolean materializeViews) {
            this.materializeViews = materializeViews;
            return this;
        }

        /** Sets the seam running the query, or {@code null} when no query job runs. */
        Builder<T> queryRunner(@Nullable QueryRunner queryRunner) {
            this.queryRunner = queryRunner;
            return this;
        }

        /** Sets the project the read session belongs to and is billed to. */
        Builder<T> parentProject(String parentProject) {
            this.parentProject = parentProject;
            return this;
        }

        /** Sets the deserializer converting rows into records. */
        Builder<T> deserializer(BigQueryRowDeserializationSchema<T> deserializer) {
            this.deserializer = deserializer;
            return this;
        }

        /** Sets the columns read, or an empty list for every column. */
        Builder<T> selectedFields(List<String> selectedFields) {
            this.selectedFields = selectedFields;
            return this;
        }

        /** Sets the server-side row filter, or {@code null} for no filter. */
        Builder<T> rowRestriction(@Nullable String rowRestriction) {
            this.rowRestriction = rowRestriction;
            return this;
        }

        /** Sets the instant the table is read as of, or {@code null} for its current contents. */
        Builder<T> snapshotTime(@Nullable Instant snapshotTime) {
            this.snapshotTime = snapshotTime;
            return this;
        }

        /** Sets the upper bound on read streams, or {@code 0} to let BigQuery decide. */
        Builder<T> maxStreamCount(int maxStreamCount) {
            this.maxStreamCount = maxStreamCount;
            return this;
        }

        /** Sets the preferred lower bound on read streams, or {@code 0} for none. */
        Builder<T> preferredMinStreamCount(int preferredMinStreamCount) {
            this.preferredMinStreamCount = preferredMinStreamCount;
            return this;
        }

        /** Sets the most rows one fetch hands to the task thread. */
        Builder<T> maxRecordsPerFetch(int maxRecordsPerFetch) {
            this.maxRecordsPerFetch = maxRecordsPerFetch;
            return this;
        }

        /** Sets the target serialized Avro bytes one fetch hands to the task thread. */
        Builder<T> maxBytesPerFetch(long maxBytesPerFetch) {
            this.maxBytesPerFetch = maxBytesPerFetch;
            return this;
        }

        /** Sets the factory minting the seam that creates the read session. */
        Builder<T> sessionCreatorFactory(ReadSessionCreatorFactory sessionCreatorFactory) {
            this.sessionCreatorFactory = sessionCreatorFactory;
            return this;
        }

        /** Sets the seam opening read streams. */
        Builder<T> rowStreamOpener(RowStreamOpener rowStreamOpener) {
            this.rowStreamOpener = rowStreamOpener;
            return this;
        }

        /**
         * Builds the configuration.
         *
         * @return the configuration
         */
        BigQuerySourceConfig<T> build() {
            Preconditions.checkNotNull(parentProject, "parentProject must not be null");
            Preconditions.checkNotNull(deserializer, "deserializer must not be null");
            Preconditions.checkNotNull(selectedFields, "selectedFields must not be null");
            Preconditions.checkNotNull(
                    sessionCreatorFactory, "sessionCreatorFactory must not be null");
            Preconditions.checkNotNull(rowStreamOpener, "rowStreamOpener must not be null");
            // Zero is not a "let the service decide" value here, as it is for the two stream
            // counts: a reader fetching no rows never finishes a split. Only an unset value can
            // produce it, since the source builder's own setter refuses anything below one.
            Preconditions.checkArgument(
                    maxRecordsPerFetch > 0,
                    "maxRecordsPerFetch must be positive: %s",
                    maxRecordsPerFetch);
            Preconditions.checkArgument(
                    maxBytesPerFetch > 0,
                    "maxBytesPerFetch must be positive: %s",
                    maxBytesPerFetch);
            return new BigQuerySourceConfig<>(this);
        }
    }
}
