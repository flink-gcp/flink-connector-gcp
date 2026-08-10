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

package io.github.flink.gcp.connector.bigquery.source;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.source.enumerator.ReadSessionCreator;
import io.github.flink.gcp.connector.bigquery.source.query.QueryRunner;
import io.github.flink.gcp.connector.bigquery.source.reader.RowStreamOpener;
import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializer;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Instant;
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
    private final boolean materializeViews;
    @Nullable private final QueryRunner queryRunner;
    private final String parentProject;
    private final BigQueryRowDeserializer<T> deserializer;
    private final List<String> selectedFields;
    @Nullable private final String rowRestriction;
    @Nullable private final Instant snapshotTime;
    private final int maxStreamCount;
    private final int preferredMinStreamCount;
    private final int maxRecordsPerFetch;
    private final ReadSessionCreator sessionCreator;
    private final RowStreamOpener rowStreamOpener;

    BigQuerySourceConfig(
            @Nullable TableDestination table,
            @Nullable String query,
            @Nullable String queryLocation,
            @Nullable String queryResultDataset,
            boolean materializeViews,
            @Nullable QueryRunner queryRunner,
            String parentProject,
            BigQueryRowDeserializer<T> deserializer,
            List<String> selectedFields,
            @Nullable String rowRestriction,
            @Nullable Instant snapshotTime,
            int maxStreamCount,
            int preferredMinStreamCount,
            int maxRecordsPerFetch,
            ReadSessionCreator sessionCreator,
            RowStreamOpener rowStreamOpener) {
        this.table = table;
        this.query = query;
        this.queryLocation = queryLocation;
        this.queryResultDataset = queryResultDataset;
        this.materializeViews = materializeViews;
        this.queryRunner = queryRunner;
        this.parentProject = parentProject;
        this.deserializer = deserializer;
        this.selectedFields = selectedFields;
        this.rowRestriction = rowRestriction;
        this.snapshotTime = snapshotTime;
        this.maxStreamCount = maxStreamCount;
        this.preferredMinStreamCount = preferredMinStreamCount;
        this.maxRecordsPerFetch = maxRecordsPerFetch;
        this.sessionCreator = sessionCreator;
        this.rowStreamOpener = rowStreamOpener;
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
    public BigQueryRowDeserializer<T> getDeserializer() {
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

    /** Returns the seam creating the read session. */
    public ReadSessionCreator getSessionCreator() {
        return sessionCreator;
    }

    /** Returns the seam opening read streams. */
    public RowStreamOpener getRowStreamOpener() {
        return rowStreamOpener;
    }
}
