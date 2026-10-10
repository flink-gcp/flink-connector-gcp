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

package io.github.flink.gcp.connector.bigquery.sink.tables;

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TimePartitioning;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * The parts of a table's storage layout a copy job requires its sources to share: time or integer
 * range partitioning, and clustering.
 *
 * <p>BigQuery refuses to copy a non-partitioned table into a column- or range-partitioned one, and
 * into an existing clustered table unless the cluster specifications match, so the temporary tables
 * a {@code FILE_LOADS} commit copies from are loaded with their destination's layout (ADR-0183).
 * Partition expiration and {@code requirePartitionFilter} are deliberately absent: a copy does not
 * require them to match (measured), and the destination keeps its own.
 */
@Internal
public final class TableLayout {

    /** The layout of a table that is neither partitioned nor clustered. */
    public static final TableLayout NONE = new TableLayout(null, null, null);

    /**
     * The partition expiration of a time-partitioned temporary table: 10,000 years. A partition
     * expires once its age passes this, and BigQuery stores no timestamp before year 1, so no
     * partition is older than about 2,000 years while a commit runs. Without an expiration of its
     * own, a table inherits its dataset's default partition expiration, and would silently drop the
     * rows of older partitions the destination keeps (measured, ADR-0183).
     */
    public static final long TEMPORARY_PARTITION_EXPIRATION_MS =
            10_000L * 365 * 24 * 60 * 60 * 1000;

    @Nullable private final TimePartitioning timePartitioning;
    @Nullable private final RangePartitioning rangePartitioning;
    @Nullable private final Clustering clustering;

    private TableLayout(
            @Nullable TimePartitioning timePartitioning,
            @Nullable RangePartitioning rangePartitioning,
            @Nullable Clustering clustering) {
        this.timePartitioning = timePartitioning;
        this.rangePartitioning = rangePartitioning;
        this.clustering = clustering;
    }

    /**
     * Creates a layout, dropping a time partitioning's expiration and partition filter requirement
     * and treating an empty clustering field list as no clustering.
     *
     * @param timePartitioning the time partitioning, or {@code null}
     * @param rangePartitioning the integer range partitioning, or {@code null}
     * @param clustering the clustering, or {@code null}
     * @return the layout
     */
    public static TableLayout of(
            @Nullable TimePartitioning timePartitioning,
            @Nullable RangePartitioning rangePartitioning,
            @Nullable Clustering clustering) {
        TimePartitioning time = null;
        if (timePartitioning != null) {
            TimePartitioning.Builder builder =
                    TimePartitioning.newBuilder(timePartitioning.getType());
            if (timePartitioning.getField() != null) {
                builder.setField(timePartitioning.getField());
            }
            time = builder.build();
        }
        Clustering clustered =
                clustering == null
                                || clustering.getFields() == null
                                || clustering.getFields().isEmpty()
                        ? null
                        : Clustering.newBuilder().setFields(clustering.getFields()).build();
        if (time == null && rangePartitioning == null && clustered == null) {
            return NONE;
        }
        return new TableLayout(time, rangePartitioning, clustered);
    }

    /**
     * Reads the layout of a table definition; anything but a standard table has none.
     *
     * @param definition the table's definition, or {@code null}
     * @return the layout
     */
    public static TableLayout of(@Nullable TableDefinition definition) {
        if (!(definition instanceof StandardTableDefinition)) {
            return NONE;
        }
        StandardTableDefinition standard = (StandardTableDefinition) definition;
        return of(
                standard.getTimePartitioning(),
                standard.getRangePartitioning(),
                standard.getClustering());
    }

    /**
     * Returns whether a copy job into a table with this layout refuses a source that is neither
     * partitioned nor clustered: the table is partitioned by a column or an integer range, or
     * clustered. Ingestion-time partitioning alone accepts such a source (measured, ADR-0183).
     */
    public boolean requiresLaidOutSources() {
        return rangePartitioning != null
                || clustering != null
                || (timePartitioning != null && timePartitioning.getField() != null);
    }

    /** Returns whether the table is neither partitioned nor clustered. */
    public boolean isEmpty() {
        return timePartitioning == null && rangePartitioning == null && clustering == null;
    }

    /** Returns the time partitioning, type and field only, or {@code null}. */
    @Nullable
    public TimePartitioning getTimePartitioning() {
        return timePartitioning;
    }

    /**
     * Returns the time partitioning a temporary table with this layout takes: the type and field,
     * with {@link #TEMPORARY_PARTITION_EXPIRATION_MS}; or {@code null} without time partitioning.
     */
    @Nullable
    public TimePartitioning getTemporaryTimePartitioning() {
        return timePartitioning == null
                ? null
                : timePartitioning.toBuilder()
                        .setExpirationMs(TEMPORARY_PARTITION_EXPIRATION_MS)
                        .build();
    }

    /** Returns the integer range partitioning, or {@code null}. */
    @Nullable
    public RangePartitioning getRangePartitioning() {
        return rangePartitioning;
    }

    /** Returns the clustering, or {@code null}. */
    @Nullable
    public Clustering getClustering() {
        return clustering;
    }

    /**
     * Returns a stable textual form of the layout, for hashing into temporary-table names and job
     * ids. It names every field the layout carries, so two layouts render equal exactly when they
     * are equal.
     *
     * <p>The form is persistent: a commit restored by a later connector version finds its temporary
     * tables and re-attaches to its jobs through the hash of this text, so changing it strands
     * every laid-out commit in flight across the upgrade.
     */
    public String fingerprint() {
        StringBuilder text = new StringBuilder();
        if (timePartitioning != null) {
            text.append("time=")
                    .append(timePartitioning.getType())
                    .append(':')
                    .append(
                            timePartitioning.getField() == null
                                    ? "_PARTITIONTIME"
                                    : timePartitioning.getField());
        }
        if (rangePartitioning != null) {
            RangePartitioning.Range range = rangePartitioning.getRange();
            text.append(text.length() == 0 ? "" : ";")
                    .append("range=")
                    .append(rangePartitioning.getField())
                    .append(':')
                    .append(range == null ? null : range.getStart())
                    .append(':')
                    .append(range == null ? null : range.getEnd())
                    .append(':')
                    .append(range == null ? null : range.getInterval());
        }
        if (clustering != null) {
            List<String> fields = clustering.getFields();
            text.append(text.length() == 0 ? "" : ";")
                    .append("cluster=")
                    .append(String.join(",", fields));
        }
        return text.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof TableLayout)) {
            return false;
        }
        TableLayout that = (TableLayout) other;
        return Objects.equals(timePartitioning, that.timePartitioning)
                && Objects.equals(rangePartitioning, that.rangePartitioning)
                && Objects.equals(clustering, that.clustering);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timePartitioning, rangePartitioning, clustering);
    }

    @Override
    public String toString() {
        return isEmpty() ? "TableLayout{none}" : "TableLayout{" + fingerprint() + "}";
    }
}
