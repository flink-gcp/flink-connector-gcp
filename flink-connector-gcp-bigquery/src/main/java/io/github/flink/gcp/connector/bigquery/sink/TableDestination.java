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

package io.github.flink.gcp.connector.bigquery.sink;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.cloud.bigquery.storage.v1.TableName;
import io.github.flink.gcp.connector.base.options.ResourceNames;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

/**
 * A fully-qualified BigQuery table reference: project, dataset and table.
 *
 * <p>Instances are pure table <em>identity</em>: {@link #equals(Object)} and {@link #hashCode()}
 * are defined over exactly (project, dataset, table) so the class can serve as a per-destination
 * key (writer caches, connection routing). Per-destination creation metadata (partitioning,
 * clustering) is intentionally not part of this class — it is supplied through {@link
 * TableCreateOptionsProvider} — keeping destination identity stable.
 *
 * <p>Instances are immutable; the resource path and hash are precomputed, so they are cheap to use
 * as map keys on the per-record write path. Resolvers should still cache and reuse instances
 * instead of re-creating them per record.
 */
@Public
public final class TableDestination extends DestinationResolution implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String project;
    private final String dataset;
    private final String table;
    private final String tablePath;
    private final int hash;

    private TableDestination(String project, String dataset, String table) {
        this.project = project;
        this.dataset = dataset;
        this.table = table;
        this.tablePath = TableName.format(project, dataset, table);
        this.hash = Objects.hash(project, dataset, table);
    }

    /**
     * Creates a {@link TableDestination} from bare ids, not resource paths.
     *
     * @param project the Google Cloud project id
     * @param dataset the BigQuery dataset id
     * @param table the BigQuery table id
     * @return the destination
     * @throws IllegalArgumentException if a component is null or blank, has leading or trailing
     *     whitespace, or contains {@code '/'} — a separator would make the composed resource path
     *     address a different resource
     */
    public static TableDestination of(String project, String dataset, String table) {
        ResourceNames.checkComponent(project, "project");
        ResourceNames.checkComponent(dataset, "dataset");
        ResourceNames.checkComponent(table, "table");
        return new TableDestination(project, dataset, table);
    }

    @Override
    <T> void accept(
            T element,
            SinkWriter.Context context,
            DestinationResolutionDispatcher.Visitor<T> visitor)
            throws IOException {
        visitor.visit(this, element, context);
    }

    /** Returns the Google Cloud project id, given as a bare id rather than a resource path. */
    public String getProject() {
        return project;
    }

    /** Returns the BigQuery dataset id, given as a bare id rather than a resource path. */
    public String getDataset() {
        return dataset;
    }

    /** Returns the BigQuery table id, given as a bare id rather than a resource path. */
    public String getTable() {
        return table;
    }

    /**
     * Returns the table path in the {@code projects/<p>/datasets/<d>/tables/<t>} form used by the
     * BigQuery Storage API.
     */
    public String toTablePath() {
        return tablePath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableDestination that = (TableDestination) o;
        return project.equals(that.project)
                && dataset.equals(that.dataset)
                && table.equals(that.table);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return project + "." + dataset + "." + table;
    }
}
