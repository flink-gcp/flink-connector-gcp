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

package io.github.flink.gcp.connector.bigquery.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.io.Serializable;
import java.util.Objects;

/** A fully-qualified BigQuery table reference: project, dataset and table. */
@PublicEvolving
public final class TableDestination implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String project;
    private final String dataset;
    private final String table;

    private TableDestination(String project, String dataset, String table) {
        this.project = project;
        this.dataset = dataset;
        this.table = table;
    }

    /**
     * Creates a {@link TableDestination}.
     *
     * @param project the Google Cloud project id
     * @param dataset the BigQuery dataset id
     * @param table the BigQuery table id
     * @return the destination
     */
    public static TableDestination of(String project, String dataset, String table) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(project), "project must not be blank");
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(dataset), "dataset must not be blank");
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(table), "table must not be blank");
        return new TableDestination(project, dataset, table);
    }

    /** Returns the Google Cloud project id. */
    public String getProject() {
        return project;
    }

    /** Returns the BigQuery dataset id. */
    public String getDataset() {
        return dataset;
    }

    /** Returns the BigQuery table id. */
    public String getTable() {
        return table;
    }

    /**
     * Returns the table path in the {@code projects/<p>/datasets/<d>/tables/<t>} form used by the
     * BigQuery Storage API.
     */
    public String toTablePath() {
        return "projects/" + project + "/datasets/" + dataset + "/tables/" + table;
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
        return Objects.hash(project, dataset, table);
    }

    @Override
    public String toString() {
        return project + "." + dataset + "." + table;
    }
}
