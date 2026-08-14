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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob;

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigquery.JobInfo;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import java.util.List;

/** Everything one terminal BigQuery query job needs, decoupled from the client for testability. */
@Internal
public final class QueryJobSpec {

    private final TableDestination sourceTable;
    private final TableDestination destination;
    private final List<JobInfo.SchemaUpdateOption> schemaUpdateOptions;

    QueryJobSpec(
            TableDestination sourceTable,
            TableDestination destination,
            List<JobInfo.SchemaUpdateOption> schemaUpdateOptions) {
        this.sourceTable = sourceTable;
        this.destination = destination;
        this.schemaUpdateOptions = List.copyOf(schemaUpdateOptions);
    }

    /** Returns the aggregate temporary table the query reads. */
    public TableDestination getSourceTable() {
        return sourceTable;
    }

    /** Returns the final destination table whose data the query replaces. */
    public TableDestination getDestination() {
        return destination;
    }

    /** Returns the explicitly enabled schema update options, possibly empty. */
    public List<JobInfo.SchemaUpdateOption> getSchemaUpdateOptions() {
        return schemaUpdateOptions;
    }

    /** Returns the Standard SQL query that reads every column from the aggregate table. */
    public String getSql() {
        return "SELECT * FROM " + quoteTable(sourceTable);
    }

    private static String quoteTable(TableDestination table) {
        String identifier = table.getProject() + "." + table.getDataset() + "." + table.getTable();
        return "`" + identifier.replace("\\", "\\\\").replace("`", "\\`") + "`";
    }

    @Override
    public String toString() {
        return "QueryJobSpec{sourceTable="
                + sourceTable
                + ", destination="
                + destination
                + ", writeDisposition=WRITE_TRUNCATE_DATA, schemaUpdateOptions="
                + schemaUpdateOptions
                + "}";
    }
}
