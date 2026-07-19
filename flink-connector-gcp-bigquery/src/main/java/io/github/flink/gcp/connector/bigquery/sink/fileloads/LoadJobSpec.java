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

package io.github.flink.gcp.connector.bigquery.sink.fileloads;

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TimePartitioning;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import javax.annotation.Nullable;

import java.util.List;

/** Everything one BigQuery load job needs, decoupled from the client for testability. */
@Internal
public final class LoadJobSpec {

    private final TableDestination destination;
    private final List<String> sourceUris;
    private final Schema schema;
    private final JobInfo.CreateDisposition createDisposition;
    private final JobInfo.WriteDisposition writeDisposition;
    private final List<JobInfo.SchemaUpdateOption> schemaUpdateOptions;
    @Nullable private final TimePartitioning timePartitioning;
    @Nullable private final Clustering clustering;

    LoadJobSpec(
            TableDestination destination,
            List<String> sourceUris,
            Schema schema,
            JobInfo.CreateDisposition createDisposition,
            JobInfo.WriteDisposition writeDisposition,
            List<JobInfo.SchemaUpdateOption> schemaUpdateOptions,
            @Nullable TimePartitioning timePartitioning,
            @Nullable Clustering clustering) {
        this.destination = destination;
        this.sourceUris = List.copyOf(sourceUris);
        this.schema = schema;
        this.createDisposition = createDisposition;
        this.writeDisposition = writeDisposition;
        this.schemaUpdateOptions = List.copyOf(schemaUpdateOptions);
        this.timePartitioning = timePartitioning;
        this.clustering = clustering;
    }

    /** Returns the destination table. */
    public TableDestination getDestination() {
        return destination;
    }

    /** Returns the staging object URIs to load. */
    public List<String> getSourceUris() {
        return sourceUris;
    }

    /** Returns the explicit destination schema. */
    public Schema getSchema() {
        return schema;
    }

    /** Returns the create disposition. */
    public JobInfo.CreateDisposition getCreateDisposition() {
        return createDisposition;
    }

    /** Returns the write disposition. */
    public JobInfo.WriteDisposition getWriteDisposition() {
        return writeDisposition;
    }

    /** Returns the schema update options, possibly empty. */
    public List<JobInfo.SchemaUpdateOption> getSchemaUpdateOptions() {
        return schemaUpdateOptions;
    }

    /** Returns the partitioning of an auto-created destination table, or {@code null}. */
    @Nullable
    public TimePartitioning getTimePartitioning() {
        return timePartitioning;
    }

    /** Returns the clustering of an auto-created destination table, or {@code null}. */
    @Nullable
    public Clustering getClustering() {
        return clustering;
    }

    @Override
    public String toString() {
        return "LoadJobSpec{destination="
                + destination
                + ", sourceUris="
                + sourceUris.size()
                + " files, createDisposition="
                + createDisposition
                + ", writeDisposition="
                + writeDisposition
                + ", schemaUpdateOptions="
                + schemaUpdateOptions
                + "}";
    }
}
