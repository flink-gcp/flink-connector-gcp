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
import com.google.cloud.bigquery.Schema;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.StagingFormat;

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
    private final StagingFormat format;

    LoadJobSpec(
            TableDestination destination,
            List<String> sourceUris,
            Schema schema,
            JobInfo.CreateDisposition createDisposition,
            JobInfo.WriteDisposition writeDisposition,
            List<JobInfo.SchemaUpdateOption> schemaUpdateOptions,
            StagingFormat format) {
        this.destination = destination;
        this.sourceUris = List.copyOf(sourceUris);
        this.schema = schema;
        this.createDisposition = createDisposition;
        this.writeDisposition = writeDisposition;
        this.schemaUpdateOptions = List.copyOf(schemaUpdateOptions);
        this.format = format;
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

    /** Returns the format the staged files were written in, which configures the load job. */
    public StagingFormat getFormat() {
        return format;
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
                + ", format="
                + format
                + "}";
    }
}
