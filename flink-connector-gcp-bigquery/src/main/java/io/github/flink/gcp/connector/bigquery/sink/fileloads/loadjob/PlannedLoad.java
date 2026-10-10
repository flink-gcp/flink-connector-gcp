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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob;

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigquery.JobInfo;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.StagingFormat;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableLayout;

import javax.annotation.Nullable;

import java.util.List;

/**
 * One deterministic leaf load: everything the job needs except the reconciled schema, which is the
 * one value that cannot be known before execution. The dispositions differ between a direct load
 * and a temporary-table load, so planning picks them rather than leaving the executor to re-derive
 * which kind of load it is holding; reconciliation switches a laid-out temporary-table load to
 * {@code CREATE_NEVER}, since its table is created first.
 */
@Internal
final class PlannedLoad {

    final TableDestination jobDestination;
    final StagingFormat format;
    final List<String> uris;
    final String jobId;
    final JobInfo.CreateDisposition createDisposition;
    final JobInfo.WriteDisposition writeDisposition;
    final List<JobInfo.SchemaUpdateOption> schemaUpdateOptions;

    /**
     * The layout of the laid-out temporary table this load fills, or {@code null} for a direct load
     * and wherever temporary tables without one are accepted: a destination whose copy accepts
     * them, and {@code WRITE_TRUNCATE_DATA}, which finishes with a query.
     */
    @Nullable final TableLayout tempTableLayout;

    PlannedLoad(
            TableDestination jobDestination,
            StagingFormat format,
            List<String> uris,
            String jobId,
            JobInfo.CreateDisposition createDisposition,
            JobInfo.WriteDisposition writeDisposition,
            List<JobInfo.SchemaUpdateOption> schemaUpdateOptions) {
        this(
                jobDestination,
                format,
                uris,
                jobId,
                createDisposition,
                writeDisposition,
                schemaUpdateOptions,
                null);
    }

    private PlannedLoad(
            TableDestination jobDestination,
            StagingFormat format,
            List<String> uris,
            String jobId,
            JobInfo.CreateDisposition createDisposition,
            JobInfo.WriteDisposition writeDisposition,
            List<JobInfo.SchemaUpdateOption> schemaUpdateOptions,
            @Nullable TableLayout tempTableLayout) {
        this.jobDestination = jobDestination;
        this.format = format;
        this.uris = uris;
        this.jobId = jobId;
        this.createDisposition = createDisposition;
        this.writeDisposition = writeDisposition;
        this.schemaUpdateOptions = schemaUpdateOptions;
        this.tempTableLayout = tempTableLayout;
    }

    /**
     * Returns this temporary-table load into {@code table}, a laid-out temporary table the commit
     * has already created, with the destination's layout, under {@code jobId}. The load does not
     * create the table: one that vanished since fails the attempt, and the next attempt's
     * preparation creates it with its expiration (see {@link
     * DestinationCommitPlan#layOutTempTables(TableLayout, DestinationCommitPlan.TempTables)}).
     */
    PlannedLoad laidOut(TableDestination table, String jobId, TableLayout layout) {
        return new PlannedLoad(
                table,
                format,
                uris,
                jobId,
                JobInfo.CreateDisposition.CREATE_NEVER,
                writeDisposition,
                schemaUpdateOptions,
                layout);
    }
}
