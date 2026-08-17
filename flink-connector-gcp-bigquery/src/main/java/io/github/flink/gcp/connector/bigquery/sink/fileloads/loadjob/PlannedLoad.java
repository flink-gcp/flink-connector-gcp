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

import java.util.List;

/**
 * One deterministic leaf load: everything the job needs except the reconciled schema, which is the
 * one value that cannot be known before execution. The dispositions differ between a direct load
 * and a temporary-table load, so planning picks them rather than leaving the executor to re-derive
 * which kind of load it is holding.
 */
@Internal
final class PlannedLoad {

    final TableDestination finalDestination;
    final TableDestination jobDestination;
    final StagingFormat format;
    final List<String> uris;
    final String jobId;
    final JobInfo.CreateDisposition createDisposition;
    final JobInfo.WriteDisposition writeDisposition;
    final List<JobInfo.SchemaUpdateOption> schemaUpdateOptions;

    PlannedLoad(
            TableDestination finalDestination,
            TableDestination jobDestination,
            StagingFormat format,
            List<String> uris,
            String jobId,
            JobInfo.CreateDisposition createDisposition,
            JobInfo.WriteDisposition writeDisposition,
            List<JobInfo.SchemaUpdateOption> schemaUpdateOptions) {
        this.finalDestination = finalDestination;
        this.jobDestination = jobDestination;
        this.format = format;
        this.uris = uris;
        this.jobId = jobId;
        this.createDisposition = createDisposition;
        this.writeDisposition = writeDisposition;
        this.schemaUpdateOptions = schemaUpdateOptions;
    }
}
