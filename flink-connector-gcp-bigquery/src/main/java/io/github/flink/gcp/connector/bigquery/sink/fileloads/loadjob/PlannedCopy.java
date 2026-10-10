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

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

/** One deterministic intermediate or final copy. */
@Internal
final class PlannedCopy {

    final String jobId;
    final CopyJobSpec spec;

    PlannedCopy(String jobId, CopyJobSpec spec) {
        this.jobId = jobId;
        this.spec = spec;
    }

    /**
     * Returns this copy under {@code jobId}, with every table passed through {@code rename}, which
     * renames a laid-out temporary table and leaves the destination as it is. The copy creates
     * nothing: a laid-out temporary table exists before any job writes it, and the destination is
     * reconciled first. {@code equivalentJobId} replaces the spec's own equivalent id, which for an
     * unchanged final copy names the laid-out id this copy is about to take.
     */
    PlannedCopy laidOut(
            UnaryOperator<TableDestination> rename,
            String jobId,
            @Nullable String equivalentJobId) {
        List<TableDestination> sources = new ArrayList<>(spec.getSourceTables().size());
        for (TableDestination source : spec.getSourceTables()) {
            sources.add(rename.apply(source));
        }
        return new PlannedCopy(
                jobId,
                new CopyJobSpec(
                        sources,
                        rename.apply(spec.getDestination()),
                        JobInfo.CreateDisposition.CREATE_NEVER,
                        spec.getWriteDisposition(),
                        equivalentJobId));
    }
}
