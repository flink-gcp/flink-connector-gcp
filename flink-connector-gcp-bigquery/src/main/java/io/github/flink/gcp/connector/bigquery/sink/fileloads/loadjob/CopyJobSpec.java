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

import java.util.List;

/** Everything one BigQuery copy job needs, decoupled from the client for testability. */
@Internal
public final class CopyJobSpec {

    private final List<TableDestination> sourceTables;
    private final TableDestination destination;
    private final JobInfo.CreateDisposition createDisposition;
    private final JobInfo.WriteDisposition writeDisposition;
    @Nullable private final String equivalentJobId;

    CopyJobSpec(
            List<TableDestination> sourceTables,
            TableDestination destination,
            JobInfo.CreateDisposition createDisposition,
            JobInfo.WriteDisposition writeDisposition) {
        this(sourceTables, destination, createDisposition, writeDisposition, null);
    }

    CopyJobSpec(
            List<TableDestination> sourceTables,
            TableDestination destination,
            JobInfo.CreateDisposition createDisposition,
            JobInfo.WriteDisposition writeDisposition,
            @Nullable String equivalentJobId) {
        this.sourceTables = List.copyOf(sourceTables);
        this.destination = destination;
        this.createDisposition = createDisposition;
        this.writeDisposition = writeDisposition;
        this.equivalentJobId = equivalentJobId;
    }

    /** Returns the temporary tables to copy from. */
    public List<TableDestination> getSourceTables() {
        return sourceTables;
    }

    /** Returns the destination table. */
    public TableDestination getDestination() {
        return destination;
    }

    /** Returns the create disposition. */
    public JobInfo.CreateDisposition getCreateDisposition() {
        return createDisposition;
    }

    /** Returns the write disposition. */
    public JobInfo.WriteDisposition getWriteDisposition() {
        return writeDisposition;
    }

    /**
     * Returns the base id under which an earlier attempt may already have run this same copy, or
     * {@code null}. A job under that id, or one of its retry ids, that has not failed stands for
     * this copy, so the runner re-attaches to it instead of copying the rows again.
     *
     * <p>Set on the copy into the destination of a commit through temporary tables, whose id
     * depends on whether the destination's layout made the commit lay its temporary tables out
     * (ADR-0183); a change of the destination's clustering between attempts can switch it.
     */
    @Nullable
    public String getEquivalentJobId() {
        return equivalentJobId;
    }

    @Override
    public String toString() {
        return "CopyJobSpec{sourceTables="
                + sourceTables
                + ", destination="
                + destination
                + ", createDisposition="
                + createDisposition
                + ", writeDisposition="
                + writeDisposition
                + (equivalentJobId != null ? ", equivalentJobId=" + equivalentJobId : "")
                + "}";
    }
}
