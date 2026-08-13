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

/** Everything one BigQuery copy job needs, decoupled from the client for testability. */
@Internal
public final class CopyJobSpec {

    private final List<TableDestination> sourceTables;
    private final TableDestination destination;
    private final JobInfo.CreateDisposition createDisposition;
    private final JobInfo.WriteDisposition writeDisposition;

    CopyJobSpec(
            List<TableDestination> sourceTables,
            TableDestination destination,
            JobInfo.CreateDisposition createDisposition,
            JobInfo.WriteDisposition writeDisposition) {
        this.sourceTables = List.copyOf(sourceTables);
        this.destination = destination;
        this.createDisposition = createDisposition;
        this.writeDisposition = writeDisposition;
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
                + "}";
    }
}
