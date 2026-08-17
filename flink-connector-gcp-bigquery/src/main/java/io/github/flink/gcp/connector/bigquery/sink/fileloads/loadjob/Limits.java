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
import org.apache.flink.annotation.VisibleForTesting;

/** BigQuery limits used by the planner; tests shrink them to exercise deep hierarchies. */
@Internal
final class Limits {

    /** BigQuery's maximum source-table count for one copy job. */
    @VisibleForTesting static final int MAX_SOURCE_TABLES_PER_COPY = 1_200;

    /** BigQuery's project-wide daily quota for each of load and copy jobs. */
    @VisibleForTesting static final int MAX_JOBS_PER_COMMIT = 100_000;

    /** BigQuery's maximum pending jobs per project and region. */
    private static final int MAX_SUBMISSIONS_PER_WAVE = 50_000;

    static final Limits BIGQUERY =
            new Limits(
                    MAX_SOURCE_TABLES_PER_COPY,
                    MAX_JOBS_PER_COMMIT,
                    MAX_JOBS_PER_COMMIT,
                    MAX_SUBMISSIONS_PER_WAVE);

    final int maxSourceTablesPerCopy;
    final int maxLoadJobsPerCommit;
    final int maxCopyJobsPerCommit;
    final int maxSubmissionsPerWave;

    Limits(
            int maxSourceTablesPerCopy,
            int maxLoadJobsPerCommit,
            int maxCopyJobsPerCommit,
            int maxSubmissionsPerWave) {
        if (maxSourceTablesPerCopy < 2
                || maxLoadJobsPerCommit < 1
                || maxCopyJobsPerCommit < 1
                || maxSubmissionsPerWave < 1) {
            throw new IllegalArgumentException("Planner limits must be positive and fan-out >= 2");
        }
        this.maxSourceTablesPerCopy = maxSourceTablesPerCopy;
        this.maxLoadJobsPerCommit = maxLoadJobsPerCommit;
        this.maxCopyJobsPerCommit = maxCopyJobsPerCommit;
        this.maxSubmissionsPerWave = maxSubmissionsPerWave;
    }
}
