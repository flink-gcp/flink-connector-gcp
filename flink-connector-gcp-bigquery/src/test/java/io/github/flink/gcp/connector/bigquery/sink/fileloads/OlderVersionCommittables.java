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

package io.github.flink.gcp.connector.bigquery.sink.fileloads;

import java.util.List;

/**
 * Committables as they are restored from state a version before #2289 wrote, which recorded no
 * temporary dataset. Main code outside this package cannot create one; the tests of other packages
 * reach {@link FileLoadsCommittable#stagedByOlderVersion(String,
 * io.github.flink.gcp.connector.bigquery.sink.TableDestination, String, long, long, StagingFormat,
 * Long, String)} through this fixture alone.
 */
public final class OlderVersionCommittables {

    private OlderVersionCommittables() {}

    /**
     * Returns {@code committable} without its recorded temporary dataset.
     *
     * @param committable the committable a run of this version staged
     * @return the same file as a version before #2289 staged it
     */
    public static FileLoadsCommittable of(FileLoadsCommittable committable) {
        return FileLoadsCommittable.stagedByOlderVersion(
                committable.getFlinkJobId(),
                committable.getDestination(),
                committable.getUri(),
                committable.getByteCount(),
                committable.getRowCount(),
                committable.getFormat(),
                committable.getCheckpointId(),
                committable.getJobProject());
    }

    /**
     * Returns every committable of {@code committables} without its recorded temporary dataset.
     *
     * @param committables the committables a run of this version staged
     * @return the same files as a version before #2289 staged them
     */
    public static List<FileLoadsCommittable> of(List<FileLoadsCommittable> committables) {
        return committables.stream().map(OlderVersionCommittables::of).toList();
    }
}
