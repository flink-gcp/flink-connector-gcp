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

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.List;

/**
 * The Cloud Storage staging area of the FILE_LOADS write method.
 *
 * <p>Abstracts the GCS client so writer and orchestration logic are unit-testable.
 */
@Internal
public interface StagingStorage extends Serializable {

    /**
     * Opens a new staging object for writing. The object becomes visible atomically when the
     * returned stream is closed; a stream abandoned without closing creates no visible object.
     *
     * @param gcsUri the object URI ({@code gs://bucket/name})
     * @return the stream to write the object contents to
     * @throws IOException if the object cannot be opened
     */
    OutputStream createObject(String gcsUri) throws IOException;

    /**
     * Deletes the given staging objects, best-effort: failures (including already-deleted objects)
     * are logged and swallowed.
     *
     * @param gcsUris the object URIs to delete
     */
    void deleteObjects(List<String> gcsUris);
}
