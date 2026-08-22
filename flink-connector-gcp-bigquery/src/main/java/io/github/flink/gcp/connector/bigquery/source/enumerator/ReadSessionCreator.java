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

package io.github.flink.gcp.connector.bigquery.source.enumerator;

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadSession;

import java.io.IOException;

/**
 * Creates the source's read session.
 *
 * <p>Abstracts the Storage Read API client so the enumerator's assignment protocol is
 * unit-testable: {@code BigQueryReadClient} is final in effect — it cannot be subclassed usefully —
 * and this repository writes no mocks, so a seam is the only way to script a session.
 *
 * <p><b>Not serializable, deliberately.</b> What travels in the job graph is a {@link
 * ReadSessionCreatorFactory}, and the source mints one creator per enumerator from it. The
 * JobManager holds one source object for a job's whole life, so a creator parked on the source
 * configuration would be shared by every enumerator a coordinator reset builds, and the first
 * teardown would refuse every later one (issue #990, {@code docs/adr/0128}). Implementations still
 * create their client state on first use, not in their constructor, so minting one opens nothing.
 */
@Internal
public interface ReadSessionCreator extends AutoCloseable {

    /**
     * Creates a read session.
     *
     * @param request the session to create
     * @return the created session
     * @throws IOException if the session cannot be created
     */
    ReadSession create(CreateReadSessionRequest request) throws IOException;

    /**
     * Releases whatever client state {@link #create} opened.
     *
     * @throws IOException if the release fails
     */
    @Override
    void close() throws IOException;
}
