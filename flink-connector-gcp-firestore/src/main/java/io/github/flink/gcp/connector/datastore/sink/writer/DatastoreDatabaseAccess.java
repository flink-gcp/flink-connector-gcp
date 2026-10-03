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

package io.github.flink.gcp.connector.datastore.sink.writer;

import org.apache.flink.annotation.Internal;

import com.google.cloud.datastore.Key;
import io.github.flink.gcp.connector.datastore.sink.DatastoreMutation;

import java.util.List;

/**
 * The two calls the writer makes against a database in Datastore mode, as a seam a test can fake
 * without the client library's thirty-method interface.
 *
 * <p>Each call is one attempt: the production access opens its client with retries off, because the
 * writer owns every retry. A failure is thrown as whatever the client library throws, and the
 * writer classifies it from its cause chain.
 *
 * <p>Not {@code Serializable}: an access holds an open client, and is opened on the task manager by
 * a {@link DatastoreDatabaseAccessFactory}.
 */
@Internal
public interface DatastoreDatabaseAccess extends AutoCloseable {

    /**
     * Applies the writes in one non-transactional commit. The writes name distinct keys.
     *
     * <p>A commit that fails may have applied some of its writes and not others; the service
     * reports no per-write outcome.
     *
     * @param writes the writes, at least one
     */
    void commit(List<DatastoreMutation> writes);

    /**
     * Looks the key up, returning normally whether or not an entity is there. The writer reads
     * nothing from the answer: that the service answered at all is what it asks, because a missing
     * database refuses the lookup the way it refused the update before it.
     *
     * @param key the key
     */
    void lookup(Key key);

    /**
     * Releases the client. Idempotent.
     *
     * @throws Exception if the client fails to shut down
     */
    @Override
    void close() throws Exception;
}
