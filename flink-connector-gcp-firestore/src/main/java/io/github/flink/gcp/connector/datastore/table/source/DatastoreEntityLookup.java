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

package io.github.flink.gcp.connector.datastore.table.source;

import org.apache.flink.annotation.Internal;

import com.google.api.core.ApiFuture;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.LookupResponse;

import java.io.Serializable;

/** Sends the {@code Lookup} of one key, the seam a lookup function reads through. */
@Internal
interface DatastoreEntityLookup extends Serializable, AutoCloseable {

    /** Opens the client, once per lookup function instance. */
    void open() throws Exception;

    /**
     * Sends one {@code Lookup} of this key, never more: the answer may defer the key, and what a
     * deferral does is the caller's. The blocking lookup waits on the same future.
     *
     * @param key a key {@link DatastoreLookupKeys#key(org.apache.flink.table.data.RowData)} built
     * @return the answer's future
     */
    ApiFuture<LookupResponse> lookupAsync(Key key);

    @Override
    void close() throws Exception;
}
