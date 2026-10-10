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

package io.github.flink.gcp.connector.datastore.sink.serializer;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.datastore.sink.DatastoreKeyAllocator;

/**
 * A serialization schema that writes entities under keys the service allocates, given the writer's
 * allocator, which reaches the database through the writer's own client, credentials and recovery
 * budget. The writer hands it over once, after {@link #open} and before the first {@link
 * #serialize}.
 *
 * <p>An {@code IOException} the allocator throws must reach the writer, unwrapped or as a cause:
 * the writer fails the job on it, because a refused allocation is the database's failure, not the
 * record's.
 *
 * @param <T> the record type
 */
@Internal
public interface KeyAllocatingSerializationSchema<T>
        extends DatastoreMutationSerializationSchema<T> {

    /**
     * Receives the writer's allocator.
     *
     * @param allocator the allocator
     */
    void setKeyAllocator(DatastoreKeyAllocator allocator);
}
