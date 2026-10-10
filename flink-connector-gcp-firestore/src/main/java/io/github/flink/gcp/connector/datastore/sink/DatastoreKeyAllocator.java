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

package io.github.flink.gcp.connector.datastore.sink;

import org.apache.flink.annotation.Internal;

import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Key;

import java.io.IOException;
import java.util.List;

/**
 * Allocates complete keys from the service ({@code AllocateIds}): ids the allocator never hands out
 * again, so an entity written under one cannot replace an entity written under another.
 */
@Internal
@FunctionalInterface
public interface DatastoreKeyAllocator {

    /**
     * Allocates a batch of complete keys, each {@code key} with an id the service chose; the
     * writer's {@code idAllocationBatchSize} sets how many.
     *
     * @param key the incomplete key: project, database, namespace and kind
     * @return the keys, at least one
     * @throws IOException if the service refuses the allocation or keeps failing it
     */
    List<Key> allocate(IncompleteKey key) throws IOException;
}
