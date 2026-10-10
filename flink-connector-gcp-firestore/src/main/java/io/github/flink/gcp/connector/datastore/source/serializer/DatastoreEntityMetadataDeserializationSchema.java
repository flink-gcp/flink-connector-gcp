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

package io.github.flink.gcp.connector.datastore.source.serializer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Collector;

import com.google.cloud.datastore.Entity;

import java.io.IOException;

/**
 * A deserialization schema that also reads what the query result carries about the entity: its
 * version, its create and update times, and the split's read time. The source's reader hands these
 * to a schema of this type in place of {@link #deserialize(Entity, Collector)}; the {@code
 * datastore} table scan reads its metadata columns through it.
 *
 * @param <T> the record type
 */
@Internal
public interface DatastoreEntityMetadataDeserializationSchema<T>
        extends DatastoreEntityDeserializationSchema<T> {

    /**
     * Deserializes one entity, with what its query result carries.
     *
     * @param entity the entity
     * @param metadata the entity's version and times
     * @param out the collector
     * @throws IOException if the entity cannot be deserialized, failing the job
     */
    void deserialize(Entity entity, EntityMetadata metadata, Collector<T> out) throws IOException;

    /**
     * Refuses an entity without its metadata: the source's reader always hands both.
     *
     * @throws IOException always
     */
    @Override
    default void deserialize(Entity entity, Collector<T> out) throws IOException {
        throw new IOException(
                "This deserialization schema reads an entity with its query result's metadata,"
                        + " which only the Datastore source's reader supplies.");
    }
}
