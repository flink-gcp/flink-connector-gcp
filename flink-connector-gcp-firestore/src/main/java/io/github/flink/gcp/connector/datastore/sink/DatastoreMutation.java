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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Key;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/**
 * One entity operation the Datastore sink applies: an upsert, insert or update carrying a whole
 * entity, or a delete carrying a key.
 *
 * <p>The entity is the client library's {@link FullEntity} and the key its {@link Key}, both
 * serializable, so a write can travel wherever the sink's records do — to a dead-letter queue, for
 * one. The key names the project, the database, the namespace and the kind; the sink refuses a
 * write whose key addresses a different project or database than the sink's.
 *
 * <p><b>The key must be complete.</b> Assigning an id is not the sink's to do: a write the sink may
 * apply twice must name its entity the same way both times, and an id the service allocated on the
 * first attempt would be lost to the second. Allocate ids before the sink, or build keys from a
 * name or id the record already carries.
 *
 * <p>How each operation answers a replay is on {@link
 * io.github.flink.gcp.connector.datastore.sink.serializer.DatastoreMutationSerializationSchema}.
 *
 * <p>Instances are immutable.
 */
@PublicEvolving
public final class DatastoreMutation implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The operation a write performs, named as the Datastore API names its mutations. */
    @PublicEvolving
    public enum Operation {
        /** Writes the entity, replacing whatever the key held. */
        UPSERT,
        /** Writes the entity, refused with {@code ALREADY_EXISTS} when the key holds one. */
        INSERT,
        /** Replaces the entity, refused with {@code NOT_FOUND} when the key holds none. */
        UPDATE,
        /** Deletes the entity, succeeding when the key holds none. */
        DELETE
    }

    private final Operation operation;
    private final Key key;
    @Nullable private final FullEntity<Key> entity;

    private DatastoreMutation(Operation operation, Key key, @Nullable FullEntity<Key> entity) {
        this.operation = operation;
        this.key = key;
        this.entity = entity;
    }

    /**
     * Creates a write that stores the entity, replacing whatever its key held.
     *
     * @param entity the entity, with a complete key
     * @return the write
     * @throws IllegalArgumentException if the entity has no key or an incomplete one
     */
    public static DatastoreMutation upsert(FullEntity<Key> entity) {
        return withEntity(Operation.UPSERT, entity);
    }

    /**
     * Creates a write that stores the entity only when its key holds none.
     *
     * @param entity the entity, with a complete key
     * @return the write
     * @throws IllegalArgumentException if the entity has no key or an incomplete one
     */
    public static DatastoreMutation insert(FullEntity<Key> entity) {
        return withEntity(Operation.INSERT, entity);
    }

    /**
     * Creates a write that replaces the entity its key holds, whole: properties the given entity
     * lacks are removed.
     *
     * @param entity the entity, with a complete key
     * @return the write
     * @throws IllegalArgumentException if the entity has no key or an incomplete one
     */
    public static DatastoreMutation update(FullEntity<Key> entity) {
        return withEntity(Operation.UPDATE, entity);
    }

    /**
     * Creates a write that deletes the entity the key names.
     *
     * @param key the key
     * @return the write
     */
    public static DatastoreMutation delete(Key key) {
        return new DatastoreMutation(
                Operation.DELETE, Preconditions.checkNotNull(key, "key must not be null"), null);
    }

    private static DatastoreMutation withEntity(Operation operation, FullEntity<Key> entity) {
        Preconditions.checkNotNull(entity, "entity must not be null");
        // The type parameter is erased, so a raw or unchecked caller can still hand over an
        // incomplete key; that is checked here rather than left to the service, which would
        // allocate an id the sink cannot repeat on a retry.
        IncompleteKey key = entity.getKey();
        Preconditions.checkArgument(
                key instanceof Key,
                "A DatastoreMutation needs an entity with a complete key (a name or an id), was %s."
                        + " Allocate ids before the sink, so that a retried write names the same"
                        + " entity.",
                key);
        return new DatastoreMutation(operation, (Key) key, entity);
    }

    /** Returns the operation. */
    public Operation getOperation() {
        return operation;
    }

    /** Returns the key of the entity the write addresses. */
    public Key getKey() {
        return key;
    }

    /** Returns the entity to write, or {@code null} for a {@link Operation#DELETE}. */
    @Nullable
    public FullEntity<Key> getEntity() {
        return entity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DatastoreMutation that = (DatastoreMutation) o;
        return operation == that.operation
                && key.equals(that.key)
                && Objects.equals(entity, that.entity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operation, key, entity);
    }

    @Override
    public String toString() {
        return "DatastoreMutation{operation="
                + operation
                + ", key="
                + key
                + (entity == null ? "" : ", entity=" + entity)
                + "}";
    }
}
