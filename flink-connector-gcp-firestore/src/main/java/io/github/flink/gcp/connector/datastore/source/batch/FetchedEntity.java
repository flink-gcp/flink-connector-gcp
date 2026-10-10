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

package io.github.flink.gcp.connector.datastore.source.batch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.datastore.Entity;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.datastore.source.serializer.EntityMetadata;

/**
 * An entity the split reader handed to the task thread, with the cursor the service returned after
 * it and what the query result carried beside it.
 *
 * <p>The cursor travels with the entity because a checkpoint turns it into a resume point: a query
 * started at that cursor continues with the entity after this one. The service sets a cursor on
 * every entity of a query's result, so no cursor has to be built here.
 */
@Internal
public final class FetchedEntity {

    private final Entity entity;
    private final ByteString cursor;
    private final EntityMetadata metadata;

    /**
     * Creates the triple.
     *
     * @param entity the entity read
     * @param cursor the cursor after the entity, as the service returned it
     * @param metadata the entity's version and times, as its query result carried them
     */
    public FetchedEntity(Entity entity, ByteString cursor, EntityMetadata metadata) {
        this.entity = Preconditions.checkNotNull(entity, "entity must not be null");
        this.cursor = Preconditions.checkNotNull(cursor, "cursor must not be null");
        Preconditions.checkArgument(!cursor.isEmpty(), "cursor must not be empty");
        this.metadata = Preconditions.checkNotNull(metadata, "metadata must not be null");
    }

    /** Returns the entity read. */
    public Entity getEntity() {
        return entity;
    }

    /** Returns the cursor after the entity. */
    public ByteString getCursor() {
        return cursor;
    }

    /** Returns the entity's version and times, as its query result carried them. */
    public EntityMetadata getMetadata() {
        return metadata;
    }
}
