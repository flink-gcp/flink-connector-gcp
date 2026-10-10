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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;

import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import io.github.flink.gcp.connector.datastore.source.serializer.EntityMetadata;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/** The metadata a {@code datastore} table scan can read, by the key a DDL declares it under. */
@Internal
enum ReadableMetadata {
    /** The key's name, NULL when the key has a numeric id instead. */
    KEY_NAME("key-name", DataTypes.STRING()),
    /**
     * The key's numeric id, NULL when the key has a name instead: what a table without a PRIMARY
     * KEY was written under.
     */
    KEY_ID("key-id", DataTypes.BIGINT()),
    /** The entity's version, a positive number that grows with every change to the entity. */
    VERSION("version", DataTypes.BIGINT().notNull()),
    /** When the entity was created. */
    CREATE_TIME("create-time", DataTypes.TIMESTAMP_LTZ(6).notNull()),
    /** When the entity was last changed. */
    UPDATE_TIME("update-time", DataTypes.TIMESTAMP_LTZ(6).notNull()),
    /** The time the entity was read at: the scan's one read time. */
    READ_TIME("read-time", DataTypes.TIMESTAMP_LTZ(6).notNull());

    private final String key;
    private final DataType dataType;

    ReadableMetadata(String key, DataType dataType) {
        this.key = key;
        this.dataType = dataType;
    }

    String key() {
        return key;
    }

    /**
     * Reads this metadata of an entity as its Flink internal value.
     *
     * @throws IOException if the query result carries no version, create time or update time where
     *     this metadata is one of them
     */
    @Nullable
    Object read(Entity entity, EntityMetadata metadata) throws IOException {
        Key entityKey = entity.getKey();
        switch (this) {
            case KEY_NAME:
                return entityKey.hasName() ? StringData.fromString(entityKey.getName()) : null;
            case KEY_ID:
                return entityKey.hasId() ? entityKey.getId() : null;
            case VERSION:
                // A proto3 scalar has no presence; a full result's version is strictly positive.
                if (metadata.getVersion() <= 0) {
                    throw missing(entityKey);
                }
                return metadata.getVersion();
            case CREATE_TIME:
                return timestamp(metadata.getCreateTime(), entityKey);
            case UPDATE_TIME:
                return timestamp(metadata.getUpdateTime(), entityKey);
            case READ_TIME:
                return timestamp(metadata.getReadTime(), entityKey);
            default:
                throw new IllegalStateException("Unknown metadata " + this);
        }
    }

    private Object timestamp(@Nullable Timestamp timestamp, Key entityKey) throws IOException {
        if (timestamp == null) {
            throw missing(entityKey);
        }
        // Truncated to the declared TIMESTAMP_LTZ(6): a configured read time may be finer.
        return DatastoreToRowDataConverter.timestampData(timestamp, 1_000);
    }

    private IOException missing(Key entityKey) {
        return new IOException(
                "Datastore returned "
                        + entityKey
                        + " without its '"
                        + key
                        + "', which a full query result carries.");
    }

    /** Every key with its type, in declaration order. */
    static Map<String, DataType> listAll() {
        Map<String, DataType> all = new LinkedHashMap<>();
        for (ReadableMetadata metadata : values()) {
            all.put(metadata.key, metadata.dataType);
        }
        return all;
    }

    /**
     * Returns the metadata of a key, refusing one this connector does not declare: a compiled plan
     * restored against another build applies its keys without the planner's validation.
     */
    static ReadableMetadata of(String key) {
        for (ReadableMetadata metadata : values()) {
            if (metadata.key.equals(key)) {
                return metadata;
            }
        }
        throw new IllegalArgumentException(
                "The datastore table connector has no readable metadata '" + key + "'.");
    }
}
