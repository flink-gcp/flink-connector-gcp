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

package io.github.flink.gcp.connector.datastore.source.batch.reader;

import org.apache.flink.annotation.Internal;

import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Entity;
import com.google.datastore.v1.Value;
import com.google.protobuf.ByteString;

import java.util.Map;

/**
 * Turns an entity a projection query returned into the client library's {@link Entity}, reading a
 * projected index value as the value it stands for.
 *
 * <p>A projection query returns a timestamp as an integer of microseconds and a blob as a string,
 * each marked with meaning 18, an index value. The client library's own query API returns such
 * results as a {@code ProjectionEntity}, whose {@code getTimestamp} and {@code getBlob} read those
 * two forms back when asked; nothing outside its package can build one. This applies the same two
 * rules to the protobuf before converting it, and the split reader applies it only to a query that
 * projects, as the library does. The value is replaced rather than read back on request, so {@link
 * Entity#getTimestamp(String)} and {@link Entity#getBlob(String)} answer on a projected property as
 * they do on a whole entity, while {@code getLong} and {@code getString}, which a {@code
 * ProjectionEntity} would answer with the raw index value, do not ({@code docs/adr/0177}). Any
 * other value, including one with meaning 18 of another type, is passed as the service returned it.
 */
@Internal
final class ProjectedValues {

    /** The meaning the service marks an index value with. */
    static final int INDEX_VALUE = 18;

    private ProjectedValues() {}

    /**
     * Converts an entity a query returned.
     *
     * @param entity the entity, as the service returned it
     * @return the client library's entity, with projected timestamps and blobs read back
     */
    static Entity toEntity(com.google.datastore.v1.Entity entity) {
        com.google.datastore.v1.Entity.Builder rewritten = null;
        for (Map.Entry<String, Value> property : entity.getPropertiesMap().entrySet()) {
            Value value = property.getValue();
            Value readBack = readBack(value);
            if (readBack != value) {
                if (rewritten == null) {
                    rewritten = entity.toBuilder();
                }
                rewritten.putProperties(property.getKey(), readBack);
            }
        }
        return Entity.fromPb(rewritten == null ? entity : rewritten.build());
    }

    private static Value readBack(Value value) {
        if (value.getMeaning() != INDEX_VALUE) {
            return value;
        }
        switch (value.getValueTypeCase()) {
            case INTEGER_VALUE:
                return value.toBuilder()
                        .clearMeaning()
                        .setTimestampValue(
                                Timestamp.ofTimeMicroseconds(value.getIntegerValue()).toProto())
                        .build();
            case STRING_VALUE:
                return value.toBuilder()
                        .clearMeaning()
                        .setBlobValue(ByteString.copyFromUtf8(value.getStringValue()))
                        .build();
            default:
                return value;
        }
    }
}
