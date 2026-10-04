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

package io.github.flink.gcp.connector.firestore.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.DocumentSnapshot;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * The metadata a {@code firestore} table reads beside a document's fields, in the order the planner
 * lays them out after the physical columns: its path, and the three times the service reports.
 */
@Internal
enum ReadableMetadata {
    /** The document's path relative to the database, such as {@code users/alice/orders/o1}. */
    DOCUMENT_PATH(
            "document-path",
            DataTypes.STRING().notNull(),
            document -> StringData.fromString(document.getReference().getPath())),
    /** When the document was created. */
    CREATE_TIME(
            "create-time",
            DataTypes.TIMESTAMP_LTZ(6).notNull(),
            document -> timestamp(document.getCreateTime())),
    /** When the document was last updated. */
    UPDATE_TIME(
            "update-time",
            DataTypes.TIMESTAMP_LTZ(6).notNull(),
            document -> timestamp(document.getUpdateTime())),
    /** The time the document was read at: the scan's one read time. */
    READ_TIME(
            "read-time",
            DataTypes.TIMESTAMP_LTZ(6).notNull(),
            document -> timestamp(document.getReadTime()));

    private final String key;
    private final DataType dataType;
    private final Function<DocumentSnapshot, Object> reader;

    ReadableMetadata(String key, DataType dataType, Function<DocumentSnapshot, Object> reader) {
        this.key = key;
        this.dataType = dataType;
        this.reader = reader;
    }

    String key() {
        return key;
    }

    /** Reads this metadata of a document as its Flink internal value. */
    Object read(DocumentSnapshot document) {
        return reader.apply(document);
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
                "The firestore table connector has no readable metadata '" + key + "'.");
    }

    private static TimestampData timestamp(Timestamp timestamp) {
        return FirestoreToRowDataConverter.timestampData(timestamp, 1);
    }
}
