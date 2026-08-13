/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.cloudtasks.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.util.LinkedHashMap;
import java.util.Map;

/** Request properties a Cloud Tasks table can supply as writable metadata columns. */
@Internal
enum WritableMetadata {
    URL("url", DataTypes.STRING().nullable()),
    HTTP_METHOD("http-method", DataTypes.STRING().nullable()),
    HEADERS(
            "headers",
            DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.STRING().nullable()).nullable()),
    SCHEDULE_TIME("schedule-time", DataTypes.TIMESTAMP_LTZ(6).nullable()),
    TASK_ID("task-id", DataTypes.STRING().nullable());

    private final String key;
    private final DataType dataType;

    WritableMetadata(String key, DataType dataType) {
        this.key = key;
        this.dataType = dataType;
    }

    String getKey() {
        return key;
    }

    static Map<String, DataType> listAll() {
        Map<String, DataType> result = new LinkedHashMap<>();
        for (WritableMetadata value : values()) {
            result.put(value.key, value.dataType);
        }
        return result;
    }

    static WritableMetadata of(String key) {
        for (WritableMetadata value : values()) {
            if (value.key.equals(key)) {
                return value;
            }
        }
        throw new IllegalArgumentException(
                "Unknown Cloud Tasks writable metadata key '" + key + "'.");
    }
}
