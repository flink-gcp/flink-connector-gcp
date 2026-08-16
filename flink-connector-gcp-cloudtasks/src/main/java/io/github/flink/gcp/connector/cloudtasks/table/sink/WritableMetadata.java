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

package io.github.flink.gcp.connector.cloudtasks.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Request properties a Cloud Tasks table can supply as writable metadata columns. */
@Internal
enum WritableMetadata {
    URL("url", DataTypes.STRING().nullable()),
    RELATIVE_URI("relative-uri", DataTypes.STRING().nullable()),
    HTTP_METHOD("http-method", DataTypes.STRING().nullable()),
    HEADERS(
            "headers",
            DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.STRING().nullable()).nullable()),
    APP_ENGINE_SERVICE("app-engine-service", DataTypes.STRING().nullable()),
    APP_ENGINE_VERSION("app-engine-version", DataTypes.STRING().nullable()),
    APP_ENGINE_INSTANCE("app-engine-instance", DataTypes.STRING().nullable()),
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

    int position(int physicalArity, WritableMetadata[] metadata) {
        for (int i = 0; i < metadata.length; i++) {
            if (metadata[i] == this) {
                return physicalArity + i;
            }
        }
        return -1;
    }

    static Map<String, DataType> listHttp() {
        return list(List.of(URL, HTTP_METHOD, HEADERS, SCHEDULE_TIME, TASK_ID));
    }

    static Map<String, DataType> listAppEngine() {
        return list(
                List.of(
                        RELATIVE_URI,
                        HTTP_METHOD,
                        HEADERS,
                        APP_ENGINE_SERVICE,
                        APP_ENGINE_VERSION,
                        APP_ENGINE_INSTANCE,
                        SCHEDULE_TIME,
                        TASK_ID));
    }

    private static Map<String, DataType> list(List<WritableMetadata> values) {
        Map<String, DataType> result = new LinkedHashMap<>();
        for (WritableMetadata value : values) {
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
