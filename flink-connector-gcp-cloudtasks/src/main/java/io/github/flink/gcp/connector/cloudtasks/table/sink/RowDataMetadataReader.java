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
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;

import com.google.cloud.tasks.v2.HttpMethod;
import com.google.protobuf.Timestamp;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Shared readers for writable metadata used by both request target families. */
@Internal
final class RowDataMetadataReader {

    static HttpMethod readMethod(RowData element, int index, HttpMethod fallback)
            throws IOException {
        if (index < 0 || element.isNullAt(index)) {
            return fallback;
        }
        String value = element.getString(index).toString();
        try {
            HttpMethod method = HttpMethod.valueOf(value.toUpperCase(Locale.ROOT));
            if (method == HttpMethod.HTTP_METHOD_UNSPECIFIED || method == HttpMethod.UNRECOGNIZED) {
                throw new IllegalArgumentException();
            }
            return method;
        } catch (IllegalArgumentException e) {
            throw new IOException(
                    "The 'http-method' metadata value '"
                            + value
                            + "' is not one of POST, GET, HEAD, PUT, DELETE, PATCH or OPTIONS.",
                    e);
        }
    }

    @Nullable
    static String readString(RowData element, int index, @Nullable String fallback) {
        if (index < 0 || element.isNullAt(index)) {
            return fallback;
        }
        return element.getString(index).toString();
    }

    static Map<String, String> readHeaders(RowData element, int index, String targetName)
            throws IOException {
        if (index < 0 || element.isNullAt(index)) {
            return Collections.emptyMap();
        }
        MapData map = element.getMap(index);
        ArrayData keys = map.keyArray();
        ArrayData values = map.valueArray();
        Set<String> normalizedNames = new HashSet<>();
        Map<String, String> headers = new LinkedHashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            if (keys.isNullAt(i) || values.isNullAt(i)) {
                throw new IOException(
                        "A Cloud Tasks "
                                + targetName
                                + " header has a null "
                                + (keys.isNullAt(i) ? "name" : "value")
                                + ", which an HTTP request cannot represent.");
            }
            String name = keys.getString(i).toString();
            String value = values.getString(i).toString();
            if (name.isBlank()) {
                throw new IOException("A Cloud Tasks " + targetName + " header has a blank name.");
            }
            if (!normalizedNames.add(name.toLowerCase(Locale.ROOT))) {
                throw new IOException(
                        "Cloud Tasks "
                                + targetName
                                + " header metadata contains the case-insensitive duplicate name '"
                                + name
                                + "'.");
            }
            headers.put(name, value);
        }
        return headers;
    }

    @Nullable
    static Timestamp readScheduleTime(RowData element, int index) {
        if (index < 0 || element.isNullAt(index)) {
            return null;
        }
        TimestampData value = element.getTimestamp(index, 6);
        Instant instant = value.toInstant();
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    private RowDataMetadataReader() {}
}
