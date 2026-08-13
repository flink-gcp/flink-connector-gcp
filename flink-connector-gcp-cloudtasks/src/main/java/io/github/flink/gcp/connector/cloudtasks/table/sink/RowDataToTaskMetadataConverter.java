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
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.util.Preconditions;

import com.google.cloud.tasks.v2.HttpMethod;
import com.google.cloud.tasks.v2.HttpRequest;
import com.google.cloud.tasks.v2.OAuthToken;
import com.google.cloud.tasks.v2.OidcToken;
import com.google.cloud.tasks.v2.Task;
import com.google.protobuf.Timestamp;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Converts writable metadata columns and fixed target options to a bodyless task builder. */
@Internal
final class RowDataToTaskMetadataConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    private final TableHttpTarget target;
    private final int urlIndex;
    private final int methodIndex;
    private final int headersIndex;
    private final int scheduleTimeIndex;

    @Nullable private transient HttpRequest prototype;

    RowDataToTaskMetadataConverter(
            int physicalArity, WritableMetadata[] metadata, TableHttpTarget target) {
        Preconditions.checkArgument(physicalArity >= 0, "physicalArity must not be negative");
        WritableMetadata[] checkedMetadata =
                Preconditions.checkNotNull(metadata, "metadata must not be null");
        this.target = Preconditions.checkNotNull(target, "target must not be null");
        this.urlIndex = metadataIndex(physicalArity, checkedMetadata, WritableMetadata.URL);
        this.methodIndex =
                metadataIndex(physicalArity, checkedMetadata, WritableMetadata.HTTP_METHOD);
        this.headersIndex = metadataIndex(physicalArity, checkedMetadata, WritableMetadata.HEADERS);
        this.scheduleTimeIndex =
                metadataIndex(physicalArity, checkedMetadata, WritableMetadata.SCHEDULE_TIME);
    }

    Task.Builder convert(RowData element) throws IOException {
        Task.Builder task = Task.newBuilder();
        HttpRequest.Builder request = task.getHttpRequestBuilder().mergeFrom(prototype());
        request.setUrl(url(element));
        request.setHttpMethod(method(element));
        applyHeaders(element, request);

        if (scheduleTimeIndex >= 0 && !element.isNullAt(scheduleTimeIndex)) {
            TimestampData value = element.getTimestamp(scheduleTimeIndex, 6);
            Instant instant = value.toInstant();
            task.setScheduleTime(
                    Timestamp.newBuilder()
                            .setSeconds(instant.getEpochSecond())
                            .setNanos(instant.getNano()));
        }
        return task;
    }

    @Nullable
    String getBodyContentType() {
        return target.getBodyContentType();
    }

    private String url(RowData element) throws IOException {
        String value = target.getUrl();
        if (urlIndex >= 0 && !element.isNullAt(urlIndex)) {
            value = element.getString(urlIndex).toString();
        }
        if (value == null || !TableHttpTarget.isAbsoluteHttpUrl(value)) {
            throw new IOException(
                    "The Cloud Tasks target URL must be an absolute http:// or https:// URL, but"
                            + " the row resolved to '"
                            + value
                            + "'.");
        }
        return value;
    }

    private HttpMethod method(RowData element) throws IOException {
        if (methodIndex < 0 || element.isNullAt(methodIndex)) {
            return target.getMethod();
        }
        String value = element.getString(methodIndex).toString();
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

    private void applyHeaders(RowData element, HttpRequest.Builder request) throws IOException {
        if (headersIndex < 0 || element.isNullAt(headersIndex)) {
            return;
        }
        MapData map = element.getMap(headersIndex);
        ArrayData keys = map.keyArray();
        ArrayData values = map.valueArray();
        Map<String, String> requestHeaderNames = new LinkedHashMap<>();
        for (String name : request.getHeadersMap().keySet()) {
            requestHeaderNames.put(name.toLowerCase(Locale.ROOT), name);
        }
        Set<String> rowHeaderNames = new HashSet<>();
        for (int i = 0; i < keys.size(); i++) {
            if (keys.isNullAt(i) || values.isNullAt(i)) {
                throw new IOException(
                        "A Cloud Tasks HTTP header has a null "
                                + (keys.isNullAt(i) ? "name" : "value")
                                + ", which an HTTP request cannot represent.");
            }
            String name = keys.getString(i).toString();
            String value = values.getString(i).toString();
            if (name.isBlank()) {
                throw new IOException("A Cloud Tasks HTTP header has a blank name.");
            }
            String normalized = name.toLowerCase(Locale.ROOT);
            if (!rowHeaderNames.add(normalized)) {
                throw new IOException(
                        "Cloud Tasks HTTP header metadata contains the case-insensitive duplicate"
                                + " name '"
                                + name
                                + "'.");
            }
            if (target.getBodyContentType() != null && "content-type".equals(normalized)) {
                if (!TableHttpTarget.sameContentType(target.getBodyContentType(), value)) {
                    throw new IOException(
                            "Cloud Tasks HTTP header metadata contains Content-Type '"
                                    + value
                                    + "', which conflicts with the body format's Content-Type '"
                                    + target.getBodyContentType()
                                    + "'.");
                }
                continue;
            }
            String existing = requestHeaderNames.put(normalized, name);
            if (existing != null) {
                request.removeHeaders(existing);
            }
            request.putHeaders(name, value);
        }
    }

    private HttpRequest prototype() {
        HttpRequest built = prototype;
        if (built == null) {
            HttpRequest.Builder builder =
                    HttpRequest.newBuilder()
                            .setHttpMethod(target.getMethod())
                            .putAllHeaders(target.getHeaders());
            if (target.getUrl() != null) {
                builder.setUrl(target.getUrl());
            }
            if (target.getOidcServiceAccountEmail() != null) {
                OidcToken.Builder token =
                        OidcToken.newBuilder()
                                .setServiceAccountEmail(target.getOidcServiceAccountEmail());
                if (target.getOidcAudience() != null) {
                    token.setAudience(target.getOidcAudience());
                }
                builder.setOidcToken(token);
            } else if (target.getOauthServiceAccountEmail() != null) {
                OAuthToken.Builder token =
                        OAuthToken.newBuilder()
                                .setServiceAccountEmail(target.getOauthServiceAccountEmail());
                if (target.getOauthScope() != null) {
                    token.setScope(target.getOauthScope());
                }
                builder.setOauthToken(token);
            }
            built = builder.build();
            prototype = built;
        }
        return built;
    }

    private static int metadataIndex(
            int physicalArity, WritableMetadata[] metadata, WritableMetadata wanted) {
        for (int i = 0; i < metadata.length; i++) {
            if (metadata[i] == wanted) {
                return physicalArity + i;
            }
        }
        return -1;
    }
}
