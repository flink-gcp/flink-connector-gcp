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
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import com.google.cloud.tasks.v2.HttpRequest;
import com.google.cloud.tasks.v2.OAuthToken;
import com.google.cloud.tasks.v2.OidcToken;
import com.google.cloud.tasks.v2.Task;
import com.google.protobuf.Timestamp;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/** Converts writable metadata columns and fixed target options to a bodyless task builder. */
@Internal
final class RowDataToTaskMetadataConverter implements RowDataToTaskConverter {

    private static final long serialVersionUID = 1L;

    private final HttpTargetSpec target;
    private final int urlIndex;
    private final int methodIndex;
    private final int headersIndex;
    private final int scheduleTimeIndex;

    @Nullable private transient HttpRequest prototype;

    RowDataToTaskMetadataConverter(
            int physicalArity, WritableMetadata[] metadata, HttpTargetSpec target) {
        Preconditions.checkArgument(physicalArity >= 0, "physicalArity must not be negative");
        WritableMetadata[] checkedMetadata =
                Preconditions.checkNotNull(metadata, "metadata must not be null");
        this.target = Preconditions.checkNotNull(target, "target must not be null");
        this.urlIndex = WritableMetadata.URL.position(physicalArity, checkedMetadata);
        this.methodIndex = WritableMetadata.HTTP_METHOD.position(physicalArity, checkedMetadata);
        this.headersIndex = WritableMetadata.HEADERS.position(physicalArity, checkedMetadata);
        this.scheduleTimeIndex =
                WritableMetadata.SCHEDULE_TIME.position(physicalArity, checkedMetadata);
    }

    @Override
    public Task.Builder convert(RowData element) throws IOException {
        Task.Builder task = Task.newBuilder();
        HttpRequest.Builder request = task.getHttpRequestBuilder().mergeFrom(prototype());
        request.setUrl(url(element));
        request.setHttpMethod(
                RowDataMetadataReader.readMethod(element, methodIndex, target.getMethod()));
        applyHeaders(element, request);
        Timestamp scheduleTime = RowDataMetadataReader.readScheduleTime(element, scheduleTimeIndex);
        if (scheduleTime != null) {
            task.setScheduleTime(scheduleTime);
        }
        return task;
    }

    @Nullable
    public String getBodyContentType() {
        return target.getBodyContentType();
    }

    private String url(RowData element) throws IOException {
        String value = target.getUrl();
        if (urlIndex >= 0 && !element.isNullAt(urlIndex)) {
            value = element.getString(urlIndex).toString();
        }
        if (value == null || !HttpTargetSpec.isAbsoluteHttpUrl(value)) {
            throw new IOException(
                    "The Cloud Tasks target URL must be an absolute http:// or https:// URL, but"
                            + " the row resolved to '"
                            + value
                            + "'.");
        }
        return value;
    }

    private void applyHeaders(RowData element, HttpRequest.Builder request) throws IOException {
        Map<String, String> rowHeaders =
                RowDataMetadataReader.readHeaders(element, headersIndex, "HTTP");
        if (rowHeaders.isEmpty()) {
            return;
        }
        Map<String, String> requestHeaderNames = new LinkedHashMap<>();
        for (String name : request.getHeadersMap().keySet()) {
            requestHeaderNames.put(name.toLowerCase(Locale.ROOT), name);
        }
        for (Map.Entry<String, String> header : rowHeaders.entrySet()) {
            String name = header.getKey();
            String value = header.getValue();
            String normalized = name.toLowerCase(Locale.ROOT);
            if (target.getBodyContentType() != null && "content-type".equals(normalized)) {
                if (!HttpTargetSpec.sameContentType(target.getBodyContentType(), value)) {
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
}
