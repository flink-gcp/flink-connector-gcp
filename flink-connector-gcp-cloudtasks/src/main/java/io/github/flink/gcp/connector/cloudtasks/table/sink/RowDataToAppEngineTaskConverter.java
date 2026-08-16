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
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import com.google.cloud.tasks.v2.AppEngineHttpRequest;
import com.google.cloud.tasks.v2.AppEngineRouting;
import com.google.cloud.tasks.v2.Task;
import com.google.protobuf.Timestamp;
import io.github.flink.gcp.connector.cloudtasks.sink.AppEngineTargetChecks;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/** Converts App Engine fixed options and writable metadata to a bodyless task. */
@Internal
final class RowDataToAppEngineTaskConverter implements RowDataToTaskConverter {

    private static final long serialVersionUID = 1L;

    private final AppEngineTargetSpec target;
    private final int relativeUriIndex;
    private final int methodIndex;
    private final int headersIndex;
    private final int serviceIndex;
    private final int versionIndex;
    private final int instanceIndex;
    private final int scheduleTimeIndex;

    @Nullable private transient AppEngineHttpRequest prototype;

    RowDataToAppEngineTaskConverter(
            int physicalArity, WritableMetadata[] metadata, AppEngineTargetSpec target) {
        Preconditions.checkArgument(physicalArity >= 0, "physicalArity must not be negative");
        WritableMetadata[] checkedMetadata =
                Preconditions.checkNotNull(metadata, "metadata must not be null");
        this.target = Preconditions.checkNotNull(target, "target must not be null");
        this.relativeUriIndex =
                WritableMetadata.RELATIVE_URI.position(physicalArity, checkedMetadata);
        this.methodIndex = WritableMetadata.HTTP_METHOD.position(physicalArity, checkedMetadata);
        this.headersIndex = WritableMetadata.HEADERS.position(physicalArity, checkedMetadata);
        this.serviceIndex =
                WritableMetadata.APP_ENGINE_SERVICE.position(physicalArity, checkedMetadata);
        this.versionIndex =
                WritableMetadata.APP_ENGINE_VERSION.position(physicalArity, checkedMetadata);
        this.instanceIndex =
                WritableMetadata.APP_ENGINE_INSTANCE.position(physicalArity, checkedMetadata);
        this.scheduleTimeIndex =
                WritableMetadata.SCHEDULE_TIME.position(physicalArity, checkedMetadata);
    }

    @Override
    public Task.Builder convert(RowData element) throws IOException {
        Task.Builder task = Task.newBuilder();
        AppEngineHttpRequest.Builder request =
                task.getAppEngineHttpRequestBuilder().mergeFrom(prototype());
        request.setRelativeUri(relativeUri(element));
        request.setHttpMethod(
                RowDataMetadataReader.readMethod(element, methodIndex, target.getMethod()));
        applyHeaders(element, request);
        applyRouting(element, request);
        Timestamp scheduleTime = RowDataMetadataReader.readScheduleTime(element, scheduleTimeIndex);
        if (scheduleTime != null) {
            task.setScheduleTime(scheduleTime);
        }
        return task;
    }

    @Override
    @Nullable
    public String getBodyContentType() {
        return target.getBodyContentType();
    }

    private String relativeUri(RowData element) throws IOException {
        String value =
                RowDataMetadataReader.readString(
                        element, relativeUriIndex, target.getRelativeUri());
        if (value == null) {
            throw new IOException(
                    "The Cloud Tasks App Engine relative URI resolved to null; configure"
                            + " 'app-engine.relative-uri' or provide non-null 'relative-uri'"
                            + " metadata.");
        }
        try {
            return AppEngineTargetChecks.checkRelativeUri(value, "relative-uri metadata");
        } catch (IllegalArgumentException | NullPointerException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    private void applyRouting(RowData element, AppEngineHttpRequest.Builder request) {
        if (serviceIndex < 0 && versionIndex < 0 && instanceIndex < 0) {
            return;
        }
        String service =
                RowDataMetadataReader.readString(element, serviceIndex, target.getService());
        String version =
                RowDataMetadataReader.readString(element, versionIndex, target.getVersion());
        String instance =
                RowDataMetadataReader.readString(element, instanceIndex, target.getInstance());
        AppEngineRouting.Builder routing = AppEngineRouting.newBuilder();
        if (service != null && !service.isEmpty()) {
            routing.setService(service);
        }
        if (version != null && !version.isEmpty()) {
            routing.setVersion(version);
        }
        if (instance != null && !instance.isEmpty()) {
            routing.setInstance(instance);
        }
        AppEngineRouting normalized =
                AppEngineTargetChecks.checkAndNormalizeRouting(routing.build(), "table routing");
        if (normalized == null) {
            request.clearAppEngineRouting();
        } else {
            request.setAppEngineRouting(normalized);
        }
    }

    private void applyHeaders(RowData element, AppEngineHttpRequest.Builder request)
            throws IOException {
        Map<String, String> rowHeaders =
                RowDataMetadataReader.readHeaders(element, headersIndex, "App Engine");
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
            try {
                AppEngineTargetChecks.checkHeaderName(name);
            } catch (IllegalArgumentException e) {
                throw new IOException(e.getMessage(), e);
            }
            String normalized = name.toLowerCase(Locale.ROOT);
            if (target.getBodyContentType() != null && "content-type".equals(normalized)) {
                if (!TargetSpec.sameContentType(target.getBodyContentType(), value)) {
                    throw new IOException(
                            "Cloud Tasks App Engine header metadata contains Content-Type '"
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

    private AppEngineHttpRequest prototype() {
        AppEngineHttpRequest built = prototype;
        if (built == null) {
            AppEngineHttpRequest.Builder builder =
                    AppEngineHttpRequest.newBuilder()
                            .setHttpMethod(target.getMethod())
                            .putAllHeaders(target.getHeaders());
            if (target.getRelativeUri() != null) {
                builder.setRelativeUri(target.getRelativeUri());
            }
            if (target.getRouting() != null) {
                builder.setAppEngineRouting(target.getRouting());
            }
            built = builder.build();
            prototype = built;
        }
        return built;
    }
}
