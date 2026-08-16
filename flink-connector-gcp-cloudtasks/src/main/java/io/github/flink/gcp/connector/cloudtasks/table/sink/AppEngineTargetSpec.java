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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;

import com.google.cloud.tasks.v2.AppEngineRouting;
import com.google.cloud.tasks.v2.HttpMethod;
import io.github.flink.gcp.connector.cloudtasks.sink.AppEngineTargetChecks;
import io.github.flink.gcp.connector.cloudtasks.table.CloudTasksConnectorOptions;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Validated fixed App Engine request values used by the table serializer. */
@Internal
public final class AppEngineTargetSpec extends TargetSpec {

    private static final long serialVersionUID = 1L;

    @Nullable private final String relativeUri;
    private final HttpMethod method;
    private final Map<String, String> headers;
    @Nullable private final String bodyContentType;
    @Nullable private final AppEngineRouting routing;

    private AppEngineTargetSpec(
            @Nullable String relativeUri,
            HttpMethod method,
            Map<String, String> headers,
            @Nullable String bodyContentType,
            @Nullable AppEngineRouting routing) {
        this.relativeUri = relativeUri;
        this.method = method;
        this.headers = Collections.unmodifiableMap(new LinkedHashMap<>(headers));
        this.bodyContentType = bodyContentType;
        this.routing = routing;
    }

    /** Maps and validates the App Engine-related table options. */
    public static AppEngineTargetSpec from(
            ReadableConfig config, @Nullable String bodyContentType) {
        String relativeUri =
                config.getOptional(CloudTasksConnectorOptions.APP_ENGINE_RELATIVE_URI).orElse(null);
        if (relativeUri != null) {
            try {
                AppEngineTargetChecks.checkRelativeUri(
                        relativeUri,
                        "Option '"
                                + CloudTasksConnectorOptions.APP_ENGINE_RELATIVE_URI.key()
                                + "'");
            } catch (IllegalArgumentException | NullPointerException e) {
                throw new ValidationException(e.getMessage(), e);
            }
        }

        HttpMethod method = config.get(CloudTasksConnectorOptions.APP_ENGINE_METHOD);
        if (method == HttpMethod.HTTP_METHOD_UNSPECIFIED || method == HttpMethod.UNRECOGNIZED) {
            throw new ValidationException(
                    String.format(
                            "Option '%s' must be a concrete HTTP method.",
                            CloudTasksConnectorOptions.APP_ENGINE_METHOD.key()));
        }

        Map<String, String> configuredHeaders =
                config.getOptional(CloudTasksConnectorOptions.APP_ENGINE_HEADERS)
                        .orElse(Collections.emptyMap());
        Map<String, String> names = new LinkedHashMap<>();
        Map<String, String> headers = new LinkedHashMap<>();
        for (Map.Entry<String, String> header : configuredHeaders.entrySet()) {
            String name = header.getKey();
            if (name.isBlank()) {
                throw new ValidationException(
                        String.format(
                                "Option '%s' contains a blank header name.",
                                CloudTasksConnectorOptions.APP_ENGINE_HEADERS.key()));
            }
            try {
                AppEngineTargetChecks.checkHeaderName(name);
            } catch (IllegalArgumentException e) {
                throw new ValidationException(e.getMessage(), e);
            }
            String normalized = name.toLowerCase(Locale.ROOT);
            String previous = names.put(normalized, name);
            if (previous != null) {
                throw new ValidationException(
                        String.format(
                                "Option '%s' contains duplicate header names '%s' and '%s'; HTTP"
                                        + " header names are case-insensitive.",
                                CloudTasksConnectorOptions.APP_ENGINE_HEADERS.key(),
                                previous,
                                name));
            }
            if (bodyContentType != null && "content-type".equals(normalized)) {
                if (!TargetSpec.sameContentType(bodyContentType, header.getValue())) {
                    throw new ValidationException(
                            String.format(
                                    "Option '%s' contains Content-Type '%s', which conflicts with"
                                            + " the body format's Content-Type '%s'.",
                                    CloudTasksConnectorOptions.APP_ENGINE_HEADERS.key(),
                                    header.getValue(),
                                    bodyContentType));
                }
                continue;
            }
            headers.put(name, header.getValue());
        }

        AppEngineRouting routing =
                routing(
                        config.getOptional(CloudTasksConnectorOptions.APP_ENGINE_SERVICE)
                                .orElse(null),
                        config.getOptional(CloudTasksConnectorOptions.APP_ENGINE_VERSION)
                                .orElse(null),
                        config.getOptional(CloudTasksConnectorOptions.APP_ENGINE_INSTANCE)
                                .orElse(null));
        return new AppEngineTargetSpec(relativeUri, method, headers, bodyContentType, routing);
    }

    @Nullable
    private static AppEngineRouting routing(
            @Nullable String service, @Nullable String version, @Nullable String instance) {
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
        return AppEngineTargetChecks.checkAndNormalizeRouting(routing.build(), "table routing");
    }

    @Nullable
    String getRelativeUri() {
        return relativeUri;
    }

    HttpMethod getMethod() {
        return method;
    }

    Map<String, String> getHeaders() {
        return headers;
    }

    @Nullable
    String getBodyContentType() {
        return bodyContentType;
    }

    @Nullable
    String getService() {
        return routing == null ? null : routing.getService();
    }

    @Nullable
    String getVersion() {
        return routing == null ? null : routing.getVersion();
    }

    @Nullable
    String getInstance() {
        return routing == null ? null : routing.getInstance();
    }

    @Nullable
    AppEngineRouting getRouting() {
        return routing;
    }

    @Override
    Map<String, DataType> writableMetadata() {
        return WritableMetadata.listAppEngine();
    }

    @Override
    WritableMetadata addressMetadata() {
        return WritableMetadata.RELATIVE_URI;
    }

    @Override
    String addressOptionKey() {
        return CloudTasksConnectorOptions.APP_ENGINE_RELATIVE_URI.key();
    }

    @Override
    @Nullable
    String fixedAddress() {
        return relativeUri;
    }

    @Override
    RowDataToTaskConverter converter(int physicalArity, WritableMetadata[] metadata) {
        return new RowDataToAppEngineTaskConverter(physicalArity, metadata, this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AppEngineTargetSpec that = (AppEngineTargetSpec) o;
        return Objects.equals(relativeUri, that.relativeUri)
                && method == that.method
                && headers.equals(that.headers)
                && Objects.equals(bodyContentType, that.bodyContentType)
                && Objects.equals(routing, that.routing);
    }

    @Override
    public int hashCode() {
        return Objects.hash(relativeUri, method, headers, bodyContentType, routing);
    }
}
