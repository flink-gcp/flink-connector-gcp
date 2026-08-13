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

package io.github.flink.gcp.connector.cloudtasks.sink.serializer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.tasks.v2.AppEngineHttpRequest;
import com.google.cloud.tasks.v2.AppEngineRouting;
import com.google.cloud.tasks.v2.HttpMethod;
import com.google.cloud.tasks.v2.Task;
import com.google.protobuf.ByteString;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Map;

/** Converts one record and its serialized body into an App Engine target task. */
@Internal
final class AppEngineTargetTaskConverter<T> implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final int MAX_RELATIVE_URI_LENGTH = 2083;

    private final String relativeUri;
    private final HttpMethod method;

    @Nullable
    private final AppEngineTargetBuilder.RelativeUriExtractor<? super T> relativeUriExtractor;

    @Nullable private final AppEngineTargetBuilder.HeadersExtractor<? super T> headersExtractor;

    @Nullable private final AppEngineRouting routing;

    @Nullable private final AppEngineTargetBuilder.RoutingExtractor<? super T> routingExtractor;

    /** Derived invariant request state; rebuilt after Flink deserializes the converter. */
    @Nullable private transient AppEngineHttpRequest prototype;

    private AppEngineTargetTaskConverter(
            String relativeUri,
            HttpMethod method,
            @Nullable AppEngineTargetBuilder.RelativeUriExtractor<? super T> relativeUriExtractor,
            @Nullable AppEngineTargetBuilder.HeadersExtractor<? super T> headersExtractor,
            @Nullable AppEngineRouting routing,
            @Nullable AppEngineTargetBuilder.RoutingExtractor<? super T> routingExtractor) {
        this.relativeUri = relativeUri;
        this.method = method;
        this.relativeUriExtractor = relativeUriExtractor;
        this.headersExtractor = headersExtractor;
        this.routing = routing;
        this.routingExtractor = routingExtractor;
    }

    static <T> AppEngineTargetTaskConverter<T> of(
            String relativeUri,
            HttpMethod method,
            @Nullable AppEngineTargetBuilder.RelativeUriExtractor<? super T> relativeUriExtractor,
            @Nullable AppEngineTargetBuilder.HeadersExtractor<? super T> headersExtractor,
            @Nullable AppEngineRouting routing,
            @Nullable AppEngineTargetBuilder.RoutingExtractor<? super T> routingExtractor) {
        return new AppEngineTargetTaskConverter<>(
                relativeUri,
                method,
                relativeUriExtractor,
                headersExtractor,
                routing,
                routingExtractor);
    }

    boolean carriesBody() {
        return method == HttpMethod.POST || method == HttpMethod.PUT;
    }

    Task convert(T element, @Nullable byte[] body) {
        AppEngineHttpRequest.Builder request = prototype().toBuilder();
        if (relativeUriExtractor != null) {
            request.setRelativeUri(
                    checkRelativeUri(
                            relativeUriExtractor.extractRelativeUri(element),
                            "extracted relative URI"));
        }
        if (routingExtractor != null) {
            AppEngineRouting extracted =
                    normalizedRouting(
                            routingExtractor.extractRouting(element), "extracted routing");
            if (extracted != null) {
                request.setAppEngineRouting(extracted);
            }
        }
        if (body != null) {
            request.setBody(ByteString.copyFrom(body));
        }
        if (headersExtractor != null) {
            putHeaders(request, headersExtractor.extractHeaders(element));
        }
        return Task.newBuilder().setAppEngineHttpRequest(request).build();
    }

    private AppEngineHttpRequest prototype() {
        AppEngineHttpRequest built = prototype;
        if (built == null) {
            AppEngineHttpRequest.Builder builder =
                    AppEngineHttpRequest.newBuilder()
                            .setRelativeUri(relativeUri)
                            .setHttpMethod(method);
            if (routing != null) {
                builder.setAppEngineRouting(routing);
            }
            built = builder.build();
            prototype = built;
        }
        return built;
    }

    private static void putHeaders(
            AppEngineHttpRequest.Builder request, @Nullable Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return;
        }
        for (Map.Entry<String, String> header : headers.entrySet()) {
            String name = header.getKey();
            if (name == null) {
                throw new NullPointerException("The headers extractor returned a null key.");
            }
            if (header.getValue() == null) {
                throw new NullPointerException(
                        "The headers extractor returned a null value for key '" + name + "'.");
            }
            checkHeaderName(name);
            request.putHeaders(name, header.getValue());
        }
    }

    private static void checkHeaderName(String name) {
        String normalized = name.trim().toLowerCase(Locale.ROOT);
        Preconditions.checkArgument(
                !normalized.equals("host")
                        && !normalized.equals("content-length")
                        && !normalized.startsWith("x-google-")
                        && !normalized.startsWith("x-appengine-"),
                "App Engine header '%s' is set by Cloud Tasks and cannot be overridden",
                name);
    }

    @Nullable
    static AppEngineRouting normalizedRouting(@Nullable AppEngineRouting value, String name) {
        if (value == null) {
            return null;
        }
        Preconditions.checkArgument(
                value.getHost().isEmpty(),
                "%s must not set AppEngineRouting.host because host is output-only",
                name);
        if (value.getService().isEmpty()
                && value.getVersion().isEmpty()
                && value.getInstance().isEmpty()) {
            return null;
        }
        return value;
    }

    static String checkRelativeUri(String value, String name) {
        Preconditions.checkNotNull(value, "%s must not be null", name);
        Preconditions.checkArgument(
                value.length() <= MAX_RELATIVE_URI_LENGTH,
                "%s must be at most %s characters",
                name,
                MAX_RELATIVE_URI_LENGTH);
        if (value.isEmpty()) {
            return value;
        }
        Preconditions.checkArgument(
                value.startsWith("/"), "%s must be empty or begin with '/': '%s'", name, value);
        Preconditions.checkArgument(
                value.chars().noneMatch(Character::isWhitespace),
                "%s must not contain whitespace: '%s'",
                name,
                value);
        final URI parsed;
        try {
            parsed = new URI(value);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(
                    name + " must be a valid HTTP relative URI: '" + value + "'");
        }
        Preconditions.checkArgument(
                !parsed.isAbsolute()
                        && parsed.getRawAuthority() == null
                        && parsed.getRawFragment() == null,
                "%s must contain only a path and optional query: '%s'",
                name,
                value);
        return value;
    }
}
