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

package io.github.flink.gcp.connector.cloudtasks.sink.serializer;

import org.apache.flink.annotation.Internal;

import com.google.cloud.tasks.v2.AppEngineHttpRequest;
import com.google.cloud.tasks.v2.AppEngineRouting;
import com.google.cloud.tasks.v2.HttpMethod;
import com.google.cloud.tasks.v2.Task;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.cloudtasks.sink.AppEngineTargetChecks;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Map;

/** Converts one record and its serialized body into an App Engine target task. */
@Internal
final class AppEngineTargetTaskConverter<T> implements Serializable {

    private static final long serialVersionUID = 1L;
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
                    AppEngineTargetChecks.checkRelativeUri(
                            relativeUriExtractor.extractRelativeUri(element),
                            "extracted relative URI"));
        }
        if (routingExtractor != null) {
            AppEngineRouting extracted =
                    AppEngineTargetChecks.checkAndNormalizeRouting(
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
            AppEngineTargetChecks.checkHeaderName(name);
            request.putHeaders(name, header.getValue());
        }
    }
}
