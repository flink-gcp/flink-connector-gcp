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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.util.Preconditions;

import com.google.cloud.tasks.v2.AppEngineRouting;
import com.google.cloud.tasks.v2.HttpMethod;
import io.github.flink.gcp.connector.cloudtasks.sink.AppEngineTargetChecks;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Map;

/** Builds an immutable {@link AppEngineTargetSerializationSchema}. */
@PublicEvolving
public final class AppEngineTargetBuilder<T> {

    private final String relativeUri;

    @Nullable private SerializationSchema<T> body;
    private HttpMethod method = HttpMethod.POST;

    @Nullable private RelativeUriExtractor<? super T> relativeUriExtractor;
    @Nullable private HeadersExtractor<? super T> headersExtractor;
    @Nullable private AppEngineRouting routing;
    @Nullable private RoutingExtractor<? super T> routingExtractor;

    AppEngineTargetBuilder(String relativeUri) {
        this.relativeUri = AppEngineTargetChecks.checkRelativeUri(relativeUri, "relativeUri");
    }

    private AppEngineTargetBuilder(
            String relativeUri,
            SerializationSchema<T> body,
            HttpMethod method,
            @Nullable AppEngineRouting routing) {
        this.relativeUri = relativeUri;
        this.body = body;
        this.method = method;
        this.routing = routing;
    }

    /**
     * Sets the request-body serializer and binds the record type.
     *
     * <p>The body must be set before record-dependent URI, header, or routing extractors. It can be
     * set only once. Fixed method and routing settings configured before it are retained.
     *
     * @param newBody the request-body serializer
     * @param <U> type of the records written by the sink
     * @return the same logical builder, typed for the records accepted by the body serializer
     */
    public <U> AppEngineTargetBuilder<U> withBody(SerializationSchema<U> newBody) {
        Preconditions.checkState(body == null, "body has already been set");
        Preconditions.checkNotNull(newBody, "body must not be null");
        return new AppEngineTargetBuilder<>(relativeUri, newBody, method, routing);
    }

    /**
     * Sets the request method. Defaults to POST.
     *
     * <p>App Engine requests carry a body only under POST and PUT. Under every other method the
     * body serializer remains unused.
     *
     * @param newMethod the concrete HTTP method
     * @return this builder
     */
    public AppEngineTargetBuilder<T> withMethod(HttpMethod newMethod) {
        Preconditions.checkNotNull(newMethod, "method must not be null");
        Preconditions.checkArgument(
                newMethod != HttpMethod.HTTP_METHOD_UNSPECIFIED
                        && newMethod != HttpMethod.UNRECOGNIZED,
                "method must be a concrete HTTP method");
        method = newMethod;
        return this;
    }

    /**
     * Sets an extractor that resolves the relative URI per record.
     *
     * @param extractor the relative-URI extractor
     * @return this builder
     */
    public AppEngineTargetBuilder<T> withRelativeUri(RelativeUriExtractor<? super T> extractor) {
        requireBodyBeforeRecordOptions();
        relativeUriExtractor = Preconditions.checkNotNull(extractor, "extractor must not be null");
        return this;
    }

    /**
     * Sets an extractor that adds headers to each request.
     *
     * @param extractor the headers extractor; {@code null} or an empty map adds no headers
     * @return this builder
     */
    public AppEngineTargetBuilder<T> withHeaders(HeadersExtractor<? super T> extractor) {
        requireBodyBeforeRecordOptions();
        headersExtractor = Preconditions.checkNotNull(extractor, "extractor must not be null");
        return this;
    }

    /**
     * Sets fixed task-level App Engine routing.
     *
     * <p>An empty routing value leaves routing unspecified. The queue's {@code
     * appEngineRoutingOverride}, when configured, takes precedence over this value.
     *
     * @param newRouting service, version, and instance routing; {@code host} must remain empty
     * @return this builder
     */
    public AppEngineTargetBuilder<T> withRouting(AppEngineRouting newRouting) {
        Preconditions.checkNotNull(newRouting, "routing must not be null");
        routing = AppEngineTargetChecks.checkAndNormalizeRouting(newRouting, "routing");
        routingExtractor = null;
        return this;
    }

    /**
     * Sets an extractor that resolves task-level App Engine routing per record.
     *
     * <p>This replaces fixed routing previously configured on the builder. The queue's {@code
     * appEngineRoutingOverride}, when configured, takes precedence over each extracted value.
     *
     * @param extractor the routing extractor; {@code null} or empty routing selects App Engine
     *     defaults
     * @return this builder
     */
    public AppEngineTargetBuilder<T> withRouting(RoutingExtractor<? super T> extractor) {
        requireBodyBeforeRecordOptions();
        routingExtractor = Preconditions.checkNotNull(extractor, "extractor must not be null");
        routing = null;
        return this;
    }

    /**
     * Builds an immutable serialization schema from the current settings.
     *
     * <p>Later changes to this builder do not affect a schema already built from it.
     *
     * @return the configured serialization schema
     */
    public AppEngineTargetSerializationSchema<T> build() {
        SerializationSchema<T> configuredBody =
                Preconditions.checkNotNull(body, "body must be set before build");
        return new AppEngineTargetSerializationSchema<>(
                configuredBody,
                AppEngineTargetTaskConverter.of(
                        relativeUri,
                        method,
                        relativeUriExtractor,
                        headersExtractor,
                        routing,
                        routingExtractor));
    }

    private void requireBodyBeforeRecordOptions() {
        Preconditions.checkState(body != null, "body must be set before record-dependent options");
    }

    /** Extracts the App Engine relative URI from a record. */
    @PublicEvolving
    @FunctionalInterface
    public interface RelativeUriExtractor<T> extends Serializable {

        /**
         * Returns the task's relative URI.
         *
         * @param element the record
         * @return an empty string for the root path, or a path beginning with {@code /} and an
         *     optional query
         */
        String extractRelativeUri(T element);
    }

    /** Extracts App Engine request headers from a record. */
    @PublicEvolving
    @FunctionalInterface
    public interface HeadersExtractor<T> extends Serializable {

        /**
         * Returns the task headers.
         *
         * @param element the record
         * @return headers, or {@code null} for none
         */
        @Nullable
        Map<String, String> extractHeaders(T element);
    }

    /** Extracts App Engine service, version, and instance routing from a sink record. */
    @PublicEvolving
    @FunctionalInterface
    public interface RoutingExtractor<T> extends Serializable {

        /**
         * Returns the routing for the task built from the given record.
         *
         * <p>{@code null} or an empty routing value leaves routing unspecified, so App Engine
         * chooses the default service, version, and an available instance. {@link
         * AppEngineRouting#getHost() host} is output-only and must remain empty.
         *
         * @param element the record
         * @return task-level routing, or {@code null} for the App Engine defaults
         */
        @Nullable
        AppEngineRouting extractRouting(T element);
    }
}
