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
import org.apache.flink.util.StringUtils;

import com.google.cloud.tasks.v2.HttpMethod;
import com.google.cloud.tasks.v2.HttpRequest;
import com.google.cloud.tasks.v2.OAuthToken;
import com.google.cloud.tasks.v2.OidcToken;
import com.google.cloud.tasks.v2.Task;
import com.google.protobuf.ByteString;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * A {@link CloudTasksSerializationSchema} producing HTTP-target tasks: a URL, an HTTP method, an
 * optional authorization token, optional headers, and a body serialized by a wrapped Flink {@link
 * SerializationSchema}.
 *
 * <p>Obtained from {@link CloudTasksSerializationSchema#httpTarget(String)}. Instances are
 * immutable: every {@code with*} method returns a new schema, and the schema itself is what the
 * sink builder takes, so no terminal {@code build()} call is needed.
 *
 * <pre>{@code
 * CloudTasksSerializationSchema.httpTarget("https://api.example.com/v1/orders")
 *         .withBody(new MyEventJsonSerializationSchema())
 *         .withMethod(HttpMethod.POST)
 *         .withUrl(e -> "https://api.example.com/v1/orders/" + e.orderId())
 *         .withHeaders(e -> Map.of("Content-Type", "application/json"))
 *         .withOidcToken("dispatcher@my-project.iam.gserviceaccount.com");
 * }</pre>
 *
 * <p>Authorization follows what is being called: OIDC for Cloud Run, Cloud Run functions and
 * anything else on Google Cloud behind IAM (and for third-party endpoints that validate the
 * Google-issued token themselves), OAuth only for Google APIs on {@code *.googleapis.com}. The two
 * are a proto {@code oneof}, so setting both is rejected rather than silently resolved.
 *
 * <p>Tasks produced here never carry a name — naming is the sink's, through {@code
 * CloudTasksSinkBuilder#taskIdExtractor(TaskIdExtractor)}.
 *
 * @param <T> type of the records written by the sink
 */
@PublicEvolving
public final class HttpTargetSerializationSchema<T> implements CloudTasksSerializationSchema<T> {

    private static final long serialVersionUID = 1L;

    private final String url;
    private final HttpMethod method;
    private final SerializationSchema<T> body;
    @Nullable private final UrlExtractor<? super T> urlExtractor;
    @Nullable private final HeadersExtractor<? super T> headersExtractor;
    @Nullable private final String oidcServiceAccount;
    @Nullable private final String oidcAudience;
    @Nullable private final String oauthServiceAccount;
    @Nullable private final String oauthScope;

    /**
     * Whether the method allows a request body. Cloud Tasks accepts one only on {@code POST},
     * {@code PUT} and {@code PATCH}, and rejects a task that carries one under any other method.
     */
    private final boolean carriesBody;

    /**
     * The invariant part of every request this schema produces (URL, method, authorization token),
     * built once per task and copied per record. Transient because it is derived state: the schema
     * travels in the job graph as its plain fields.
     */
    @Nullable private transient HttpRequest prototype;

    private HttpTargetSerializationSchema(
            String url,
            HttpMethod method,
            SerializationSchema<T> body,
            @Nullable UrlExtractor<? super T> urlExtractor,
            @Nullable HeadersExtractor<? super T> headersExtractor,
            @Nullable String oidcServiceAccount,
            @Nullable String oidcAudience,
            @Nullable String oauthServiceAccount,
            @Nullable String oauthScope) {
        this.url = url;
        this.method = method;
        this.body = body;
        this.urlExtractor = urlExtractor;
        this.headersExtractor = headersExtractor;
        this.oidcServiceAccount = oidcServiceAccount;
        this.oidcAudience = oidcAudience;
        this.oauthServiceAccount = oauthServiceAccount;
        this.oauthScope = oauthScope;
        this.carriesBody =
                method == HttpMethod.POST || method == HttpMethod.PUT || method == HttpMethod.PATCH;
    }

    static <T> HttpTargetSerializationSchema<T> of(String url, SerializationSchema<T> body) {
        Preconditions.checkNotNull(body, "body must not be null");
        return new HttpTargetSerializationSchema<>(
                url, HttpMethod.POST, body, null, null, null, null, null, null);
    }

    /**
     * Returns a schema sending its requests with the given HTTP method. Defaults to {@link
     * HttpMethod#POST}.
     *
     * <p>Cloud Tasks allows a request body only on {@code POST}, {@code PUT} and {@code PATCH} —
     * setting one on any other method is an error the service rejects. Under a method that forbids
     * a body the schema therefore sends none, and the body schema goes unused rather than failing
     * every task at the service.
     *
     * @param method the HTTP method
     * @return the new schema
     */
    public HttpTargetSerializationSchema<T> withMethod(HttpMethod method) {
        Preconditions.checkNotNull(method, "method must not be null");
        Preconditions.checkArgument(
                method != HttpMethod.HTTP_METHOD_UNSPECIFIED && method != HttpMethod.UNRECOGNIZED,
                "method must be a concrete HTTP method");
        return new HttpTargetSerializationSchema<>(
                url,
                method,
                body,
                urlExtractor,
                headersExtractor,
                oidcServiceAccount,
                oidcAudience,
                oauthServiceAccount,
                oauthScope);
    }

    /**
     * Returns a schema resolving the target URL per record, overriding the URL the chain started
     * from.
     *
     * <p>Note that a queue carrying an {@code httpTarget.uriOverride} overrides task-level URLs and
     * cannot be detected through the v2 client, so per-record URLs silently go to the queue's URL
     * against such a queue.
     *
     * @param extractor the URL extractor; must return an absolute {@code http://} or {@code
     *     https://} URL
     * @return the new schema
     */
    public HttpTargetSerializationSchema<T> withUrl(UrlExtractor<? super T> extractor) {
        Preconditions.checkNotNull(extractor, "extractor must not be null");
        return new HttpTargetSerializationSchema<>(
                url,
                method,
                body,
                extractor,
                headersExtractor,
                oidcServiceAccount,
                oidcAudience,
                oauthServiceAccount,
                oauthScope);
    }

    /**
     * Returns a schema adding the extracted headers to its requests.
     *
     * @param extractor the headers extractor; {@code null} or an empty map adds no headers
     * @return the new schema
     */
    public HttpTargetSerializationSchema<T> withHeaders(HeadersExtractor<? super T> extractor) {
        Preconditions.checkNotNull(extractor, "extractor must not be null");
        return new HttpTargetSerializationSchema<>(
                url,
                method,
                body,
                urlExtractor,
                extractor,
                oidcServiceAccount,
                oidcAudience,
                oauthServiceAccount,
                oauthScope);
    }

    /**
     * Returns a schema authorizing its requests with an OIDC token minted for the given service
     * account, with the target URL as the audience.
     *
     * @param serviceAccountEmail the service account the token is minted for
     * @return the new schema
     */
    public HttpTargetSerializationSchema<T> withOidcToken(String serviceAccountEmail) {
        return withOidcToken(serviceAccountEmail, null);
    }

    /**
     * Returns a schema authorizing its requests with an OIDC token minted for the given service
     * account and audience.
     *
     * @param serviceAccountEmail the service account the token is minted for
     * @param audience the token audience, or {@code null} to let Cloud Tasks default it to the
     *     target URL
     * @return the new schema
     */
    public HttpTargetSerializationSchema<T> withOidcToken(
            String serviceAccountEmail, @Nullable String audience) {
        checkNotBlank(serviceAccountEmail, "serviceAccountEmail");
        checkNoOtherToken(oauthServiceAccount, "OAuth", "OIDC");
        return new HttpTargetSerializationSchema<>(
                url,
                method,
                body,
                urlExtractor,
                headersExtractor,
                serviceAccountEmail,
                audience,
                null,
                null);
    }

    /**
     * Returns a schema authorizing its requests with an OAuth access token minted for the given
     * service account.
     *
     * <p>Google documents OAuth tokens as "generally only" useful for calling Google APIs on {@code
     * *.googleapis.com}; anything else behind IAM wants {@link #withOidcToken(String)}.
     *
     * @param serviceAccountEmail the service account the token is minted for
     * @return the new schema
     */
    public HttpTargetSerializationSchema<T> withOAuthToken(String serviceAccountEmail) {
        return withOAuthToken(serviceAccountEmail, null);
    }

    /**
     * Returns a schema authorizing its requests with an OAuth access token minted for the given
     * service account and scope.
     *
     * @param serviceAccountEmail the service account the token is minted for
     * @param scope the OAuth scope, or {@code null} to let Cloud Tasks default it
     * @return the new schema
     */
    public HttpTargetSerializationSchema<T> withOAuthToken(
            String serviceAccountEmail, @Nullable String scope) {
        checkNotBlank(serviceAccountEmail, "serviceAccountEmail");
        checkNoOtherToken(oidcServiceAccount, "OIDC", "OAuth");
        return new HttpTargetSerializationSchema<>(
                url,
                method,
                body,
                urlExtractor,
                headersExtractor,
                null,
                null,
                serviceAccountEmail,
                scope);
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        body.open(context);
    }

    @Override
    public Task serialize(T element) throws IOException {
        HttpRequest.Builder request = prototype().toBuilder();
        if (urlExtractor != null) {
            request.setUrl(checkUrl(urlExtractor.extractUrl(element), "extracted url"));
        }
        if (carriesBody) {
            request.setBody(ByteString.copyFrom(body.serialize(element)));
        }
        if (headersExtractor != null) {
            Map<String, String> headers = headersExtractor.extractHeaders(element);
            if (headers != null && !headers.isEmpty()) {
                for (Map.Entry<String, String> header : headers.entrySet()) {
                    // Explicit throws (not the varargs Preconditions overloads) keep the
                    // per-entry checks allocation-free on the per-record path.
                    if (header.getKey() == null) {
                        throw new NullPointerException(
                                "The headers extractor returned a null key.");
                    }
                    if (header.getValue() == null) {
                        throw new NullPointerException(
                                "The headers extractor returned a null value for key '"
                                        + header.getKey()
                                        + "'.");
                    }
                    request.putHeaders(header.getKey(), header.getValue());
                }
            }
        }
        return Task.newBuilder().setHttpRequest(request).build();
    }

    private HttpRequest prototype() {
        HttpRequest built = prototype;
        if (built == null) {
            HttpRequest.Builder builder =
                    HttpRequest.newBuilder().setUrl(url).setHttpMethod(method);
            if (oidcServiceAccount != null) {
                OidcToken.Builder token =
                        OidcToken.newBuilder().setServiceAccountEmail(oidcServiceAccount);
                if (oidcAudience != null) {
                    token.setAudience(oidcAudience);
                }
                builder.setOidcToken(token);
            } else if (oauthServiceAccount != null) {
                OAuthToken.Builder token =
                        OAuthToken.newBuilder().setServiceAccountEmail(oauthServiceAccount);
                if (oauthScope != null) {
                    token.setScope(oauthScope);
                }
                builder.setOauthToken(token);
            }
            built = builder.build();
            prototype = built;
        }
        return built;
    }

    private static void checkNoOtherToken(
            @Nullable String otherServiceAccount, String other, String requested) {
        Preconditions.checkState(
                otherServiceAccount == null,
                "A %s token is already set; %s and %s tokens are a proto oneof and cannot both be"
                        + " set. The target decides which one applies: OIDC for Google Cloud"
                        + " services behind IAM, OAuth only for *.googleapis.com.",
                other,
                other,
                requested);
    }

    private static void checkNotBlank(String value, String name) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(value), "%s must not be blank", name);
    }

    static String checkUrl(String value, String name) {
        checkNotBlank(value, name);
        Preconditions.checkArgument(
                value.startsWith("http://") || value.startsWith("https://"),
                "%s must be an absolute http:// or https:// URL: '%s'",
                name,
                value);
        return value;
    }

    /**
     * Extracts the target URL from a record.
     *
     * @param <T> type of the records written by the sink
     */
    @PublicEvolving
    @FunctionalInterface
    public interface UrlExtractor<T> extends Serializable {

        /**
         * Returns the target URL of the task built for the given record.
         *
         * @param element the record
         * @return the URL; an absolute {@code http://} or {@code https://} URL
         */
        String extractUrl(T element);
    }

    /**
     * Extracts HTTP headers from a record.
     *
     * @param <T> type of the records written by the sink
     */
    @PublicEvolving
    @FunctionalInterface
    public interface HeadersExtractor<T> extends Serializable {

        /**
         * Returns the headers of the task built for the given record. {@code null} or an empty map
         * adds no headers; entries must have non-null keys and values.
         *
         * @param element the record
         * @return the headers, or {@code null} for none
         */
        Map<String, String> extractHeaders(T element);
    }
}
