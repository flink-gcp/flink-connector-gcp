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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import com.google.cloud.tasks.v2.HttpMethod;
import io.github.flink.gcp.connector.cloudtasks.table.CloudTasksConnectorOptions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Validated fixed HTTP request values used by the table serializer. */
@Internal
public final class TableHttpTarget implements Serializable {

    private static final long serialVersionUID = 1L;

    @Nullable private final String url;
    private final HttpMethod method;
    private final Map<String, String> headers;
    @Nullable private final String oidcServiceAccountEmail;
    @Nullable private final String oidcAudience;
    @Nullable private final String oauthServiceAccountEmail;
    @Nullable private final String oauthScope;

    private TableHttpTarget(
            @Nullable String url,
            HttpMethod method,
            Map<String, String> headers,
            @Nullable String oidcServiceAccountEmail,
            @Nullable String oidcAudience,
            @Nullable String oauthServiceAccountEmail,
            @Nullable String oauthScope) {
        this.url = url;
        this.method = method;
        this.headers = Collections.unmodifiableMap(new LinkedHashMap<>(headers));
        this.oidcServiceAccountEmail = oidcServiceAccountEmail;
        this.oidcAudience = oidcAudience;
        this.oauthServiceAccountEmail = oauthServiceAccountEmail;
        this.oauthScope = oauthScope;
    }

    /** Maps and validates the HTTP-related table options. */
    public static TableHttpTarget from(ReadableConfig config) {
        String url = config.getOptional(CloudTasksConnectorOptions.HTTP_URL).orElse(null);
        if (url != null && !isAbsoluteHttpUrl(url)) {
            throw new ValidationException(
                    String.format(
                            "Option '%s' must be an absolute http:// or https:// URL.",
                            CloudTasksConnectorOptions.HTTP_URL.key()));
        }

        HttpMethod method = config.get(CloudTasksConnectorOptions.HTTP_METHOD);
        if (method == HttpMethod.HTTP_METHOD_UNSPECIFIED || method == HttpMethod.UNRECOGNIZED) {
            throw new ValidationException(
                    String.format(
                            "Option '%s' must be a concrete HTTP method.",
                            CloudTasksConnectorOptions.HTTP_METHOD.key()));
        }

        Map<String, String> headers =
                config.getOptional(CloudTasksConnectorOptions.HTTP_HEADERS)
                        .orElse(Collections.emptyMap());
        Map<String, String> names = new LinkedHashMap<>();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            if (header.getKey().isBlank()) {
                throw new ValidationException(
                        String.format(
                                "Option '%s' contains a blank header name.",
                                CloudTasksConnectorOptions.HTTP_HEADERS.key()));
            }
            String normalized = header.getKey().toLowerCase(Locale.ROOT);
            String previous = names.put(normalized, header.getKey());
            if (previous != null) {
                throw new ValidationException(
                        String.format(
                                "Option '%s' contains duplicate header names '%s' and '%s'; HTTP"
                                        + " header names are case-insensitive.",
                                CloudTasksConnectorOptions.HTTP_HEADERS.key(),
                                previous,
                                header.getKey()));
            }
        }

        String oidcEmail =
                optionalNonBlank(
                        config, CloudTasksConnectorOptions.HTTP_OIDC_SERVICE_ACCOUNT_EMAIL);
        String oidcAudience =
                optionalNonBlank(config, CloudTasksConnectorOptions.HTTP_OIDC_AUDIENCE);
        String oauthEmail =
                optionalNonBlank(
                        config, CloudTasksConnectorOptions.HTTP_OAUTH_SERVICE_ACCOUNT_EMAIL);
        String oauthScope = optionalNonBlank(config, CloudTasksConnectorOptions.HTTP_OAUTH_SCOPE);
        if (oidcEmail != null && oauthEmail != null) {
            throw new ValidationException(
                    String.format(
                            "Options '%s' and '%s' cannot be combined: OIDC and OAuth tokens are a"
                                    + " Cloud Tasks oneof.",
                            CloudTasksConnectorOptions.HTTP_OIDC_SERVICE_ACCOUNT_EMAIL.key(),
                            CloudTasksConnectorOptions.HTTP_OAUTH_SERVICE_ACCOUNT_EMAIL.key()));
        }
        requireParent(
                oidcAudience,
                oidcEmail,
                CloudTasksConnectorOptions.HTTP_OIDC_AUDIENCE.key(),
                CloudTasksConnectorOptions.HTTP_OIDC_SERVICE_ACCOUNT_EMAIL.key());
        requireParent(
                oauthScope,
                oauthEmail,
                CloudTasksConnectorOptions.HTTP_OAUTH_SCOPE.key(),
                CloudTasksConnectorOptions.HTTP_OAUTH_SERVICE_ACCOUNT_EMAIL.key());
        return new TableHttpTarget(
                url, method, headers, oidcEmail, oidcAudience, oauthEmail, oauthScope);
    }

    @Nullable
    private static String optionalNonBlank(
            ReadableConfig config, org.apache.flink.configuration.ConfigOption<String> option) {
        String value = config.getOptional(option).orElse(null);
        if (value != null && value.isBlank()) {
            throw new ValidationException(
                    String.format("Option '%s' must not be blank.", option.key()));
        }
        return value;
    }

    private static void requireParent(
            @Nullable String child, @Nullable String parent, String childKey, String parentKey) {
        if (child != null && parent == null) {
            throw new ValidationException(
                    String.format("Option '%s' requires option '%s'.", childKey, parentKey));
        }
    }

    static boolean isAbsoluteHttpUrl(String value) {
        if (!value.startsWith("http://") && !value.startsWith("https://")) {
            return false;
        }
        try {
            URI uri = new URI(value);
            return uri.isAbsolute()
                    && uri.getRawAuthority() != null
                    && !uri.getRawAuthority().isBlank();
        } catch (URISyntaxException e) {
            return false;
        }
    }

    @Nullable
    String getUrl() {
        return url;
    }

    HttpMethod getMethod() {
        return method;
    }

    Map<String, String> getHeaders() {
        return headers;
    }

    @Nullable
    String getOidcServiceAccountEmail() {
        return oidcServiceAccountEmail;
    }

    @Nullable
    String getOidcAudience() {
        return oidcAudience;
    }

    @Nullable
    String getOauthServiceAccountEmail() {
        return oauthServiceAccountEmail;
    }

    @Nullable
    String getOauthScope() {
        return oauthScope;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableHttpTarget that = (TableHttpTarget) o;
        return Objects.equals(url, that.url)
                && method == that.method
                && headers.equals(that.headers)
                && Objects.equals(oidcServiceAccountEmail, that.oidcServiceAccountEmail)
                && Objects.equals(oidcAudience, that.oidcAudience)
                && Objects.equals(oauthServiceAccountEmail, that.oauthServiceAccountEmail)
                && Objects.equals(oauthScope, that.oauthScope);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                url,
                method,
                headers,
                oidcServiceAccountEmail,
                oidcAudience,
                oauthServiceAccountEmail,
                oauthScope);
    }
}
