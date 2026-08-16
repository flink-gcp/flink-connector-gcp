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

package io.github.flink.gcp.connector.cloudtasks.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import com.google.cloud.tasks.v2.HttpMethod;

import java.time.Duration;
import java.util.Map;

/** The {@code WITH} options of the {@code cloud-tasks} table connector. */
@PublicEvolving
public final class CloudTasksConnectorOptions {

    public static final ConfigOption<CloudTasksTargetType> TARGET_TYPE =
            ConfigOptions.key("target.type")
                    .enumType(CloudTasksTargetType.class)
                    .defaultValue(CloudTasksTargetType.HTTP)
                    .withDescription(
                            "The task request target. 'http' preserves the external HTTP target;"
                                    + " 'app-engine' selects an App Engine request in the queue's"
                                    + " project and region.");

    public static final ConfigOption<String> PROJECT =
            ConfigOptions.key("project")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Google Cloud project owning the task queue.");

    public static final ConfigOption<String> LOCATION =
            ConfigOptions.key("location")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The region containing the task queue.");

    public static final ConfigOption<String> QUEUE =
            ConfigOptions.key("queue")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The queue id. The connector never creates a queue.");

    public static final ConfigOption<String> HTTP_URL =
            ConfigOptions.key("http.url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The default absolute HTTP target URL. A non-null 'url' metadata value"
                                    + " overrides it per row. When omitted, the table must declare"
                                    + " a NOT NULL writable 'url' metadata column.");

    public static final ConfigOption<HttpMethod> HTTP_METHOD =
            ConfigOptions.key("http.method")
                    .enumType(HttpMethod.class)
                    .defaultValue(HttpMethod.POST)
                    .withDescription(
                            "The default HTTP method. A non-null 'http-method' metadata value"
                                    + " overrides it per row. Only POST, PUT and PATCH carry the"
                                    + " encoded body.");

    public static final ConfigOption<Map<String, String>> HTTP_HEADERS =
            ConfigOptions.key("http.headers")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Default HTTP headers. Row metadata overrides a matching header name"
                                    + " case-insensitively.");

    public static final ConfigOption<String> HTTP_OIDC_SERVICE_ACCOUNT_EMAIL =
            ConfigOptions.key("http.oidc.service-account-email")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Service account used to mint an OIDC token for Cloud Run, Cloud Run"
                                    + " functions, or another handler that validates Google OIDC"
                                    + " tokens.");

    public static final ConfigOption<String> HTTP_OIDC_AUDIENCE =
            ConfigOptions.key("http.oidc.audience")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "OIDC audience. Requires http.oidc.service-account-email; when absent"
                                    + " Cloud Tasks uses the target URL.");

    public static final ConfigOption<String> HTTP_OAUTH_SERVICE_ACCOUNT_EMAIL =
            ConfigOptions.key("http.oauth.service-account-email")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Service account used to mint an OAuth access token for a Google API"
                                    + " endpoint on *.googleapis.com.");

    public static final ConfigOption<String> HTTP_OAUTH_SCOPE =
            ConfigOptions.key("http.oauth.scope")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "OAuth scope. Requires http.oauth.service-account-email; when absent"
                                    + " Cloud Tasks uses its default scope.");

    public static final ConfigOption<String> APP_ENGINE_RELATIVE_URI =
            ConfigOptions.key("app-engine.relative-uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The default App Engine path and optional query. A non-null"
                                    + " 'relative-uri' metadata value overrides it per row. When"
                                    + " omitted, the table must declare writable 'relative-uri'"
                                    + " metadata as STRING NOT NULL.");

    public static final ConfigOption<HttpMethod> APP_ENGINE_METHOD =
            ConfigOptions.key("app-engine.method")
                    .enumType(HttpMethod.class)
                    .defaultValue(HttpMethod.POST)
                    .withDescription(
                            "The default App Engine request method. A non-null 'http-method'"
                                    + " metadata value overrides it per row. Only POST and PUT"
                                    + " carry the encoded body.");

    public static final ConfigOption<Map<String, String>> APP_ENGINE_HEADERS =
            ConfigOptions.key("app-engine.headers")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Default App Engine request headers. Row metadata overrides a"
                                    + " matching header name case-insensitively.");

    public static final ConfigOption<String> APP_ENGINE_SERVICE =
            ConfigOptions.key("app-engine.service")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The default App Engine service. A non-null 'app-engine-service'"
                                    + " metadata value overrides it per row.");

    public static final ConfigOption<String> APP_ENGINE_VERSION =
            ConfigOptions.key("app-engine.version")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The default App Engine version. A non-null 'app-engine-version'"
                                    + " metadata value overrides it per row.");

    public static final ConfigOption<String> APP_ENGINE_INSTANCE =
            ConfigOptions.key("app-engine.instance")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The default App Engine instance. A non-null 'app-engine-instance'"
                                    + " metadata value overrides it per row; instance routing"
                                    + " requires a manually scaled service.");

    public static final ConfigOption<String> SERVICE_ACCOUNT_KEY_FILE =
            ConfigOptions.key("service-account-key-file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Path to a service-account JSON key file readable from every eligible"
                                    + " TaskManager. Uses application-default credentials when"
                                    + " unset and cannot be combined with emulator-endpoint.");

    public static final ConfigOption<String> EMULATOR_ENDPOINT =
            ConfigOptions.key("emulator-endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Host and port of a Cloud Tasks emulator. It uses plaintext with no"
                                    + " credentials and is for tests only.");

    public static final ConfigOption<Integer> SINK_MAX_IN_FLIGHT_TASKS =
            ConfigOptions.key("sink.max-in-flight-tasks")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The cap on outstanding task creations per sink subtask.");

    public static final ConfigOption<Duration> SINK_RETRY_INITIAL_BACKOFF =
            ConfigOptions.key("sink.retry.initial-backoff")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The first backoff for transient CreateTask failures.");

    public static final ConfigOption<Duration> SINK_RETRY_MAX_BACKOFF =
            ConfigOptions.key("sink.retry.max-backoff")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The cap on the transient-failure retry backoff.");

    public static final ConfigOption<Integer> SINK_RETRY_MAX_ATTEMPTS =
            ConfigOptions.key("sink.retry.max-attempts")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The maximum transient-failure attempts, including the first.");

    public static final ConfigOption<Duration> SINK_NOT_FOUND_RETRY_INITIAL_BACKOFF =
            ConfigOptions.key("sink.not-found-retry.initial-backoff")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The first backoff of the separate NOT_FOUND retry budget.");

    public static final ConfigOption<Duration> SINK_NOT_FOUND_RETRY_MAX_BACKOFF =
            ConfigOptions.key("sink.not-found-retry.max-backoff")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The cap on the NOT_FOUND retry backoff.");

    public static final ConfigOption<Integer> SINK_NOT_FOUND_RETRY_MAX_ATTEMPTS =
            ConfigOptions.key("sink.not-found-retry.max-attempts")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The maximum NOT_FOUND attempts, including the first.");

    public static final ConfigOption<Boolean> SINK_METRICS_PER_DESTINATION =
            ConfigOptions.key("sink.metrics.per-destination")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("Whether to register per-queue send counters.");

    private CloudTasksConnectorOptions() {}
}
