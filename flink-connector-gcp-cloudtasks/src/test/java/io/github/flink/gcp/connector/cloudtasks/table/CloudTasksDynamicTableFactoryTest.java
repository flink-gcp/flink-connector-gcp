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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;

import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksCreateTaskSink;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksWriterOptions;
import io.github.flink.gcp.connector.cloudtasks.table.form.FormUrlEncodedFormatFactory;
import io.github.flink.gcp.connector.cloudtasks.table.sink.AppEngineTargetSpec;
import io.github.flink.gcp.connector.cloudtasks.table.sink.CloudTasksDynamicSink;
import io.github.flink.gcp.connector.cloudtasks.table.sink.HttpTargetSpec;
import io.github.flink.gcp.connector.testutils.StubWriterInitContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CloudTasksDynamicTableFactory}. */
class CloudTasksDynamicTableFactoryTest {

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("id", DataTypes.STRING()),
                    Column.physical("amount", DataTypes.INT()));

    private static final ResolvedSchema FORM_SCHEMA =
            ResolvedSchema.of(Column.physical("name", DataTypes.STRING()));

    private static Map<String, String> minimalOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", CloudTasksDynamicTableFactory.IDENTIFIER);
        options.put("project", "my-project");
        options.put("location", "asia-northeast1");
        options.put("queue", "orders");
        options.put("http.url", "https://example.com/tasks");
        options.put("format", "json");
        return options;
    }

    private static CloudTasksCreateTaskSink<?> runtimeSink(Map<String, String> options) {
        return runtimeSink(SCHEMA, options);
    }

    private static CloudTasksCreateTaskSink<?> runtimeSink(
            ResolvedSchema schema, Map<String, String> options) {
        SinkV2Provider provider =
                (SinkV2Provider)
                        FactoryMocks.createTableSink(schema, options)
                                .getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        return (CloudTasksCreateTaskSink<?>) provider.createSink();
    }

    private static Map<String, String> formOptions() {
        Map<String, String> options = minimalOptions();
        options.put("format", FormUrlEncodedFormatFactory.IDENTIFIER);
        return options;
    }

    private static Map<String, String> appEngineFormOptions() {
        Map<String, String> options = appEngineOptions();
        options.put("format", FormUrlEncodedFormatFactory.IDENTIFIER);
        return options;
    }

    private static Task serialize(Map<String, String> options, GenericRowData row)
            throws Exception {
        return serialize(SCHEMA, options, row);
    }

    private static Task serialize(
            ResolvedSchema schema, Map<String, String> options, GenericRowData row)
            throws Exception {
        io.github.flink.gcp.connector.cloudtasks.sink.serializer.CloudTasksSerializationSchema<
                        ? super GenericRowData>
                serializer =
                        ((CloudTasksCreateTaskSink<GenericRowData>) runtimeSink(schema, options))
                                .getConfig()
                                .getSerializer();
        serializer.open(new StubWriterInitContext(0).asSerializationSchemaInitializationContext());
        return serializer.serialize(row);
    }

    private static Map<String, String> appEngineOptions() {
        Map<String, String> options = minimalOptions();
        options.remove("http.url");
        options.put("target.type", "app-engine");
        options.put("app-engine.relative-uri", "/tasks/default");
        return options;
    }

    @Test
    void buildsASinkFromTheMinimalOptionSet() {
        DynamicTableSink sink = FactoryMocks.createTableSink(SCHEMA, minimalOptions());

        assertThat(sink).isInstanceOf(CloudTasksDynamicSink.class);
        assertThat(sink.asSummaryString()).isEqualTo("Cloud Tasks table sink");
    }

    @Test
    void httpRemainsTheDefaultTargetType() throws Exception {
        Task task =
                serialize(
                        minimalOptions(),
                        GenericRowData.of(StringData.fromString("order-1"), Integer.valueOf(17)));

        assertThat(task.getMessageTypeCase()).isEqualTo(Task.MessageTypeCase.HTTP_REQUEST);
        assertThat(task.getHttpRequest().getUrl()).isEqualTo("https://example.com/tasks");
    }

    @Test
    void mapsFixedAppEngineTargetOptions() throws Exception {
        Map<String, String> options = appEngineOptions();
        options.put("app-engine.method", "PUT");
        options.put("app-engine.headers.X-Origin", "table");
        options.put("app-engine.service", "worker");
        options.put("app-engine.version", "v2");
        options.put("app-engine.instance", "instance-3");

        Task task =
                serialize(
                        options,
                        GenericRowData.of(StringData.fromString("order-1"), Integer.valueOf(17)));

        assertThat(task.getMessageTypeCase())
                .isEqualTo(Task.MessageTypeCase.APP_ENGINE_HTTP_REQUEST);
        assertThat(task.getAppEngineHttpRequest().getRelativeUri()).isEqualTo("/tasks/default");
        assertThat(task.getAppEngineHttpRequest().getHttpMethod())
                .isEqualTo(com.google.cloud.tasks.v2.HttpMethod.PUT);
        assertThat(task.getAppEngineHttpRequest().getHeadersMap())
                .containsEntry("X-Origin", "table");
        assertThat(task.getAppEngineHttpRequest().getAppEngineRouting().getService())
                .isEqualTo("worker");
        assertThat(task.getAppEngineHttpRequest().getAppEngineRouting().getVersion())
                .isEqualTo("v2");
        assertThat(task.getAppEngineHttpRequest().getAppEngineRouting().getInstance())
                .isEqualTo("instance-3");
    }

    @ParameterizedTest(name = "format={0}")
    @ValueSource(strings = {"json", "csv"})
    void discoversAnyEncodingFormat(String format) {
        Map<String, String> options = minimalOptions();
        options.put("format", format);

        assertThat(FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(CloudTasksDynamicSink.class);
    }

    @Test
    void discoversTheBuiltInFormEncodingFormat() throws Exception {
        CloudTasksCreateTaskSink<?> sink = runtimeSink(FORM_SCHEMA, formOptions());

        Task task =
                ((CloudTasksCreateTaskSink<GenericRowData>) sink)
                        .getConfig()
                        .getSerializer()
                        .serialize(GenericRowData.of(StringData.fromString("Alice & Bob")));

        assertThat(task.getHttpRequest().getBody().toStringUtf8()).isEqualTo("name=Alice+%26+Bob");
        assertThat(task.getHttpRequest().getHeadersMap())
                .containsEntry("Content-Type", FormUrlEncodedFormatFactory.CONTENT_TYPE);
    }

    @Test
    void passesTheFormContentTypeToTheAppEngineTarget() throws Exception {
        Map<String, String> options = appEngineOptions();
        options.put("format", FormUrlEncodedFormatFactory.IDENTIFIER);

        Task task =
                serialize(
                        FORM_SCHEMA,
                        options,
                        GenericRowData.of(StringData.fromString("Alice & Bob")));

        assertThat(task.getMessageTypeCase())
                .isEqualTo(Task.MessageTypeCase.APP_ENGINE_HTTP_REQUEST);
        assertThat(task.getAppEngineHttpRequest().getBody().toStringUtf8())
                .isEqualTo("name=Alice+%26+Bob");
        assertThat(task.getAppEngineHttpRequest().getHeadersMap())
                .containsEntry("Content-Type", FormUrlEncodedFormatFactory.CONTENT_TYPE);
    }

    @Test
    void validatesFormPhysicalTypesWhileCreatingTheTableSink() {
        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, formOptions()))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("supports only STRING and ARRAY<STRING>")
                .hasStackTraceContaining("amount")
                .hasStackTraceContaining("Cast the value to STRING explicitly in SQL");
    }

    @Test
    void rejectsAFixedContentTypeThatConflictsWithTheFormFormat() {
        Map<String, String> options = formOptions();
        options.put(
                "http.headers.content-type", "application/x-www-form-urlencoded; charset=UTF-8");

        assertThatThrownBy(() -> FactoryMocks.createTableSink(FORM_SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("conflicts with the body format's Content-Type")
                .hasStackTraceContaining("application/x-www-form-urlencoded; charset=UTF-8");
    }

    @Test
    void acceptsAndCanonicalizesAMatchingFixedContentType() throws Exception {
        Map<String, String> options = formOptions();
        options.put("http.headers.content-type", " APPLICATION/X-WWW-FORM-URLENCODED ");

        CloudTasksCreateTaskSink<?> sink = runtimeSink(FORM_SCHEMA, options);
        Task task =
                ((CloudTasksCreateTaskSink<GenericRowData>) sink)
                        .getConfig()
                        .getSerializer()
                        .serialize(GenericRowData.of(StringData.fromString("Alice")));

        assertThat(task.getHttpRequest().getHeadersMap())
                .containsOnly(
                        org.assertj.core.api.Assertions.entry(
                                "Content-Type", FormUrlEncodedFormatFactory.CONTENT_TYPE));
    }

    @Test
    void passesTheConfiguredSinkParallelismToTheProvider() {
        Map<String, String> options = minimalOptions();
        options.put("sink.parallelism", "3");

        SinkV2Provider provider =
                (SinkV2Provider)
                        FactoryMocks.createTableSink(SCHEMA, options)
                                .getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));

        assertThat(provider.getParallelism()).contains(3);
    }

    @Test
    void mapsEveryWriterOptionWithoutReplacingUnspecifiedDefaults() {
        Map<String, String> options = minimalOptions();
        options.put("sink.max-in-flight-tasks", "17");
        options.put("sink.channel-pool-size", "4");
        options.put("sink.retry.initial-backoff", "200 ms");
        options.put("sink.retry.max-backoff", "4 s");
        options.put("sink.retry.max-attempts", "5");
        options.put("sink.not-found-retry.initial-backoff", "600 ms");
        options.put("sink.not-found-retry.max-backoff", "3 s");
        options.put("sink.not-found-retry.max-attempts", "4");
        options.put("sink.metrics.per-destination", "true");

        CloudTasksWriterOptions mapped = runtimeSink(options).getConfig().getWriterOptions();

        assertThat(mapped.getMaxInFlightTasks()).isEqualTo(17);
        assertThat(mapped.getChannelPoolSize()).isEqualTo(4);
        assertThat(mapped.getRetryInitialBackoff()).isEqualTo(Duration.ofMillis(200));
        assertThat(mapped.getRetryMaxBackoff()).isEqualTo(Duration.ofSeconds(4));
        assertThat(mapped.getRetryMaxAttempts()).isEqualTo(5);
        assertThat(mapped.getNotFoundInitialBackoff()).isEqualTo(Duration.ofMillis(600));
        assertThat(mapped.getNotFoundMaxBackoff()).isEqualTo(Duration.ofSeconds(3));
        assertThat(mapped.getNotFoundMaxAttempts()).isEqualTo(4);
        assertThat(mapped.isPerDestinationMetrics()).isTrue();
    }

    @Test
    void omittedWriterOptionsPreserveEveryDataStreamDefault() {
        assertThat(runtimeSink(minimalOptions()).getConfig().getWriterOptions())
                .isEqualTo(CloudTasksWriterOptions.builder().build());
    }

    @Test
    void mapsCredentialsAndEmulatorModesSeparately() {
        Map<String, String> credentials = minimalOptions();
        credentials.put("service-account-key-file", "/var/run/secrets/gcp/key.json");
        Map<String, String> emulator = minimalOptions();
        emulator.put("emulator-endpoint", "localhost:8123");

        assertThat(runtimeSink(credentials).getConfig().getServiceAccountKeyFile())
                .isEqualTo("/var/run/secrets/gcp/key.json");
        assertThat(runtimeSink(emulator).getConfig().getEmulatorEndpoint()).isNotNull();
    }

    @Test
    void rejectsCredentialsAlongsideAnEmulator() {
        Map<String, String> options = minimalOptions();
        options.put("service-account-key-file", "/var/run/secrets/gcp/key.json");
        options.put("emulator-endpoint", "localhost:8123");

        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("cannot be combined");
    }

    /**
     * Issue #1019: the rejection names {@code emulator-endpoint}, the key the DDL carried, rather
     * than the {@code emulatorEndpoint(...)} setter the value used to reach on its way to a client.
     *
     * <p>Asserted on the root cause. {@code FactoryUtil} dumps every {@code WITH} option into its
     * own message, so a needle of just the key would pass with the parse deleted; the root cause is
     * the {@code IllegalArgumentException} the parse throws and carries nothing else. The needle
     * also discriminates the fix, since {@code emulator-endpoint must be} is not a substring of
     * {@code emulatorEndpoint must be}.
     *
     * <p>Two values, not a catalogue. {@code "localhost"} exercises the shape, and {@code ""} the
     * one thing that is this layer's rather than the parser's: whether an option written {@code ''}
     * arrives as present-and-empty rather than absent, so the check sees it at all. The rejection
     * set itself belongs to {@code EmulatorEndpointTest}.
     */
    @Test
    void rejectsAMalformedEmulatorEndpoint() {
        for (String malformed : new String[] {"localhost", ""}) {
            Map<String, String> options = minimalOptions();
            options.put("emulator-endpoint", malformed);

            assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                    .as("'%s'", malformed)
                    .isInstanceOf(ValidationException.class)
                    .rootCause()
                    .hasMessage("emulator-endpoint must be host:port, was '" + malformed + "'");
        }
    }

    /**
     * Pins the endpoint parse behind every check this factory makes that refuses an option outright
     * — a DDL told to remove an option is not helped by an answer about that option's shape. The
     * option pre-empted need not be {@code emulator-endpoint} itself: an endpoint is legal under
     * either target type, and {@code validateTargetFamily} refuses <em>other</em> options.
     *
     * <p>Asserted on the root cause and paired with the negative: with the parse moved above {@code
     * validateCredentials} or {@code validateTargetFamily} the root cause becomes the {@code
     * IllegalArgumentException}, whose message these phrases do not appear in.
     *
     * <p>Green on {@code origin/main} by construction. It guards the ordering, not the fix.
     */
    @Test
    void refusesAnOptionOutrightBeforeReportingTheEndpointShape() {
        Map<String, String> credentials = minimalOptions();
        credentials.put("service-account-key-file", "/var/run/secrets/gcp/key.json");
        credentials.put("emulator-endpoint", "localhost");
        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, credentials))
                .as("an emulator endpoint beside a key file")
                .isInstanceOf(ValidationException.class)
                .rootCause()
                .hasMessageContaining("cannot be combined")
                .hasMessageNotContaining("must be host:port");

        Map<String, String> foreignFamily = minimalOptions();
        foreignFamily.put("target.type", "app-engine");
        foreignFamily.put("app-engine.relative-uri", "/tasks");
        foreignFamily.put("emulator-endpoint", "localhost");
        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, foreignFamily))
                .as("an http option under the App Engine target")
                .isInstanceOf(ValidationException.class)
                .rootCause()
                .hasMessageContaining("does not belong to target.type")
                .hasMessageNotContaining("must be host:port");
    }

    @Test
    void rejectsAChannelPoolAlongsideAnEmulator() {
        Map<String, String> options = minimalOptions();
        options.put("sink.channel-pool-size", "8");
        options.put("emulator-endpoint", "localhost:8123");

        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("cannot be combined");
    }

    @Test
    void rejectsOidcAndOAuthTogether() {
        Map<String, String> options = minimalOptions();
        options.put("http.oidc.service-account-email", "oidc@example.iam.gserviceaccount.com");
        options.put("http.oauth.service-account-email", "oauth@example.iam.gserviceaccount.com");

        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("OIDC and OAuth tokens");
    }

    @ParameterizedTest(name = "{0} requires {1}")
    @CsvSource({
        "http.oidc.audience, http.oidc.service-account-email, https://service.example",
        "http.oauth.scope, http.oauth.service-account-email, https://www.googleapis.com/auth/cloud-platform"
    })
    void rejectsAChildTokenOptionWithoutItsServiceAccount(
            String child, String parent, String value) {
        Map<String, String> options = minimalOptions();
        options.put(child, value);

        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("Option '" + child + "' requires option '" + parent + "'");
    }

    // The two malformed shapes both start http(s):// and fail the URI parse; a scheme Cloud Tasks
    // cannot call at all is rejected one step earlier, which nothing reached before.
    @ParameterizedTest(name = "http.url={0}")
    @ValueSource(strings = {"https://", "https://example.com/bad path", "ftp://example.com/tasks"})
    void rejectsAMalformedAbsoluteHttpUrl(String url) {
        Map<String, String> options = minimalOptions();
        options.put("http.url", url);

        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("http.url")
                .hasStackTraceContaining("absolute http:// or https:// URL");
    }

    // Both spellings of "no method" parse: Flink matches an enum constant by name, and UNRECOGNIZED
    // is protobuf's synthetic constant, so each reaches the connector as a target with no verb.
    @ParameterizedTest(name = "{0}={1}")
    @CsvSource({
        "http.method, HTTP_METHOD_UNSPECIFIED",
        "http.method, UNRECOGNIZED",
        "app-engine.method, HTTP_METHOD_UNSPECIFIED",
        "app-engine.method, UNRECOGNIZED"
    })
    void rejectsAMethodThatNamesNoVerb(String option, String value) {
        Map<String, String> options =
                option.startsWith("app-engine.") ? appEngineOptions() : minimalOptions();
        options.put(option, value);

        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                // The key too, so a failure says which target's copy of the guard fired; the two
                // specs carry one each.
                .hasStackTraceContaining("Option '" + option + "' must be a concrete HTTP method.");
    }

    // The blank check is per option rather than shared, so each of the four token options needs its
    // own case: a blank one would otherwise reach the service as an empty audience or scope.
    @ParameterizedTest(name = "{0}=''")
    @ValueSource(
            strings = {
                "http.oidc.service-account-email",
                "http.oidc.audience",
                "http.oauth.service-account-email",
                "http.oauth.scope"
            })
    void rejectsABlankTokenOption(String option) {
        Map<String, String> options = minimalOptions();
        options.put(option, "");

        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("Option '" + option + "' must not be blank.");
    }

    @Test
    void rejectsMixedHeaderMapSyntax() {
        Map<String, String> options = minimalOptions();
        options.put("http.headers", "X-One:1");
        options.put("http.headers.X-Two", "2");

        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("either packed map syntax or prefixed entries");
    }

    @Test
    void rejectsMixedAppEngineHeaderMapSyntax() {
        Map<String, String> options = appEngineOptions();
        options.put("app-engine.headers", "X-One:1");
        options.put("app-engine.headers.X-Two", "2");

        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("either packed map syntax or prefixed entries");
    }

    @ParameterizedTest(name = "target.type={0} rejects {1}")
    @CsvSource({
        "http, app-engine.relative-uri, /tasks",
        "app-engine, http.url, https://example.com"
    })
    void rejectsOptionsFromTheOtherTargetFamily(String targetType, String option, String value) {
        Map<String, String> options = minimalOptions();
        options.put("target.type", targetType);
        if ("app-engine".equals(targetType)) {
            options.remove("http.url");
            options.put("app-engine.relative-uri", "/tasks");
        }
        options.put(option, value);

        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("does not belong to target.type");
    }

    @ParameterizedTest(name = "app-engine.relative-uri={0}")
    @ValueSource(strings = {"tasks", "/bad path", "/tasks#fragment"})
    void rejectsMalformedFixedAppEngineRelativeUris(String relativeUri) {
        Map<String, String> options = appEngineOptions();
        options.put("app-engine.relative-uri", relativeUri);

        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("app-engine.relative-uri");
    }

    @Test
    void rejectsReservedFixedAppEngineHeaders() {
        Map<String, String> options = appEngineOptions();
        options.put("app-engine.headers.X-AppEngine-QueueName", "orders");

        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("set by Cloud Tasks and cannot be overridden");
    }

    @Test
    void rejectsCaseInsensitiveDuplicateFixedHeaders() {
        Map<String, String> options = minimalOptions();
        options.put("http.headers.X-Request-Id", "first");
        options.put("http.headers.x-request-id", "second");

        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("duplicate header names")
                .hasStackTraceContaining("case-insensitive");
    }

    @Test
    void rejectsABlankFixedHeaderName() {
        Configuration config = new Configuration();
        config.set(
                CloudTasksConnectorOptions.HTTP_HEADERS,
                java.util.Collections.singletonMap(" ", "value"));

        assertThatThrownBy(() -> HttpTargetSpec.from(config))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("blank header name");
    }

    // Prefixed DDL cannot spell a blank name, so this goes through the spec directly, as the HTTP
    // case above does. The App Engine target has its own copy of the header loop, and each of the
    // three rejections below was unexercised on that copy while the HTTP twin was covered.
    @Test
    void rejectsABlankFixedAppEngineHeaderName() {
        Configuration config = new Configuration();
        config.set(CloudTasksConnectorOptions.APP_ENGINE_RELATIVE_URI, "/tasks/default");
        config.set(
                CloudTasksConnectorOptions.APP_ENGINE_HEADERS,
                java.util.Collections.singletonMap(" ", "value"));

        assertThatThrownBy(() -> AppEngineTargetSpec.from(config, null))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("blank header name");
    }

    @Test
    void rejectsCaseInsensitiveDuplicateFixedAppEngineHeaders() {
        Map<String, String> options = appEngineOptions();
        options.put("app-engine.headers.X-Request-Id", "first");
        options.put("app-engine.headers.x-request-id", "second");

        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("duplicate header names")
                .hasStackTraceContaining("case-insensitive");
    }

    @Test
    void rejectsAFixedAppEngineContentTypeThatConflictsWithTheFormFormat() {
        Map<String, String> options = appEngineFormOptions();
        options.put(
                "app-engine.headers.content-type",
                "application/x-www-form-urlencoded; charset=UTF-8");

        assertThatThrownBy(() -> FactoryMocks.createTableSink(FORM_SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("conflicts with the body format's Content-Type")
                .hasStackTraceContaining("application/x-www-form-urlencoded; charset=UTF-8");
    }

    // The accepting side of the same branch, asserted on the request rather than on "did not
    // throw": an unrelated fixed header has to survive the conflict check *and* still be emitted,
    // which a mutant dropping the loop's put would otherwise pass.
    @Test
    void keepsAnUnrelatedFixedAppEngineHeaderBesideTheFormFormat() throws Exception {
        Map<String, String> options = appEngineFormOptions();
        options.put("app-engine.headers.X-Origin", "table");

        Task task =
                serialize(FORM_SCHEMA, options, GenericRowData.of(StringData.fromString("Alice")));

        assertThat(task.getAppEngineHttpRequest().getHeadersMap())
                .containsEntry("X-Origin", "table")
                .containsEntry("Content-Type", FormUrlEncodedFormatFactory.CONTENT_TYPE);
    }

    // The App Engine twin of acceptsAndCanonicalizesAMatchingFixedContentType: a matching header is
    // dropped in favour of the format's own canonical spelling, so the request carries one
    // Content-Type rather than the operator's casing and padding.
    @Test
    void acceptsAndCanonicalizesAMatchingFixedAppEngineContentType() throws Exception {
        Map<String, String> options = appEngineFormOptions();
        options.put("app-engine.headers.content-type", " APPLICATION/X-WWW-FORM-URLENCODED ");

        Task task =
                serialize(FORM_SCHEMA, options, GenericRowData.of(StringData.fromString("Alice")));

        assertThat(task.getAppEngineHttpRequest().getHeadersMap())
                .containsOnly(
                        org.assertj.core.api.Assertions.entry(
                                "Content-Type", FormUrlEncodedFormatFactory.CONTENT_TYPE));
    }

    @Test
    void requiresAUrlBeforeBuildingTheRuntimeWhenNoMetadataWasSelected() {
        Map<String, String> options = minimalOptions();
        options.remove("http.url");
        DynamicTableSink sink = FactoryMocks.createTableSink(SCHEMA, options);

        assertThatThrownBy(() -> sink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("http.url")
                .hasMessageContaining("url");
    }

    @Test
    void rejectsAnUnknownOption() {
        Map<String, String> options = minimalOptions();
        options.put("http.multipart.boundary", "x");

        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("http.multipart.boundary");
    }

    @Test
    void namesTheOptionKeyWhenAWriterValueIsRejected() {
        Map<String, String> options = minimalOptions();
        options.put("sink.max-in-flight-tasks", "0");

        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("Option 'sink.max-in-flight-tasks' is invalid")
                .hasStackTraceContaining("maxInFlightTasks must be positive");
    }
}
