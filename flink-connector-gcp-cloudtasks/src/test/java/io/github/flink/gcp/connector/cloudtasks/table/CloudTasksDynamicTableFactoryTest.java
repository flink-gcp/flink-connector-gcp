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
        options.put("sink.retry.initial-backoff", "200 ms");
        options.put("sink.retry.max-backoff", "4 s");
        options.put("sink.retry.max-attempts", "5");
        options.put("sink.not-found-retry.initial-backoff", "600 ms");
        options.put("sink.not-found-retry.max-backoff", "3 s");
        options.put("sink.not-found-retry.max-attempts", "4");
        options.put("sink.metrics.per-destination", "true");

        CloudTasksWriterOptions mapped = runtimeSink(options).getConfig().getWriterOptions();

        assertThat(mapped.getMaxInFlightTasks()).isEqualTo(17);
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

    @ParameterizedTest(name = "http.url={0}")
    @ValueSource(strings = {"https://", "https://example.com/bad path"})
    void rejectsAMalformedAbsoluteHttpUrl(String url) {
        Map<String, String> options = minimalOptions();
        options.put("http.url", url);

        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("http.url")
                .hasStackTraceContaining("absolute http:// or https:// URL");
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
}
