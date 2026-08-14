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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import com.google.cloud.tasks.v2.HttpMethod;
import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.cloudtasks.table.CloudTasksConnectorOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RowDataSerializationSchema}. */
class RowDataSerializationSchemaTest {

    private static final class RecordingEncoder implements SerializationSchema<RowData> {

        private static final long serialVersionUID = 1L;

        private int calls;
        private String seen;

        @Override
        public byte[] serialize(RowData element) {
            calls++;
            StringBuilder value = new StringBuilder();
            for (int i = 0; i < element.getArity(); i++) {
                if (i > 0) {
                    value.append('|');
                }
                value.append(element.getString(i));
            }
            seen = value.toString();
            return seen.getBytes(StandardCharsets.UTF_8);
        }
    }

    private static HttpTargetSpec target(Configuration config) {
        return HttpTargetSpec.from(config);
    }

    private static Configuration target(String url) {
        Configuration config = new Configuration();
        config.set(CloudTasksConnectorOptions.HTTP_URL, url);
        return config;
    }

    private static AppEngineTargetSpec appEngineTarget(
            Configuration config, String bodyContentType) {
        return AppEngineTargetSpec.from(config, bodyContentType);
    }

    private static Configuration appEngineTarget(String relativeUri) {
        Configuration config = new Configuration();
        config.set(CloudTasksConnectorOptions.APP_ENGINE_RELATIVE_URI, relativeUri);
        return config;
    }

    private static StringData str(String value) {
        return value == null ? null : StringData.fromString(value);
    }

    private static GenericMapData headers(Map<String, String> headers) {
        Map<StringData, StringData> data = new LinkedHashMap<>();
        headers.forEach((key, value) -> data.put(str(key), str(value)));
        return new GenericMapData(data);
    }

    @Test
    void metadataOverridesTheFixedRequestAndTheFormatSeesOnlyPhysicalColumns() throws Exception {
        Configuration config = target("https://fixed.example/tasks");
        config.set(CloudTasksConnectorOptions.HTTP_METHOD, HttpMethod.PUT);
        config.set(
                CloudTasksConnectorOptions.HTTP_HEADERS,
                java.util.Collections.singletonMap("Content-Type", "application/json"));
        config.set(
                CloudTasksConnectorOptions.HTTP_OIDC_SERVICE_ACCOUNT_EMAIL,
                "dispatcher@example.iam.gserviceaccount.com");
        config.set(CloudTasksConnectorOptions.HTTP_OIDC_AUDIENCE, "https://service.example");
        RecordingEncoder encoder = new RecordingEncoder();
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(
                        encoder,
                        2,
                        new WritableMetadata[] {
                            WritableMetadata.URL,
                            WritableMetadata.HTTP_METHOD,
                            WritableMetadata.HEADERS,
                            WritableMetadata.SCHEDULE_TIME,
                            WritableMetadata.TASK_ID
                        },
                        target(config));
        Instant scheduled = Instant.parse("2026-08-13T01:02:03.123456Z");

        Task task =
                schema.serialize(
                        GenericRowData.of(
                                str("order-1"),
                                str("ready"),
                                str("https://row.example/orders/1"),
                                str("patch"),
                                headers(
                                        java.util.Collections.singletonMap(
                                                "content-type", "application/merge-patch+json")),
                                TimestampData.fromInstant(scheduled),
                                str("dedupe-1")));

        assertThat(encoder.calls).isOne();
        assertThat(encoder.seen).isEqualTo("order-1|ready");
        assertThat(task.getHttpRequest().getUrl()).isEqualTo("https://row.example/orders/1");
        assertThat(task.getHttpRequest().getHttpMethod()).isEqualTo(HttpMethod.PATCH);
        assertThat(task.getHttpRequest().getBody().toStringUtf8()).isEqualTo("order-1|ready");
        assertThat(task.getHttpRequest().getHeadersMap())
                .containsOnly(
                        org.assertj.core.api.Assertions.entry(
                                "content-type", "application/merge-patch+json"));
        assertThat(task.getHttpRequest().getOidcToken().getServiceAccountEmail())
                .isEqualTo("dispatcher@example.iam.gserviceaccount.com");
        assertThat(task.getHttpRequest().getOidcToken().getAudience())
                .isEqualTo("https://service.example");
        assertThat(task.getScheduleTime().getSeconds()).isEqualTo(scheduled.getEpochSecond());
        assertThat(task.getScheduleTime().getNanos()).isEqualTo(scheduled.getNano());
        assertThat(task.getName()).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(
            value = HttpMethod.class,
            names = {"GET", "HEAD", "DELETE", "OPTIONS"})
    void bodylessMethodDoesNotInvokeTheFormat(HttpMethod method) throws Exception {
        RecordingEncoder encoder = new RecordingEncoder();
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(
                        encoder,
                        1,
                        new WritableMetadata[] {WritableMetadata.HTTP_METHOD},
                        target(target("https://example.com/search")));

        Task task = schema.serialize(GenericRowData.of(str("ignored"), str(method.name())));

        assertThat(encoder.calls).isZero();
        assertThat(task.getHttpRequest().getHttpMethod()).isEqualTo(method);
        assertThat(task.getHttpRequest().getBody()).isEmpty();
    }

    @Test
    void bodylessMethodDoesNotCarryTheFormatsContentType() throws Exception {
        RecordingEncoder encoder = new RecordingEncoder();
        Configuration config = target("https://example.com/search");
        config.set(CloudTasksConnectorOptions.HTTP_METHOD, HttpMethod.GET);
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(
                        encoder,
                        1,
                        new WritableMetadata[0],
                        HttpTargetSpec.from(config, "application/x-www-form-urlencoded"));

        Task task = schema.serialize(GenericRowData.of(str("ignored")));

        assertThat(encoder.calls).isZero();
        assertThat(task.getHttpRequest().getHeadersMap()).doesNotContainKey("Content-Type");
    }

    @ParameterizedTest
    @EnumSource(
            value = HttpMethod.class,
            names = {"POST", "PUT", "PATCH"})
    void bodyMethodInvokesTheFormat(HttpMethod method) throws Exception {
        RecordingEncoder encoder = new RecordingEncoder();
        Configuration config = target("https://example.com/tasks");
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(
                        encoder,
                        1,
                        new WritableMetadata[] {WritableMetadata.HTTP_METHOD},
                        HttpTargetSpec.from(config, "application/x-www-form-urlencoded"));

        Task task = schema.serialize(GenericRowData.of(str("payload"), str(method.name())));

        assertThat(encoder.calls).isOne();
        assertThat(task.getHttpRequest().getHttpMethod()).isEqualTo(method);
        assertThat(task.getHttpRequest().getBody().toStringUtf8()).isEqualTo("payload");
        assertThat(task.getHttpRequest().getHeadersMap())
                .containsEntry("Content-Type", "application/x-www-form-urlencoded");
    }

    @Test
    void nullFromTheFlinkFormatIsASerializationFailure() {
        SerializationSchema<RowData> nullEncoder = element -> null;
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(
                        nullEncoder,
                        1,
                        new WritableMetadata[0],
                        target(target("https://example.com/tasks")));

        assertThatThrownBy(() -> schema.serialize(GenericRowData.of(str("payload"))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(nullEncoder.getClass().getName())
                .hasMessageContaining("returned null")
                .hasMessageContaining("serialization failure");
    }

    @Test
    void appEngineMetadataOverridesFixedValuesAndTheFormatSeesOnlyPhysicalColumns()
            throws Exception {
        Configuration config = appEngineTarget("/fixed");
        config.set(CloudTasksConnectorOptions.APP_ENGINE_METHOD, HttpMethod.POST);
        config.set(
                CloudTasksConnectorOptions.APP_ENGINE_HEADERS,
                java.util.Collections.singletonMap("X-Origin", "fixed"));
        config.set(CloudTasksConnectorOptions.APP_ENGINE_SERVICE, "fixed-service");
        config.set(CloudTasksConnectorOptions.APP_ENGINE_VERSION, "fixed-version");
        config.set(CloudTasksConnectorOptions.APP_ENGINE_INSTANCE, "fixed-instance");
        RecordingEncoder encoder = new RecordingEncoder();
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(
                        encoder,
                        2,
                        new WritableMetadata[] {
                            WritableMetadata.RELATIVE_URI,
                            WritableMetadata.HTTP_METHOD,
                            WritableMetadata.HEADERS,
                            WritableMetadata.APP_ENGINE_SERVICE,
                            WritableMetadata.APP_ENGINE_VERSION,
                            WritableMetadata.APP_ENGINE_INSTANCE,
                            WritableMetadata.SCHEDULE_TIME,
                            WritableMetadata.TASK_ID
                        },
                        appEngineTarget(config, "application/x-www-form-urlencoded"));
        Instant scheduled = Instant.parse("2026-08-14T01:02:03.123456Z");

        Task task =
                schema.serialize(
                        GenericRowData.of(
                                str("order-1"),
                                str("ready"),
                                str("/row/tasks/1?source=table"),
                                str("put"),
                                headers(java.util.Collections.singletonMap("x-origin", "row")),
                                str("row-service"),
                                null,
                                str(""),
                                TimestampData.fromInstant(scheduled),
                                str("dedupe-1")));

        assertThat(encoder.calls).isOne();
        assertThat(encoder.seen).isEqualTo("order-1|ready");
        assertThat(task.getMessageTypeCase())
                .isEqualTo(Task.MessageTypeCase.APP_ENGINE_HTTP_REQUEST);
        assertThat(task.getAppEngineHttpRequest().getRelativeUri())
                .isEqualTo("/row/tasks/1?source=table");
        assertThat(task.getAppEngineHttpRequest().getHttpMethod()).isEqualTo(HttpMethod.PUT);
        assertThat(task.getAppEngineHttpRequest().getBody().toStringUtf8())
                .isEqualTo("order-1|ready");
        assertThat(task.getAppEngineHttpRequest().getHeadersMap())
                .containsOnly(
                        org.assertj.core.api.Assertions.entry("x-origin", "row"),
                        org.assertj.core.api.Assertions.entry(
                                "Content-Type", "application/x-www-form-urlencoded"));
        assertThat(task.getAppEngineHttpRequest().getAppEngineRouting().getService())
                .isEqualTo("row-service");
        assertThat(task.getAppEngineHttpRequest().getAppEngineRouting().getVersion())
                .isEqualTo("fixed-version");
        assertThat(task.getAppEngineHttpRequest().getAppEngineRouting().getInstance()).isEmpty();
        assertThat(task.getScheduleTime().getSeconds()).isEqualTo(scheduled.getEpochSecond());
        assertThat(task.getScheduleTime().getNanos()).isEqualTo(scheduled.getNano());
        assertThat(task.getName()).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(
            value = HttpMethod.class,
            names = {"POST", "PUT"})
    void appEnginePostAndPutCarryTheEncodedBody(HttpMethod method) throws Exception {
        Configuration config = appEngineTarget("/tasks");
        config.set(CloudTasksConnectorOptions.APP_ENGINE_METHOD, method);
        RecordingEncoder encoder = new RecordingEncoder();
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(
                        encoder, 1, new WritableMetadata[0], appEngineTarget(config, null));

        Task task = schema.serialize(GenericRowData.of(str("payload")));

        assertThat(encoder.calls).isOne();
        assertThat(task.getAppEngineHttpRequest().getBody().toStringUtf8()).isEqualTo("payload");
    }

    @ParameterizedTest
    @EnumSource(
            value = HttpMethod.class,
            names = {"GET", "HEAD", "DELETE", "PATCH", "OPTIONS"})
    void appEngineBodylessMethodsDoNotInvokeTheFormat(HttpMethod method) throws Exception {
        Configuration config = appEngineTarget("/tasks");
        config.set(CloudTasksConnectorOptions.APP_ENGINE_METHOD, method);
        RecordingEncoder encoder = new RecordingEncoder();
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(
                        encoder,
                        1,
                        new WritableMetadata[0],
                        appEngineTarget(config, "application/x-www-form-urlencoded"));

        Task task = schema.serialize(GenericRowData.of(str("ignored")));

        assertThat(encoder.calls).isZero();
        assertThat(task.getAppEngineHttpRequest().getBody()).isEmpty();
        assertThat(task.getAppEngineHttpRequest().getHeadersMap())
                .doesNotContainKey("Content-Type");
    }

    @Test
    void appEngineMetadataRejectsReservedHeaders() {
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(
                        new RecordingEncoder(),
                        1,
                        new WritableMetadata[] {WritableMetadata.HEADERS},
                        appEngineTarget(appEngineTarget("/tasks"), null));

        assertThatThrownBy(
                        () ->
                                schema.serialize(
                                        GenericRowData.of(
                                                str("payload"),
                                                headers(
                                                        java.util.Collections.singletonMap(
                                                                "X-Google-Internal", "value")))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("set by Cloud Tasks and cannot be overridden");
    }
}
