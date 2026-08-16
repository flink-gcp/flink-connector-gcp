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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import com.google.cloud.tasks.v2.HttpMethod;
import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.cloudtasks.table.CloudTasksConnectorOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RowDataToTaskMetadataConverter}. */
class RowDataToTaskMetadataConverterTest {

    private static HttpTargetSpec target(Configuration config) {
        return HttpTargetSpec.from(config);
    }

    private static Configuration target(String url) {
        Configuration config = new Configuration();
        config.set(CloudTasksConnectorOptions.HTTP_URL, url);
        return config;
    }

    private static StringData str(String value) {
        return value == null ? null : StringData.fromString(value);
    }

    private static Task convert(Configuration config, WritableMetadata[] metadata, RowData row)
            throws IOException {
        return convert(config, metadata, row, null);
    }

    private static Task convert(
            Configuration config, WritableMetadata[] metadata, RowData row, String bodyContentType)
            throws IOException {
        return new RowDataToTaskMetadataConverter(
                        1, metadata, HttpTargetSpec.from(config, bodyContentType))
                .convert(row)
                .build();
    }

    @Test
    void nullMetadataFallsBackToTheFixedRequest() throws Exception {
        Configuration config = target("https://fixed.example/tasks");
        config.set(CloudTasksConnectorOptions.HTTP_METHOD, HttpMethod.PUT);
        config.set(
                CloudTasksConnectorOptions.HTTP_HEADERS,
                java.util.Collections.singletonMap("X-Fixed", "yes"));

        Task task =
                convert(
                        config,
                        new WritableMetadata[] {
                            WritableMetadata.URL,
                            WritableMetadata.HTTP_METHOD,
                            WritableMetadata.HEADERS,
                            WritableMetadata.SCHEDULE_TIME
                        },
                        GenericRowData.of(str("body"), null, null, null, null));

        assertThat(task.getHttpRequest().getUrl()).isEqualTo("https://fixed.example/tasks");
        assertThat(task.getHttpRequest().getHttpMethod()).isEqualTo(HttpMethod.PUT);
        assertThat(task.getHttpRequest().getHeadersMap())
                .containsOnly(org.assertj.core.api.Assertions.entry("X-Fixed", "yes"));
        assertThat(task.hasScheduleTime()).isFalse();
        assertThat(task.getHttpRequest().getBody()).isEmpty();
        assertThat(task.getName()).isEmpty();
    }

    @Test
    void configuresAnOAuthToken() throws Exception {
        Configuration config = target("https://storage.googleapis.com/upload");
        config.set(
                CloudTasksConnectorOptions.HTTP_OAUTH_SERVICE_ACCOUNT_EMAIL,
                "dispatcher@example.iam.gserviceaccount.com");
        config.set(CloudTasksConnectorOptions.HTTP_OAUTH_SCOPE, "scope-1");

        Task task = convert(config, new WritableMetadata[0], GenericRowData.of(str("body")));

        assertThat(task.getHttpRequest().getOauthToken().getServiceAccountEmail())
                .isEqualTo("dispatcher@example.iam.gserviceaccount.com");
        assertThat(task.getHttpRequest().getOauthToken().getScope()).isEqualTo("scope-1");
    }

    @Test
    void rejectsAnUnsupportedMethodFromMetadata() {
        Configuration config = target("https://example.com/tasks");

        assertThatThrownBy(
                        () ->
                                convert(
                                        config,
                                        new WritableMetadata[] {WritableMetadata.HTTP_METHOD},
                                        GenericRowData.of(str("body"), str("TRACE"))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("TRACE")
                .hasMessageContaining("POST, GET, HEAD, PUT, DELETE, PATCH or OPTIONS");
    }

    @ParameterizedTest(name = "url={0}")
    @ValueSource(strings = {"https://", "https://example.com/bad path"})
    void rejectsAMalformedUrlFromMetadata(String url) {
        Configuration config = target("https://example.com/tasks");

        assertThatThrownBy(
                        () ->
                                convert(
                                        config,
                                        new WritableMetadata[] {WritableMetadata.URL},
                                        GenericRowData.of(str("body"), str(url))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("absolute http:// or https:// URL")
                .hasMessageContaining(url);
    }

    @Test
    void rejectsANullHeaderEntry() {
        Map<StringData, StringData> data = new LinkedHashMap<>();
        data.put(str("X-Key"), null);
        Configuration config = target("https://example.com/tasks");

        assertThatThrownBy(
                        () ->
                                convert(
                                        config,
                                        new WritableMetadata[] {WritableMetadata.HEADERS},
                                        GenericRowData.of(str("body"), new GenericMapData(data))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("null value");
    }

    @Test
    void rejectsABlankHeaderName() {
        Configuration config = target("https://example.com/tasks");

        assertThatThrownBy(
                        () ->
                                convert(
                                        config,
                                        new WritableMetadata[] {WritableMetadata.HEADERS},
                                        GenericRowData.of(
                                                str("body"),
                                                new GenericMapData(
                                                        java.util.Collections.singletonMap(
                                                                str(" "), str("value"))))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("blank name");
    }

    @Test
    void rejectsCaseInsensitiveDuplicateRowHeaders() {
        Map<StringData, StringData> data = new LinkedHashMap<>();
        data.put(str("X-Request-Id"), str("first"));
        data.put(str("x-request-id"), str("second"));
        Configuration config = target("https://example.com/tasks");

        assertThatThrownBy(
                        () ->
                                convert(
                                        config,
                                        new WritableMetadata[] {WritableMetadata.HEADERS},
                                        GenericRowData.of(str("body"), new GenericMapData(data))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("case-insensitive duplicate")
                .hasMessageContaining("x-request-id");
    }

    @Test
    void rejectsMetadataContentTypeThatConflictsWithTheBodyFormat() {
        Configuration config = target("https://example.com/tasks");

        assertThatThrownBy(
                        () ->
                                convert(
                                        config,
                                        new WritableMetadata[] {WritableMetadata.HEADERS},
                                        GenericRowData.of(
                                                str("body"),
                                                new GenericMapData(
                                                        java.util.Collections.singletonMap(
                                                                str("content-TYPE"),
                                                                str(
                                                                        "application/x-www-form-urlencoded;"
                                                                                + " charset=UTF-8")))),
                                        "application/x-www-form-urlencoded"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(
                        "metadata contains Content-Type"
                                + " 'application/x-www-form-urlencoded; charset=UTF-8'")
                .hasMessageContaining("conflicts with the body format's Content-Type");
    }

    @Test
    void leavesAMatchingMetadataContentTypeForTheBodyFormatToCanonicalize() throws Exception {
        Configuration config = target("https://example.com/tasks");

        Task task =
                convert(
                        config,
                        new WritableMetadata[] {WritableMetadata.HEADERS},
                        GenericRowData.of(
                                str("body"),
                                new GenericMapData(
                                        java.util.Collections.singletonMap(
                                                str("content-type"),
                                                str(" APPLICATION/X-WWW-FORM-URLENCODED ")))),
                        "application/x-www-form-urlencoded");

        assertThat(task.getHttpRequest().getHeadersMap()).isEmpty();
    }
}
