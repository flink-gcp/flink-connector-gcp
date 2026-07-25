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

import org.apache.flink.api.common.serialization.SimpleStringSchema;

import com.google.cloud.tasks.v2.HttpMethod;
import com.google.cloud.tasks.v2.HttpRequest;
import com.google.cloud.tasks.v2.Task;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link HttpTargetSerializationSchema}. */
class HttpTargetSerializationSchemaTest {

    private static final String URL = "https://api.example.com/v1/orders";

    @Test
    void producesAPostTaskWithTheSerializedBodyAndNoName() throws Exception {
        Task task = schema().serialize("order-1");

        HttpRequest request = task.getHttpRequest();
        assertThat(request.getUrl()).isEqualTo(URL);
        assertThat(request.getHttpMethod()).isEqualTo(HttpMethod.POST);
        assertThat(request.getBody().toStringUtf8()).isEqualTo("order-1");
        assertThat(request.getHeadersMap()).isEmpty();
        assertThat(request.hasOidcToken()).isFalse();
        assertThat(request.hasOauthToken()).isFalse();
        // Naming belongs to the sink, which hashes the extracted key.
        assertThat(task.getName()).isEmpty();
    }

    @Test
    void appliesTheConfiguredMethodAndHeaders() throws Exception {
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("X-Trace", "abc");

        Task task =
                schema().withMethod(HttpMethod.PUT)
                        .withHeaders(element -> headers)
                        .serialize("order-1");

        assertThat(task.getHttpRequest().getHttpMethod()).isEqualTo(HttpMethod.PUT);
        assertThat(task.getHttpRequest().getHeadersMap()).containsAllEntriesOf(headers);
    }

    @Test
    void toleratesAnEmptyOrAbsentHeaderExtraction() throws Exception {
        assertThat(
                        schema().withHeaders(element -> null)
                                .serialize("x")
                                .getHttpRequest()
                                .getHeadersMap())
                .isEmpty();
        assertThat(
                        schema().withHeaders(element -> Collections.emptyMap())
                                .serialize("x")
                                .getHttpRequest()
                                .getHeadersMap())
                .isEmpty();
    }

    @Test
    void rejectsNullHeaderKeysAndValues() {
        Map<String, String> nullValue = new HashMap<>();
        nullValue.put("X-Trace", null);

        assertThatThrownBy(() -> schema().withHeaders(element -> nullValue).serialize("x"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("X-Trace");
    }

    @Test
    void resolvesTheUrlPerRecord() throws Exception {
        HttpTargetSerializationSchema<String> schema =
                schema().withUrl(element -> URL + "/" + element);

        assertThat(schema.serialize("order-1").getHttpRequest().getUrl())
                .isEqualTo(URL + "/order-1");
    }

    @Test
    void rejectsAnExtractedUrlThatIsNotAbsoluteHttp() {
        HttpTargetSerializationSchema<String> schema =
                schema().withUrl(element -> "/v1/orders/" + element);

        assertThatThrownBy(() -> schema.serialize("order-1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("absolute http");
    }

    @Test
    void appliesAnOidcToken() throws Exception {
        Task task =
                schema().withOidcToken("dispatcher@my-project.iam.gserviceaccount.com")
                        .serialize("order-1");

        assertThat(task.getHttpRequest().getOidcToken().getServiceAccountEmail())
                .isEqualTo("dispatcher@my-project.iam.gserviceaccount.com");
        assertThat(task.getHttpRequest().getOidcToken().getAudience()).isEmpty();
    }

    @Test
    void appliesAnOAuthTokenWithItsScope() throws Exception {
        Task task =
                schema().withOAuthToken("caller@my-project.iam.gserviceaccount.com", "scope")
                        .serialize("order-1");

        assertThat(task.getHttpRequest().getOauthToken().getServiceAccountEmail())
                .isEqualTo("caller@my-project.iam.gserviceaccount.com");
        assertThat(task.getHttpRequest().getOauthToken().getScope()).isEqualTo("scope");
    }

    @Test
    void rejectsSettingBothTokenTypes() {
        // The underlying field is a proto oneof, and the target decides which token applies.
        assertThatThrownBy(() -> schema().withOidcToken("a@b.com").withOAuthToken("a@b.com"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("oneof");
        assertThatThrownBy(() -> schema().withOAuthToken("a@b.com").withOidcToken("a@b.com"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("oneof");
    }

    @Test
    void layeringReturnsANewSchemaAndLeavesTheOriginalAlone() throws Exception {
        HttpTargetSerializationSchema<String> base = schema();
        HttpTargetSerializationSchema<String> layered = base.withMethod(HttpMethod.GET);

        assertThat(layered).isNotSameAs(base);
        assertThat(base.serialize("x").getHttpRequest().getHttpMethod()).isEqualTo(HttpMethod.POST);
        assertThat(layered.serialize("x").getHttpRequest().getHttpMethod())
                .isEqualTo(HttpMethod.GET);
    }

    @Test
    void rejectsAMalformedTargetUrl() {
        assertThatThrownBy(() -> CloudTasksSerializationSchema.httpTarget("api.example.com"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CloudTasksSerializationSchema.httpTarget("  "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsAMissingBodySchema() {
        assertThatThrownBy(() -> CloudTasksSerializationSchema.httpTarget(URL).withBody(null))
                .isInstanceOf(NullPointerException.class);
    }

    private static HttpTargetSerializationSchema<String> schema() {
        return CloudTasksSerializationSchema.httpTarget(URL).withBody(new SimpleStringSchema());
    }
}
