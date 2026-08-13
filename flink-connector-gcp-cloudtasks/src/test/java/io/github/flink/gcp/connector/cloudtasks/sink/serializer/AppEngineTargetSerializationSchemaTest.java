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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.tasks.v2.AppEngineHttpRequest;
import com.google.cloud.tasks.v2.AppEngineRouting;
import com.google.cloud.tasks.v2.HttpMethod;
import com.google.cloud.tasks.v2.Task;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AppEngineTargetSerializationSchema}. */
class AppEngineTargetSerializationSchemaTest {

    private static final String RELATIVE_URI = "/tasks/orders";

    @Test
    void producesOnlyAnAppEnginePostRequestWithBodyAndNoName() throws Exception {
        Task task = schema().serialize("order-1");

        assertThat(task.getMessageTypeCase())
                .isEqualTo(Task.MessageTypeCase.APP_ENGINE_HTTP_REQUEST);
        assertThat(task.hasHttpRequest()).isFalse();
        assertThat(task.getName()).isEmpty();

        AppEngineHttpRequest request = task.getAppEngineHttpRequest();
        assertThat(request.getRelativeUri()).isEqualTo(RELATIVE_URI);
        assertThat(request.getHttpMethod()).isEqualTo(HttpMethod.POST);
        assertThat(request.getBody().toStringUtf8()).isEqualTo("order-1");
        assertThat(request.getHeadersMap()).isEmpty();
        assertThat(request.hasAppEngineRouting()).isFalse();
    }

    @Test
    void appliesFixedRoutingMethodAndHeaders() throws Exception {
        AppEngineRouting routing =
                AppEngineRouting.newBuilder()
                        .setService("worker")
                        .setVersion("v2")
                        .setInstance("instance-1")
                        .build();
        Map<String, String> headers = Map.of("Content-Type", "application/json");

        AppEngineHttpRequest request =
                builder()
                        .withMethod(HttpMethod.PUT)
                        .withHeaders(ignored -> headers)
                        .withRouting(routing)
                        .build()
                        .serialize("order-1")
                        .getAppEngineHttpRequest();

        assertThat(request.getHttpMethod()).isEqualTo(HttpMethod.PUT);
        assertThat(request.getHeadersMap()).containsExactlyEntriesOf(headers);
        assertThat(request.getAppEngineRouting()).isEqualTo(routing);
    }

    @Test
    void resolvesRelativeUriAndRoutingPerRecord() throws Exception {
        AppEngineTargetSerializationSchema<String> schema =
                builder()
                        .withRouting(AppEngineRouting.newBuilder().setService("fixed").build())
                        .withRelativeUri(value -> "/tasks/" + value)
                        .withRouting(
                                value ->
                                        AppEngineRouting.newBuilder()
                                                .setService("worker-" + value)
                                                .build())
                        .build();

        AppEngineHttpRequest request = schema.serialize("one").getAppEngineHttpRequest();
        assertThat(request.getRelativeUri()).isEqualTo("/tasks/one");
        assertThat(request.getAppEngineRouting().getService()).isEqualTo("worker-one");
    }

    @Test
    void theLastRoutingFormWins() throws Exception {
        AppEngineRouting fixed = AppEngineRouting.newBuilder().setService("fixed").build();
        AppEngineTargetSerializationSchema<String> schema =
                builder()
                        .withRouting(value -> AppEngineRouting.getDefaultInstance())
                        .withRouting(fixed)
                        .build();

        assertThat(schema.serialize("one").getAppEngineHttpRequest().getAppEngineRouting())
                .isEqualTo(fixed);
    }

    @Test
    void nullAndEmptyRoutingLeaveTheFieldUnset() throws Exception {
        assertThat(
                        builder()
                                .withRouting(AppEngineRouting.getDefaultInstance())
                                .build()
                                .serialize("one")
                                .getAppEngineHttpRequest()
                                .hasAppEngineRouting())
                .isFalse();
        assertThat(
                        builder()
                                .withRouting(value -> null)
                                .build()
                                .serialize("one")
                                .getAppEngineHttpRequest()
                                .hasAppEngineRouting())
                .isFalse();
        assertThat(
                        builder()
                                .withRouting(value -> AppEngineRouting.getDefaultInstance())
                                .build()
                                .serialize("one")
                                .getAppEngineHttpRequest()
                                .hasAppEngineRouting())
                .isFalse();
    }

    @Test
    void sendsBodyOnlyWithPostAndPut() throws Exception {
        assertThat(
                        builder()
                                .withMethod(HttpMethod.POST)
                                .build()
                                .serialize("post")
                                .getAppEngineHttpRequest()
                                .getBody()
                                .toStringUtf8())
                .isEqualTo("post");
        assertThat(
                        builder()
                                .withMethod(HttpMethod.PUT)
                                .build()
                                .serialize("put")
                                .getAppEngineHttpRequest()
                                .getBody()
                                .toStringUtf8())
                .isEqualTo("put");
        assertThat(
                        builder()
                                .withMethod(HttpMethod.GET)
                                .build()
                                .serialize("get")
                                .getAppEngineHttpRequest()
                                .getBody())
                .isEmpty();
        assertThat(
                        builder()
                                .withMethod(HttpMethod.PATCH)
                                .build()
                                .serialize("patch")
                                .getAppEngineHttpRequest()
                                .getBody())
                .isEmpty();
    }

    @Test
    void aBodylessMethodDoesNotInvokeTheBodySchema() throws Exception {
        AppEngineTargetSerializationSchema<String> schema =
                CloudTasksSerializationSchema.appEngineTarget(RELATIVE_URI)
                        .withBody(new NullReturningSchema())
                        .withMethod(HttpMethod.GET)
                        .build();

        assertThat(schema.serialize("order-1").getAppEngineHttpRequest().getBody()).isEmpty();
    }

    @Test
    void reportsANullBodyAsAFailureRatherThanASkip() {
        AppEngineTargetSerializationSchema<String> schema =
                CloudTasksSerializationSchema.appEngineTarget(RELATIVE_URI)
                        .withBody(new NullReturningSchema())
                        .build();

        assertThatThrownBy(() -> schema.serialize("order-1"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(NullReturningSchema.class.getName())
                .hasMessageContaining("returned null");
    }

    @Test
    void toleratesAbsentHeaders() throws Exception {
        assertThat(
                        builder()
                                .withHeaders(value -> null)
                                .build()
                                .serialize("one")
                                .getAppEngineHttpRequest()
                                .getHeadersMap())
                .isEmpty();
        assertThat(
                        builder()
                                .withHeaders(value -> Collections.emptyMap())
                                .build()
                                .serialize("one")
                                .getAppEngineHttpRequest()
                                .getHeadersMap())
                .isEmpty();
    }

    @Test
    void rejectsReservedHeadersCaseInsensitively() {
        for (String name :
                new String[] {"Host", "content-LENGTH", "X-Google-Trace", "x-appengine-task"}) {
            AppEngineTargetSerializationSchema<String> schema =
                    builder().withHeaders(value -> Map.of(name, "value")).build();

            assertThatThrownBy(() -> schema.serialize("one"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(name)
                    .hasMessageContaining("cannot be overridden");
        }
    }

    @Test
    void rejectsNullHeaderKeysAndValues() {
        Map<String, String> nullKey = new HashMap<>();
        nullKey.put(null, "value");
        Map<String, String> nullValue = new HashMap<>();
        nullValue.put("X-Trace", null);

        assertThatThrownBy(() -> builder().withHeaders(value -> nullKey).build().serialize("one"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("null key");
        assertThatThrownBy(() -> builder().withHeaders(value -> nullValue).build().serialize("one"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("X-Trace");
    }

    @Test
    void acceptsTheDocumentedRelativeUriForms() throws Exception {
        assertThat(schema("").serialize("one").getAppEngineHttpRequest().getRelativeUri())
                .isEmpty();
        assertThat(schema("/").serialize("one").getAppEngineHttpRequest().getRelativeUri())
                .isEqualTo("/");
        assertThat(
                        schema("/tasks/%E2%9C%93?source=flink&empty=")
                                .serialize("one")
                                .getAppEngineHttpRequest()
                                .getRelativeUri())
                .isEqualTo("/tasks/%E2%9C%93?source=flink&empty=");
        String maximumLength = "/" + "a".repeat(2082);
        assertThat(
                        schema(maximumLength)
                                .serialize("one")
                                .getAppEngineHttpRequest()
                                .getRelativeUri())
                .isEqualTo(maximumLength);
    }

    @Test
    void rejectsMalformedFixedRelativeUris() {
        for (String value :
                new String[] {
                    "tasks/orders",
                    "//worker.example/tasks",
                    "/tasks#fragment",
                    "/tasks with space",
                    "/tasks%"
                }) {
            assertThatThrownBy(() -> CloudTasksSerializationSchema.appEngineTarget(value))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(
                        () -> CloudTasksSerializationSchema.appEngineTarget("/" + "a".repeat(2083)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("2083");
        assertThatThrownBy(() -> CloudTasksSerializationSchema.appEngineTarget(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void validatesExtractedRelativeUrisPerRecord() {
        AppEngineTargetSerializationSchema<String> schema =
                builder().withRelativeUri(value -> value).build();

        assertThatThrownBy(() -> schema.serialize("not/absolute-path"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("extracted relative URI");
    }

    @Test
    void rejectsTheOutputOnlyRoutingHost() {
        AppEngineRouting withHost =
                AppEngineRouting.newBuilder()
                        .setService("worker")
                        .setHost("worker.example")
                        .build();

        assertThatThrownBy(() -> builder().withRouting(withHost))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("output-only");
        assertThatThrownBy(() -> builder().withRouting(value -> withHost).build().serialize("one"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("extracted routing")
                .hasMessageContaining("output-only");
    }

    @Test
    void rejectsIncompleteBuildersAndUnspecifiedMethods() {
        assertThatThrownBy(
                        () -> CloudTasksSerializationSchema.appEngineTarget(RELATIVE_URI).build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("body must be set");
        assertThatThrownBy(
                        () ->
                                CloudTasksSerializationSchema.appEngineTarget(RELATIVE_URI)
                                        .withBody(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder().withBody(new SimpleStringSchema()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already been set");
        assertThatThrownBy(
                        () ->
                                CloudTasksSerializationSchema.appEngineTarget(RELATIVE_URI)
                                        .withHeaders(ignored -> Map.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("body must be set");
        assertThatThrownBy(() -> builder().withMethod(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder().withMethod(HttpMethod.HTTP_METHOD_UNSPECIFIED))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                builder()
                                        .withRouting(
                                                (AppEngineTargetBuilder.RoutingExtractor<String>)
                                                        null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void builderMutatesInPlaceAndBuildCreatesIndependentSnapshots() throws Exception {
        AppEngineTargetBuilder<String> builder = builder();
        assertThat(builder.withMethod(HttpMethod.PUT)).isSameAs(builder);
        AppEngineTargetSerializationSchema<String> first = builder.build();

        builder.withMethod(HttpMethod.GET);
        AppEngineTargetSerializationSchema<String> second = builder.build();

        assertThat(first.serialize("one").getAppEngineHttpRequest().getHttpMethod())
                .isEqualTo(HttpMethod.PUT);
        assertThat(second.serialize("one").getAppEngineHttpRequest().getHttpMethod())
                .isEqualTo(HttpMethod.GET);
    }

    @Test
    void bodyBindingRetainsFixedSettingsConfiguredBeforeTheRecordType() throws Exception {
        AppEngineRouting routing = AppEngineRouting.newBuilder().setService("worker").build();
        AppEngineTargetBuilder<Void> untyped =
                CloudTasksSerializationSchema.appEngineTarget(RELATIVE_URI)
                        .withMethod(HttpMethod.PUT)
                        .withRouting(routing);

        AppEngineTargetSerializationSchema<String> schema =
                untyped.withBody(new SimpleStringSchema()).build();

        AppEngineHttpRequest request = schema.serialize("one").getAppEngineHttpRequest();
        assertThat(request.getHttpMethod()).isEqualTo(HttpMethod.PUT);
        assertThat(request.getAppEngineRouting()).isEqualTo(routing);
    }

    @Test
    void opensTheWrappedBodySchema() throws Exception {
        TrackingSchema body = new TrackingSchema();
        AppEngineTargetSerializationSchema<String> schema =
                CloudTasksSerializationSchema.appEngineTarget(RELATIVE_URI).withBody(body).build();

        schema.open(null);

        assertThat(body.opened).isTrue();
    }

    @Test
    void rebuildsTheTransientPrototypeAfterSerialization() throws Exception {
        AppEngineTargetSerializationSchema<String> original =
                builder()
                        .withRouting(AppEngineRouting.newBuilder().setService("worker").build())
                        .build();
        original.serialize("before");

        AppEngineTargetSerializationSchema<String> copy = InstantiationUtil.clone(original);

        assertThat(copy.serialize("after").getAppEngineHttpRequest().getBody().toStringUtf8())
                .isEqualTo("after");
        assertThat(
                        copy.serialize("after")
                                .getAppEngineHttpRequest()
                                .getAppEngineRouting()
                                .getService())
                .isEqualTo("worker");
    }

    private static AppEngineTargetSerializationSchema<String> schema() {
        return schema(RELATIVE_URI);
    }

    private static AppEngineTargetSerializationSchema<String> schema(String relativeUri) {
        return builder(relativeUri).build();
    }

    private static AppEngineTargetBuilder<String> builder() {
        return builder(RELATIVE_URI);
    }

    private static AppEngineTargetBuilder<String> builder(String relativeUri) {
        return CloudTasksSerializationSchema.appEngineTarget(relativeUri)
                .withBody(new SimpleStringSchema());
    }

    /** A body schema breaking Flink's contract by returning no bytes. */
    private static final class NullReturningSchema implements SerializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public byte[] serialize(String element) {
            return null;
        }
    }

    private static final class TrackingSchema implements SerializationSchema<String> {

        private static final long serialVersionUID = 1L;
        private boolean opened;

        @Override
        public void open(InitializationContext context) {
            opened = true;
        }

        @Override
        public byte[] serialize(String element) {
            return element.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
    }
}
