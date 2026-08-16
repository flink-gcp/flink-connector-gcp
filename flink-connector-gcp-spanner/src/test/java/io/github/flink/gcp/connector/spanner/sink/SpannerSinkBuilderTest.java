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

package io.github.flink.gcp.connector.spanner.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.spanner.Mutation;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.sink.serializer.SpannerMutationSerializationSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SpannerSinkBuilder}. */
class SpannerSinkBuilderTest {

    private static final SpannerDatabase DATABASE = SpannerDatabase.of("p", "i", "d");

    @Test
    void buildsASinkFromTheTwoRequiredOptions() {
        Sink<String> sink =
                SpannerSink.<String>builder().database(DATABASE).serializer(serializer()).build();

        assertThat(sink).isInstanceOf(SpannerMutationsSink.class);
        SpannerSinkConfig<String> config = ((SpannerMutationsSink<String>) sink).getConfig();
        assertThat(config.getDatabase()).isEqualTo(DATABASE);
        assertThat(config.getWriterOptions()).isEqualTo(SpannerWriterOptions.defaults());
        assertThat(config.getEmulatorEndpoint()).isNull();
        assertThat(config.getFailedMutationHandler()).hasToString("FailureHandler.failJob()");
    }

    @Test
    void carriesEveryOptionItWasGiven() {
        SpannerWriterOptions options = SpannerWriterOptions.builder().maxBatchMutations(7).build();
        FailureHandler<FailedElement> handler = FailureHandler.logAndDrop();

        Sink<String> sink =
                SpannerSink.<String>builder()
                        .database(DATABASE)
                        .serializer(serializer())
                        .writerOptions(options)
                        .failedMutationHandler(handler)
                        .emulatorEndpoint("localhost:9010")
                        .build();

        SpannerSinkConfig<String> config = ((SpannerMutationsSink<String>) sink).getConfig();
        assertThat(config.getWriterOptions()).isSameAs(options);
        assertThat(config.getFailedMutationHandler()).isSameAs(handler);
        assertThat(config.getEmulatorEndpoint()).isNotNull();
        assertThat(config.getEmulatorEndpoint().getTarget()).isEqualTo("localhost:9010");
    }

    @Test
    void serviceAccountKeyFileSurvivesJobSubmissionSerialization() throws Exception {
        Sink<String> sink =
                SpannerSink.<String>builder()
                        .database(DATABASE)
                        .serializer(serializer())
                        .serviceAccountKeyFile("/var/run/secrets/spanner.json")
                        .build();

        Sink<String> restored =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(sink), getClass().getClassLoader());

        assertThat(((SpannerMutationsSink<String>) restored).getConfig().getServiceAccountKeyFile())
                .isEqualTo("/var/run/secrets/spanner.json");
    }

    @Test
    void rejectsInvalidOrConflictingServiceAccountKeyFile() {
        assertThatThrownBy(() -> SpannerSink.<String>builder().serviceAccountKeyFile(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("serviceAccountKeyFile must not be null");
        assertThatThrownBy(() -> SpannerSink.<String>builder().serviceAccountKeyFile(" \t"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("serviceAccountKeyFile must not be blank");
        assertThatThrownBy(
                        () ->
                                SpannerSink.<String>builder()
                                        .database(DATABASE)
                                        .serializer(serializer())
                                        .serviceAccountKeyFile("key.json")
                                        .emulatorEndpoint("localhost:9010")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("serviceAccountKeyFile(...)")
                .hasMessageContaining("emulatorEndpoint(...)");
    }

    @Test
    void defaultsToFailingTheJobOnAConstraintViolation() {
        Sink<String> sink =
                SpannerSink.<String>builder().database(DATABASE).serializer(serializer()).build();

        assertThat(((SpannerMutationsSink<String>) sink).getConfig().getConstraintViolationPolicy())
                .isEqualTo(ConstraintViolationPolicy.FAIL_JOB);
    }

    @Test
    void carriesTheConstraintViolationPolicyItWasGiven() {
        Sink<String> sink =
                SpannerSink.<String>builder()
                        .database(DATABASE)
                        .serializer(serializer())
                        .constraintViolationPolicy(
                                ConstraintViolationPolicy.ROUTE_TO_FAILURE_HANDLER)
                        .build();

        assertThat(((SpannerMutationsSink<String>) sink).getConfig().getConstraintViolationPolicy())
                .isEqualTo(ConstraintViolationPolicy.ROUTE_TO_FAILURE_HANDLER);
    }

    @Test
    void namesTheMissingOptionAndHowToSetIt() {
        assertThatThrownBy(() -> SpannerSink.<String>builder().serializer(serializer()).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("database(...)");
        assertThatThrownBy(() -> SpannerSink.<String>builder().database(DATABASE).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("serializer(...)");
    }

    @Test
    void rejectsNullOptions() {
        SpannerSinkBuilder<String> builder = SpannerSink.builder();

        assertThatThrownBy(() -> builder.database(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.serializer(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.writerOptions(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.failedMutationHandler(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.constraintViolationPolicy(null))
                .isInstanceOf(NullPointerException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"localhost", "localhost:", "localhost:0", "localhost:70000", " a:1"})
    void rejectsAMalformedEmulatorEndpointWhereItIsTyped(String endpoint) {
        // Parsed at the setter rather than at writer creation, so a typo fails on submission
        // instead of on a task manager.
        assertThatThrownBy(() -> SpannerSink.<String>builder().emulatorEndpoint(endpoint))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void acceptsASerializerOfASupertype() {
        SpannerMutationSerializationSchema<Object> wide =
                (element, context) ->
                        Mutation.newInsertOrUpdateBuilder("Orders")
                                .set("Id")
                                .to(element.toString())
                                .build();

        Sink<String> sink =
                SpannerSink.<String>builder().database(DATABASE).serializer(wide).build();

        // The variance is checked by this compiling; the assertion checks the builder carried the
        // schema through rather than wrapping or replacing it.
        assertThat(((SpannerMutationsSink<String>) sink).getConfig().getSerializer())
                .isSameAs(wide);
    }

    private static SpannerMutationSerializationSchema<String> serializer() {
        return (element, context) ->
                Mutation.newInsertOrUpdateBuilder("Orders").set("Id").to(element).build();
    }
}
