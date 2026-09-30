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

package io.github.flink.gcp.connector.firestore.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.util.InstantiationUtil;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.ServiceOptions;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.sink.serializer.FirestoreWriteSerializationSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FirestoreSinkBuilder}. */
class FirestoreSinkBuilderTest {

    private static final DatabaseDestination DATABASE = DatabaseDestination.of("p");

    @Test
    void buildsASinkFromTheTwoRequiredOptions() {
        Sink<String> sink =
                FirestoreSink.<String>builder().database(DATABASE).serializer(serializer()).build();

        assertThat(sink).isInstanceOf(FirestoreBulkWriterSink.class);
        FirestoreSinkConfig<String> config = ((FirestoreBulkWriterSink<String>) sink).getConfig();
        assertThat(config.getDatabase()).isEqualTo(DATABASE);
        assertThat(config.getWriterOptions()).isEqualTo(FirestoreWriterOptions.defaults());
        assertThat(config.getEmulatorEndpoint()).isNull();
        assertThat(config.getFailedWriteHandler()).hasToString("FailureHandler.failJob()");
    }

    @Test
    void carriesEveryOptionItWasGiven() {
        FirestoreWriterOptions options =
                FirestoreWriterOptions.builder().maxInFlightWrites(7).build();
        FailureHandler<FailedElement> handler = FailureHandler.logAndDrop();

        Sink<String> sink =
                FirestoreSink.<String>builder()
                        .database(DATABASE)
                        .serializer(serializer())
                        .writerOptions(options)
                        .failedWriteHandler(handler)
                        .emulatorEndpoint("localhost:8080")
                        .build();

        FirestoreSinkConfig<String> config = ((FirestoreBulkWriterSink<String>) sink).getConfig();
        assertThat(config.getWriterOptions()).isSameAs(options);
        assertThat(config.getFailedWriteHandler()).isSameAs(handler);
        assertThat(config.getEmulatorEndpoint()).isNotNull();
        assertThat(config.getEmulatorEndpoint().getTarget()).isEqualTo("localhost:8080");
    }

    @Test
    void serviceAccountKeyFileSurvivesJobSubmissionSerialization() throws Exception {
        Sink<String> sink =
                FirestoreSink.<String>builder()
                        .database(DATABASE)
                        .serializer(serializer())
                        .serviceAccountKeyFile("/var/run/secrets/firestore.json")
                        .build();

        Sink<String> restored =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(sink), getClass().getClassLoader());

        assertThat(
                        ((FirestoreBulkWriterSink<String>) restored)
                                .getConfig()
                                .getServiceAccountKeyFile())
                .isEqualTo("/var/run/secrets/firestore.json");
    }

    @Test
    void rejectsInvalidOrConflictingServiceAccountKeyFile() {
        assertThatThrownBy(() -> FirestoreSink.<String>builder().serviceAccountKeyFile(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("serviceAccountKeyFile must not be null");
        assertThatThrownBy(() -> FirestoreSink.<String>builder().serviceAccountKeyFile(" \t"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("serviceAccountKeyFile must not be blank");
        assertThatThrownBy(
                        () ->
                                FirestoreSink.<String>builder()
                                        .database(DATABASE)
                                        .serializer(serializer())
                                        .serviceAccountKeyFile("key.json")
                                        .emulatorEndpoint("localhost:8080")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("serviceAccountKeyFile(...)")
                .hasMessageContaining("emulatorEndpoint(...)");
    }

    @Test
    void defaultsToFailingTheJobOnAPreconditionFailure() {
        Sink<String> sink =
                FirestoreSink.<String>builder().database(DATABASE).serializer(serializer()).build();

        assertThat(
                        ((FirestoreBulkWriterSink<String>) sink)
                                .getConfig()
                                .getPreconditionFailurePolicy())
                .isEqualTo(PreconditionFailurePolicy.FAIL_JOB);
    }

    @Test
    void carriesThePreconditionFailurePolicyItWasGiven() {
        Sink<String> sink =
                FirestoreSink.<String>builder()
                        .database(DATABASE)
                        .serializer(serializer())
                        .preconditionFailurePolicy(
                                PreconditionFailurePolicy.ROUTE_TO_FAILURE_HANDLER)
                        .build();

        assertThat(
                        ((FirestoreBulkWriterSink<String>) sink)
                                .getConfig()
                                .getPreconditionFailurePolicy())
                .isEqualTo(PreconditionFailurePolicy.ROUTE_TO_FAILURE_HANDLER);
    }

    @Test
    void namesTheMissingOptionAndHowToSetIt() {
        assertThatThrownBy(() -> FirestoreSink.<String>builder().serializer(serializer()).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("database(...)");
        assertThatThrownBy(() -> FirestoreSink.<String>builder().database(DATABASE).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("serializer(...)");
    }

    @Test
    void rejectsNullOptions() {
        FirestoreSinkBuilder<String> builder = FirestoreSink.builder();

        assertThatThrownBy(() -> builder.database(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.serializer(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.writerOptions(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.failedWriteHandler(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.preconditionFailurePolicy(null))
                .isInstanceOf(NullPointerException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"localhost", "localhost:", "localhost:0", "localhost:70000", " a:1"})
    void rejectsAMalformedEmulatorEndpointWhereItIsTyped(String endpoint) {
        // Parsed at the setter rather than at writer creation, so a typo fails on submission
        // instead of on a task manager.
        assertThatThrownBy(() -> FirestoreSink.<String>builder().emulatorEndpoint(endpoint))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("emulatorEndpoint must be host:port, was '" + endpoint + "'");
    }

    @Test
    void acceptsASerializerOfASupertype() {
        FirestoreWriteSerializationSchema<Object> wide =
                (element, context) -> FirestoreWrite.set("orders/" + element, java.util.Map.of());

        Sink<String> sink =
                FirestoreSink.<String>builder().database(DATABASE).serializer(wide).build();

        // The variance is checked by this compiling; the assertion checks the builder carried the
        // schema through rather than wrapping or replacing it.
        assertThat(((FirestoreBulkWriterSink<String>) sink).getConfig().getSerializer())
                .isSameAs(wide);
    }

    @Test
    void retryOverridesTheLibraryWouldDropAreRefusedWhenTheSinkIsBuilt() {
        RetrySettings gaxDefault = ServiceOptions.getDefaultRetrySettings();
        FirestoreWriterOptions dropped =
                FirestoreWriterOptions.builder()
                        .retryTotalTimeout(gaxDefault.getTotalTimeoutDuration())
                        .retryInitialDelay(gaxDefault.getInitialRetryDelayDuration())
                        .retryDelayMultiplier(gaxDefault.getRetryDelayMultiplier())
                        .retryMaxDelay(gaxDefault.getMaxRetryDelayDuration())
                        .retryInitialRpcTimeout(gaxDefault.getInitialRpcTimeoutDuration())
                        .retryRpcTimeoutMultiplier(gaxDefault.getRpcTimeoutMultiplier())
                        .retryMaxRpcTimeout(gaxDefault.getMaxRpcTimeoutDuration())
                        .retryMaxAttempts(gaxDefault.getMaxAttempts())
                        .build();

        assertThatThrownBy(
                        () ->
                                FirestoreSink.<String>builder()
                                        .database(DATABASE)
                                        .serializer(serializer())
                                        .writerOptions(dropped)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("treats as unset");
        assertThat(
                        FirestoreSink.<String>builder()
                                .database(DATABASE)
                                .serializer(serializer())
                                .writerOptions(
                                        FirestoreWriterOptions.builder()
                                                .retryTotalTimeout(Duration.ofMinutes(2))
                                                .build())
                                .build())
                .isNotNull();
    }

    private static FirestoreWriteSerializationSchema<String> serializer() {
        return (element, context) -> FirestoreWrite.set("orders/" + element, java.util.Map.of());
    }
}
