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

package io.github.flink.gcp.connector.cloudtasks.sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.tasks.v2.HttpMethod;
import com.google.cloud.tasks.v2.HttpRequest;
import com.google.cloud.tasks.v2.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CloudTasksStagedOptionsTest {
    @Test
    void defaultsSurviveSerializationAndAnOptionsBuilderIsASnapshot() throws Exception {
        var builder = CloudTasksStagedOptions.builder();
        var options = InstantiationUtil.clone(builder.build());
        builder.maxStagedTasks(7).requestTimeout(Duration.ofSeconds(1));
        assertThat(options.getNameRetention()).isEqualTo(Duration.ofHours(1));
        assertThat(options.getClockSkewAllowance()).isEqualTo(Duration.ofMinutes(5));
        assertThat(options.getRequestTimeout()).isEqualTo(Duration.ofSeconds(20));
        assertThat(options.getMaxStagedTasks()).isEqualTo(100_000);
        assertThat(options.getMaxStagedBytes()).isEqualTo(64L * 1024 * 1024);
        assertThat(options.isVerifyQueueRetention()).isTrue();
        assertThat(options.getExpiredEnvelopePolicy())
                .isEqualTo(CloudTasksStagedOptions.ExpiredEnvelopePolicy.FAIL);
        assertThat(options.toStagingConfig().authorizationDeadlineMillis(1000))
                .isEqualTo(3_281_000);
    }

    @Test
    void rejectsInvalidValuesAtThePublicSetterAndInvalidWindowsAtBuild() {
        var tooLarge = Duration.ofNanos(Long.MAX_VALUE).plusNanos(1);
        assertThatThrownBy(() -> CloudTasksStagedOptions.builder().nameRetention(tooLarge))
                .hasMessageContaining("nameRetention")
                .hasMessageContaining("292 years");
        assertThatThrownBy(() -> CloudTasksStagedOptions.builder().clockSkewAllowance(tooLarge))
                .hasMessageContaining("clockSkewAllowance");
        assertThatThrownBy(() -> CloudTasksStagedOptions.builder().requestTimeout(tooLarge))
                .hasMessageContaining("requestTimeout");
        assertThatThrownBy(() -> CloudTasksStagedOptions.builder().requestTimeout(Duration.ZERO))
                .hasMessageContaining("requestTimeout");
        assertThatThrownBy(
                        () ->
                                CloudTasksStagedOptions.builder()
                                        .clockSkewAllowance(Duration.ofSeconds(-1)))
                .hasMessageContaining("clockSkewAllowance");
        assertThatThrownBy(() -> CloudTasksStagedOptions.builder().maxStagedTasks(0))
                .hasMessageContaining("maxStagedTasks");
        assertThatThrownBy(() -> CloudTasksStagedOptions.builder().maxStagedBytes(0))
                .hasMessageContaining("maxStagedBytes");
        assertThatThrownBy(
                        () ->
                                CloudTasksStagedOptions.builder()
                                        .nameRetention(Duration.ofSeconds(320))
                                        .build())
                .hasMessageContaining("nameRetention")
                .hasMessageContaining("clockSkewAllowance")
                .hasMessageContaining("requestTimeout");
        assertThatThrownBy(() -> CloudTasksStagedOptions.builder().expiredEnvelopePolicy(null))
                .hasMessageContaining("expiredEnvelopePolicy");
    }

    @Test
    void requiresFixedQueuesFailJobAndTheSelectedModeButAcceptsStableKeys() throws Exception {
        assertThat(builder().build()).isInstanceOf(CloudTasksCreateTaskSink.class);
        assertThat(
                        InstantiationUtil.clone(
                                builder()
                                        .deliveryGuarantee(CloudTasksDeliveryGuarantee.EXACTLY_ONCE)
                                        .taskIdExtractor(value -> value)
                                        .build()))
                .isInstanceOf(CloudTasksStagedCreateTaskSink.class);
        assertThatThrownBy(
                        () ->
                                builder()
                                        .stagedOptions(CloudTasksStagedOptions.builder().build())
                                        .build())
                .hasMessageContaining("stagedOptions")
                .hasMessageContaining("EXACTLY_ONCE");
        assertThatThrownBy(
                        () ->
                                builder()
                                        .deliveryGuarantee(CloudTasksDeliveryGuarantee.EXACTLY_ONCE)
                                        .destinationResolver(
                                                (value, context) ->
                                                        QueueDestination.of("p", "l", "q"))
                                        .build())
                .hasMessageContaining("fixed queue");
        assertThatThrownBy(
                        () ->
                                builder()
                                        .deliveryGuarantee(CloudTasksDeliveryGuarantee.EXACTLY_ONCE)
                                        .failedTaskHandler(element -> {})
                                        .build())
                .hasMessageContaining("failJob");
    }

    @ParameterizedTest
    @EnumSource(RuntimeExecutionMode.class)
    void graphRequiresExplicitStreaming(RuntimeExecutionMode mode) {
        var environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setRuntimeMode(mode);
        environment.enableCheckpointing(100);
        attach(environment);
        if (mode == RuntimeExecutionMode.STREAMING) {
            assertThat(environment.getStreamGraph()).isNotNull();
        } else {
            assertThatThrownBy(environment::getStreamGraph)
                    .hasMessageContaining("execution.runtime-mode");
        }
    }

    @Test
    void graphRequiresExactlyOnceCheckpointsAndTheBoundedStreamingTail() {
        var disabled = StreamExecutionEnvironment.getExecutionEnvironment();
        disabled.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        attach(disabled);
        assertThatThrownBy(disabled::getStreamGraph)
                .hasMessageContaining("execution.checkpointing.interval");
        var atLeastOnce = StreamExecutionEnvironment.getExecutionEnvironment();
        atLeastOnce.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        atLeastOnce.enableCheckpointing(100);
        atLeastOnce
                .getCheckpointConfig()
                .setCheckpointingConsistencyMode(CheckpointingMode.AT_LEAST_ONCE);
        attach(atLeastOnce);
        assertThatThrownBy(atLeastOnce::getStreamGraph)
                .hasMessageContaining("execution.checkpointing.mode");
        var configuration = new Configuration();
        configuration.set(CheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, false);
        var noTail = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        noTail.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        noTail.enableCheckpointing(100);
        attach(noTail);
        assertThatThrownBy(noTail::getStreamGraph)
                .hasMessageContaining("execution.checkpointing.checkpoints-after-tasks-finish=true")
                .hasMessageNotContaining("checkpoints-after-tasks-finish.enabled");
    }

    @Test
    void deserializedOptionsAreRevalidatedBeforeStaging() throws Exception {
        var options = CloudTasksStagedOptions.builder().build();
        var field = CloudTasksStagedOptions.class.getDeclaredField("requestTimeout");
        field.setAccessible(true);
        field.set(options, Duration.ofNanos(Long.MAX_VALUE).plusNanos(1));
        var restored = InstantiationUtil.clone(options);
        assertThatThrownBy(restored::toStagingConfig).hasMessageContaining("requestTimeout");
        assertThat(CloudTasksStagedOptions.builder().build().getRequestTimeout())
                .isEqualTo(Duration.ofSeconds(20));
    }

    private static void attach(StreamExecutionEnvironment environment) {
        environment
                .fromData("value")
                .sinkTo(
                        builder()
                                .deliveryGuarantee(CloudTasksDeliveryGuarantee.EXACTLY_ONCE)
                                .build())
                .uid("tasks");
    }

    private static CloudTasksSinkBuilder<String> builder() {
        return CloudTasksSink.<String>builder()
                .queue(QueueDestination.of("p", "l", "q"))
                .emulatorEndpoint("localhost:1")
                .serializer(
                        value ->
                                Task.newBuilder()
                                        .setHttpRequest(
                                                HttpRequest.newBuilder()
                                                        .setUrl("https://example.com/task")
                                                        .setHttpMethod(HttpMethod.POST))
                                        .build());
    }
}
