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

package io.github.flink.gcp.connector.docs;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;

import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksDeliveryGuarantee;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSink;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksStagedOptions;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;
import io.github.flink.gcp.connector.cloudtasks.sink.serializer.CloudTasksSerializationSchema;

import java.time.Duration;

final class CloudTasksCheckpointedCreation {
    private CloudTasksCheckpointedCreation() {}

    static void attach(DataStream<String> input, String durableCheckpointDirectory) {
        // tag::cloud-tasks-checkpointed-creation[]
        var environment = input.getExecutionEnvironment();
        Configuration checkpointSettings = new Configuration();
        checkpointSettings.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        checkpointSettings.set(CheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        checkpointSettings.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY, durableCheckpointDirectory);
        checkpointSettings.set(
                CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        checkpointSettings.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        checkpointSettings.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
        checkpointSettings.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(1));
        environment.configure(checkpointSettings);
        environment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        environment.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        // Size this timeout for every pending batch and retry wave in the deployment.
        environment.getCheckpointConfig().setCheckpointTimeout(Duration.ofMinutes(5).toMillis());

        Sink<String> sink =
                CloudTasksSink.<String>builder()
                        .queue(QueueDestination.of("my-project", "asia-northeast1", "webhooks"))
                        .deliveryGuarantee(CloudTasksDeliveryGuarantee.EXACTLY_ONCE)
                        .stagedOptions(
                                CloudTasksStagedOptions.builder().maxStagedTasks(1000).build())
                        .serializer(
                                CloudTasksSerializationSchema.httpTarget(
                                                "https://api.example.com/tasks")
                                        .withBody(new SimpleStringSchema()))
                        .build();
        input.sinkTo(sink).uid("checkpointed-cloud-tasks");
        // end::cloud-tasks-checkpointed-creation[]
    }
}
