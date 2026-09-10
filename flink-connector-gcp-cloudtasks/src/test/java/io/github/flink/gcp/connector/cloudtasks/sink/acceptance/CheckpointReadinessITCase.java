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

package io.github.flink.gcp.connector.cloudtasks.sink.acceptance;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;

import io.github.flink.gcp.connector.testutils.Awaits;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Job RUNNING does not imply that each checkpoint participant has initialized. */
@Timeout(60)
class CheckpointReadinessITCase {
    @RegisterExtension
    static final MiniClusterExtension CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    private static CountDownLatch initializing;
    private static CountDownLatch releaseInitialization;
    @TempDir Path temporary;

    @Test
    void waitsForInitializingParticipantBeforeCheckpoint(@InjectMiniCluster MiniCluster cluster)
            throws Exception {
        initializing = new CountDownLatch(1);
        releaseInitialization = new CountDownLatch(1);
        var config = new Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, temporary.toUri().toString());
        var env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);
        env.disableOperatorChaining();
        env.enableCheckpointing(TimeUnit.HOURS.toMillis(1));
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(TimeUnit.HOURS.toMillis(1));
        env.fromSource(
                        new DataGeneratorSource<Long>(
                                value -> value,
                                Long.MAX_VALUE,
                                RateLimiterStrategy.perSecond(20),
                                Types.LONG),
                        WatermarkStrategy.noWatermarks(),
                        "records")
                .map(new DelayedInitialization())
                .sinkTo(new DiscardingSink<>());
        var executor = Executors.newSingleThreadExecutor();
        var job = env.executeAsync("checkpoint-readiness");
        try {
            assertThat(initializing.await(20, TimeUnit.SECONDS)).isTrue();
            Awaits.await(
                    "job RUNNING while participant initializes",
                    Duration.ofSeconds(20),
                    () -> {
                        try {
                            return job.getJobStatus().get(5, TimeUnit.SECONDS) == JobStatus.RUNNING;
                        } catch (Exception failure) {
                            throw new AssertionError(failure);
                        }
                    });
            assertThatThrownBy(
                            () ->
                                    cluster.triggerCheckpoint(job.getJobID())
                                            .get(5, TimeUnit.SECONDS))
                    .hasStackTraceContaining("Not all required tasks are currently running");
            var waiting = new CountDownLatch(1);
            var ready =
                    executor.submit(
                            () -> {
                                waiting.countDown();
                                StagedRecoveryAcceptance.awaitRunning(cluster, job);
                                return null;
                            });
            assertThat(waiting.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> ready.get(200, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            releaseInitialization.countDown();
            ready.get(20, TimeUnit.SECONDS);
            assertThat(cluster.triggerCheckpoint(job.getJobID()).get(20, TimeUnit.SECONDS))
                    .isNotBlank();
        } finally {
            releaseInitialization.countDown();
            executor.shutdownNow();
            StagedRecoveryAcceptance.stop(job);
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static final class DelayedInitialization extends RichMapFunction<Long, Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public void open(OpenContext context) throws Exception {
            initializing.countDown();
            if (!releaseInitialization.await(30, TimeUnit.SECONDS)) {
                throw new AssertionError("Initialization was not released");
            }
        }

        @Override
        public Long map(Long value) {
            return value;
        }
    }
}
