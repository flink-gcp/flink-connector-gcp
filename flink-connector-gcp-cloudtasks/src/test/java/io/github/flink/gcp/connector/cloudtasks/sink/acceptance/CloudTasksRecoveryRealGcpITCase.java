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

import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.test.junit5.InjectMiniCluster;

import com.google.cloud.tasks.v2.Task;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Manually approved service acceptance; the dedicated gate excludes ordinary E2E discovery. */
@Tag("gated")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@EnabledIfEnvironmentVariable(named = "CLOUDTASKS_RECOVERY_ACCEPTANCE", matches = "approved")
class CloudTasksRecoveryRealGcpITCase extends StagedRecoveryAcceptance {
    private static CloudTasksAcceptanceRun run;

    @org.junit.jupiter.api.extension.RegisterExtension
    static final org.junit.jupiter.api.extension.TestWatcher STOP_ON_FAILURE =
            new org.junit.jupiter.api.extension.TestWatcher() {
                @Override
                public void testFailed(
                        org.junit.jupiter.api.extension.ExtensionContext context, Throwable cause) {
                    if (run != null) {
                        run.stopAdmission();
                    }
                }

                @Override
                public void testAborted(
                        org.junit.jupiter.api.extension.ExtensionContext context, Throwable cause) {
                    testFailed(context, cause);
                }
            };

    private final java.util.Set<String> caseTasks = new java.util.LinkedHashSet<>();

    @BeforeAll
    static void provision() throws Exception {
        String version = org.apache.flink.runtime.util.EnvironmentInformation.getVersion();
        if (!version.equals("1.20.4") && !version.equals("2.2.1")) {
            throw new IllegalArgumentException(
                    "Unregistered real-service Flink version: " + version);
        }
        String phase = version.equals("1.20.4") ? "120" : "221";
        run = new CloudTasksAcceptanceRun(phase);
        run.provision();
    }

    @AfterAll
    static void cleanup() throws Exception {
        if (run != null) {
            run.close();
        }
    }

    @Order(1)
    @ParameterizedTest(name = "direct-production-path {0}")
    @MethodSource("shapes")
    void productionCredentialsAndRetentionReadbackReachTheService(
            Shape shape, @InjectMiniCluster MiniCluster cluster) throws Exception {
        run.verifyPaused(queue(shape.appEngine));
        var before =
                run.list(queue(shape.appEngine)).stream()
                        .map(Task::getName)
                        .collect(java.util.stream.Collectors.toList());
        var job = start(shape, null, 1, false, "fail", true);
        staged(job, 4);
        assertThat(
                        run.list(queue(shape.appEngine)).stream()
                                .map(Task::getName)
                                .collect(java.util.stream.Collectors.toList()))
                .containsExactlyInAnyOrderElementsOf(before);
        checkpoint(cluster, job);
        committed(job, 4);
        var added =
                run.list(queue(shape.appEngine)).stream()
                        .filter(task -> !before.contains(task.getName()))
                        .collect(java.util.stream.Collectors.toList());
        assertThat(added).hasSize(4);
        for (Task task : added) {
            caseTasks.add(task.getName());
            // The connector consumes this response directly. Record this arm as readback-only.
            run.evidence.record(
                    "direct-path-readback",
                    Map.of("name", task.getName(), "createTime", task.getCreateTime().toString()));
            assertThat(run.get(task.getName()).getCreateTime()).isEqualTo(task.getCreateTime());
            assertThat(task.getDispatchCount()).isZero();
        }
        passed();
    }

    @Override
    RecordingTaskProxy.Service service() {
        return run;
    }

    @Override
    String queue(boolean appEngine) {
        return run.queue(appEngine);
    }

    @Override
    void observe(Task task) {
        caseTasks.add(task.getName());
        run.observe(task);
    }

    @Override
    void observeAll() {
        var originals =
                proxy.responses().stream()
                        .collect(
                                java.util.stream.Collectors.toMap(
                                        Task::getName, task -> task, (a, b) -> a));
        caseTasks.addAll(originals.keySet());
        run.observeAll(new java.util.ArrayList<>(originals.values()));
    }

    @Override
    java.util.List<String> liveTaskNames() {
        return run.list(currentQueue).stream()
                .map(Task::getName)
                .collect(java.util.stream.Collectors.toList());
    }

    @Override
    AcceptanceEvidence openEvidence() {
        run.requireActive();
        return run.evidence;
    }

    @Override
    void closeEvidence() {
        // Jobs and the proxy have stopped. Retire only this case's observed tasks.
        for (String name : caseTasks) {
            if (run.get(name) != null) {
                run.delete(name);
            }
            assertThat(run.get(name)).isNull();
        }
    }
}
