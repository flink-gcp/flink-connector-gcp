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

package io.github.flink.gcp.connector.bigtable.sink;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Stage2CampaignCleanupProcessTest {
    @TempDir Path directory;

    @ParameterizedTest
    @ValueSource(strings = {"before", "during", "after", "forced"})
    @Timeout(40)
    void terminalCleanupQuiescesTheControllerBeforeDeletingItsWork(String mode) throws Exception {
        Path campaign = Stage2CampaignTestPlan.write(directory);
        ProcessHandle parent = ProcessHandle.current();
        Path output = directory.resolve("controller.log");
        Process controller =
                new ProcessBuilder(
                                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                                "-Xmx128m",
                                "-cp",
                                System.getProperty(
                                        "surefire.test.class.path",
                                        System.getProperty("java.class.path")),
                                Stage2CampaignCleanupProcess.class.getName(),
                                campaign.toString(),
                                mode,
                                Long.toString(parent.pid()),
                                parent.info().startInstant().orElseThrow().toString())
                        .redirectErrorStream(true)
                        .redirectOutput(output.toFile())
                        .start();
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (!Files.exists(campaign.resolve("barrier"))) {
                assertThat(controller.isAlive()).as(Files.readString(output)).isTrue();
                assertThat(System.nanoTime()).isLessThan(deadline);
                Thread.sleep(10);
            }
            Stage2CampaignJournal journal = new Stage2CampaignJournal(campaign);
            var state = journal.read();
            assertThat(state.getProperty("controllerPid"))
                    .isEqualTo(Long.toString(controller.pid()));
            String started = controller.info().startInstant().orElseThrow().toString();
            assertThat(state.getProperty("controllerStart")).isEqualTo(started);
            assertThatThrownBy(() -> journal.prepareCell(1))
                    .hasMessageContaining("Different campaign controller");
            assertThatThrownBy(journal::tablesReady)
                    .hasMessageContaining("Different campaign controller");
            assertThatThrownBy(() -> journal.cellCleaned("f".repeat(64)))
                    .hasMessageContaining("Different campaign controller");
            List<String> events = new ArrayList<>();
            var resources =
                    new Stage2CampaignSupervisor.Resources() {
                        @Override
                        public void verifyOwned() {}

                        @Override
                        public void deleteOwnedAndVerifyAbsent() {
                            assertThat(controller.isAlive())
                                    .as("controller must be quiescent before deletion")
                                    .isFalse();
                            assertThat(campaign.resolve("work")).exists();
                            events.add("cloud-absence");
                        }
                    };
            Stage2CampaignSupervisor supervisor =
                    new Stage2CampaignSupervisor(
                            journal,
                            resources,
                            (pid, start) -> {
                                assertThat(journal.read().getProperty("phase"))
                                        .isEqualTo("STOPPED");
                                assertThat(campaign.resolve("stop")).exists();
                                if (pid == controller.pid()) {
                                    assertThat(start).isEqualTo(started);
                                    if (mode.equals("before")) {
                                        Files.writeString(
                                                campaign.resolve("release"),
                                                "attempt cleanup after stop");
                                        assertThat(controller.waitFor(5, TimeUnit.SECONDS))
                                                .isTrue();
                                        assertThat(controller.exitValue())
                                                .as(Files.readString(output))
                                                .isZero();
                                        assertThat(campaign.resolve("refused")).exists();
                                    }
                                    Stage2CampaignSupervisor.stopProcess(pid, start);
                                    assertThat(controller.isAlive()).isFalse();
                                    events.add("controller-gone");
                                } else {
                                    assertThat(pid).isEqualTo(999_999_995L);
                                    Stage2CampaignSupervisor.stopProcess(pid, start);
                                    events.add("worker-gone");
                                }
                            });
            supervisor.cleanup();
            assertThat(events).containsExactly("controller-gone", "worker-gone", "cloud-absence");
            assertThat(journal.read().getProperty("phase")).isEqualTo("ABSENT");
            assertThat(campaign.resolve("work")).doesNotExist();
            if (mode.equals("after")) {
                assertThat(campaign.resolve("cell-0.properties")).exists();
            } else {
                assertThat(campaign.resolve("cell-0.properties")).doesNotExist();
            }
            if (mode.equals("forced")) {
                assertThat(campaign.resolve("terminating")).exists();
            }
        } finally {
            if (controller.isAlive()) {
                controller.destroyForcibly();
            }
            assertThat(controller.waitFor(5, TimeUnit.SECONDS)).isTrue();
        }
    }
}
