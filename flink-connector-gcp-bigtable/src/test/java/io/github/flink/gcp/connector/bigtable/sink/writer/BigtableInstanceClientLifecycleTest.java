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

package io.github.flink.gcp.connector.bigtable.sink.writer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Runs the production client lifecycle in a bounded child JVM. */
@Timeout(60)
class BigtableInstanceClientLifecycleTest {

    @Test
    void historicalInstancesDoNotAccumulateClientResources() throws Exception {
        Path java = Path.of(System.getProperty("java.home"), "bin", "java");
        String classpath =
                System.getProperty(
                        "surefire.test.class.path", System.getProperty("java.class.path"));
        Path outputFile = Files.createTempFile("bigtable-client-lifecycle-", ".log");
        Process process =
                new ProcessBuilder(
                                java.toString(),
                                "-Xmx128m",
                                "-cp",
                                classpath,
                                BigtableInstanceClientLifecycleProbe.class.getName())
                        .redirectErrorStream(true)
                        .redirectOutput(outputFile.toFile())
                        .start();

        boolean completed;
        String output;
        try {
            completed = process.waitFor(Duration.ofSeconds(45).toMillis(), TimeUnit.MILLISECONDS);
            if (!completed) {
                process.destroyForcibly();
                process.waitFor();
            }
            output = Files.readString(outputFile, StandardCharsets.UTF_8);
        } finally {
            Files.deleteIfExists(outputFile);
        }

        assertThat(completed).as(output).isTrue();
        assertThat(process.exitValue()).as(output).isZero();
        assertThat(output).contains("OK baselineThreads=");
    }
}
