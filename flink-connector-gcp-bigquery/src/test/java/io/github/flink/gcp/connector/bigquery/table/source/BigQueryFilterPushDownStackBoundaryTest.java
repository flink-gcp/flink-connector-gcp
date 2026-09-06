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

package io.github.flink.gcp.connector.bigquery.table.source;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Exercises translation and deferred rendering in a JVM with an explicit stack size. */
class BigQueryFilterPushDownStackBoundaryTest {
    @TempDir Path temporaryDirectory;

    @ParameterizedTest
    @ValueSource(
            strings = {
                "and-left",
                "and-right",
                "or-left",
                "or-right",
                "mixed-left",
                "mixed-right",
                "fallback",
                "budget"
            })
    void deepPredicatesCompleteWithAOneMiBStack(String scenario) throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath =
                System.getProperty(
                        "surefire.test.class.path", System.getProperty("java.class.path"));
        Path log = temporaryDirectory.resolve(scenario + ".log");
        Process process =
                new ProcessBuilder(
                                java,
                                "-Xss1m",
                                "-Xmx512m",
                                "-cp",
                                classpath,
                                BigQueryFilterPushDownStackProbe.class.getName(),
                                scenario)
                        .redirectErrorStream(true)
                        .redirectOutput(log.toFile())
                        .start();
        boolean completed;
        try {
            completed = process.waitFor(60, TimeUnit.SECONDS);
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly().waitFor();
            }
        }
        String output = Files.readString(log);
        assertThat(completed).as(output).isTrue();
        assertThat(process.exitValue()).as(output).isZero();
        assertThat(output).contains("PASS " + scenario);
    }
}
