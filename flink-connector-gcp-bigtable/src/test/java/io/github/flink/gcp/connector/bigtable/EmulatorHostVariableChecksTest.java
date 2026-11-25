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

package io.github.flink.gcp.connector.bigtable;

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link EmulatorHostVariableChecks}. */
@Timeout(60)
class EmulatorHostVariableChecksTest {

    private static final EmulatorEndpoint ENDPOINT =
            EmulatorEndpoint.parse("localhost:8086", "emulatorEndpoint");

    @Test
    void anUnsetOrEmptyVariableIsAccepted() {
        assertThatCode(() -> EmulatorHostVariableChecks.check(null, null))
                .doesNotThrowAnyException();
        assertThatCode(() -> EmulatorHostVariableChecks.check("", null)).doesNotThrowAnyException();
    }

    @Test
    void aConfiguredEndpointTakesPrecedence() {
        assertThatCode(() -> EmulatorHostVariableChecks.check("elsewhere:9000", ENDPOINT))
                .doesNotThrowAnyException();
        // Not parsed when an endpoint is configured, as the client library never reads it then.
        assertThatCode(() -> EmulatorHostVariableChecks.check("not a host", ENDPOINT))
                .doesNotThrowAnyException();
    }

    @Test
    void aSetVariableWithoutAnEndpointIsRefusedByName() {
        assertThatThrownBy(() -> EmulatorHostVariableChecks.check("localhost:8086", null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(EmulatorHostVariableChecks.NAME)
                .hasMessageContaining("'localhost:8086'")
                .hasMessageContaining("emulator endpoint");
    }

    /**
     * The wiring, in a child JVM whose environment carries the variable, since a test cannot set
     * its own: every settings path of both client families refuses it, and none fails earlier on
     * loading credentials (see {@link EmulatorHostVariableChecksProbe}). The variable is named here
     * literally, and the probe checks that the client library reads the same one.
     */
    @Test
    void everyClientFamilyRefusesTheVariableInAProcessThatSetsIt() throws Exception {
        assertThat(runProbe("localhost:8086")).contains(EmulatorHostVariableChecksProbe.OK);
    }

    /**
     * A value the client library cannot parse is refused by the connector too, not by the library:
     * the check runs before the library reads the variable.
     */
    @Test
    void aMalformedValueIsRefusedByTheConnectorBeforeTheLibraryParsesIt() throws Exception {
        assertThat(runProbe("localhost")).contains(EmulatorHostVariableChecksProbe.OK);
    }

    private static String runProbe(String value) throws Exception {
        Path java = Path.of(System.getProperty("java.home"), "bin", "java");
        String classpath =
                System.getProperty(
                        "surefire.test.class.path", System.getProperty("java.class.path"));
        Path outputFile = Files.createTempFile("bigtable-emulator-host-variable-", ".log");
        ProcessBuilder builder =
                new ProcessBuilder(
                                java.toString(),
                                "-Xmx128m",
                                "-cp",
                                classpath,
                                EmulatorHostVariableChecksProbe.class.getName())
                        .redirectErrorStream(true)
                        .redirectOutput(outputFile.toFile());
        builder.environment().put("BIGTABLE_EMULATOR_HOST", value);
        Process process = builder.start();

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
        return output;
    }
}
