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

package io.github.flink.gcp.connector.base.rpc;

import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link EmulatorEndpoint}. */
class EmulatorEndpointTest {

    @ParameterizedTest
    @CsvSource({
        "localhost:8086, localhost, 8086",
        "127.0.0.1:1, 127.0.0.1, 1",
        "host.example:65535, host.example, 65535",
        // The host is split at the last colon and kept verbatim, so an IPv6 literal keeps its
        // brackets rather than being reinterpreted.
        "[::1]:8086, [::1], 8086",
        // A leading zero is a port written oddly, not a different port.
        "localhost:08086, localhost, 8086",
    })
    void parsesHostAndPort(String endpoint, String host, int port) {
        EmulatorEndpoint parsed = EmulatorEndpoint.parse(endpoint);

        assertThat(parsed.getHost()).isEqualTo(host);
        assertThat(parsed.getPort()).isEqualTo(port);
    }

    @ParameterizedTest
    @ValueSource(strings = {"localhost:8086", "127.0.0.1:1", "[::1]:65535"})
    void theTargetIsTheFormItWasParsedFrom(String endpoint) {
        EmulatorEndpoint parsed = EmulatorEndpoint.parse(endpoint);

        assertThat(parsed.getTarget()).isEqualTo(endpoint);
        assertThat(parsed).hasToString(endpoint);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "localhost8086", // the missing colon issue #235 opens with
                "localhost",
                "localhost:",
                ":8086",
                ":",
                "localhost:port",
                "8086",
                "  ",
                "",
            })
    void rejectsWhatIsNotHostAndPort(String endpoint) {
        assertThatThrownBy(() -> EmulatorEndpoint.parse(endpoint))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("emulatorEndpoint must be host:port, was '" + endpoint + "'");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {" localhost:8086", "localhost:8086 ", "localhost: 8086", "local host:8086"})
    void rejectsWhitespaceRatherThanTrimmingIt(String endpoint) {
        // Trimming would accept an endpoint other than the one that was configured, and the stray
        // space is one of the typos issue #235 exists to catch.
        assertThatThrownBy(() -> EmulatorEndpoint.parse(endpoint))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("emulatorEndpoint must be host:port, was '" + endpoint + "'");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "localhost:0",
                "localhost:65536",
                "localhost:+8086",
                "localhost:-1",
                // Unicode decimal digits and an overflowing port both parse to something under a
                // laxer reading; neither is a port.
                "localhost:８０８６",
                "localhost:99999999999",
            })
    void rejectsAPortThatIsNotOneToSixtyFiveThousand(String endpoint) {
        assertThatThrownBy(() -> EmulatorEndpoint.parse(endpoint))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("emulatorEndpoint must be host:port, was '" + endpoint + "'");
    }

    @Test
    void rejectsNull() {
        assertThatThrownBy(() -> EmulatorEndpoint.parse(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("emulatorEndpoint must not be null");
    }

    @Test
    void equalsAndHashCodeAreValueBased() {
        EmulatorEndpoint endpoint = EmulatorEndpoint.parse("localhost:8086");

        assertThat(endpoint)
                .isEqualTo(EmulatorEndpoint.parse("localhost:8086"))
                .hasSameHashCodeAs(EmulatorEndpoint.parse("localhost:8086"))
                .isNotEqualTo(EmulatorEndpoint.parse("localhost:8087"))
                .isNotEqualTo(EmulatorEndpoint.parse("127.0.0.1:8086"))
                .isNotEqualTo(null)
                .isNotEqualTo("localhost:8086");
    }

    @Test
    void survivesSerialization() throws Exception {
        // Every consumer holds it inside a sink configuration that ships with the job graph.
        EmulatorEndpoint endpoint = EmulatorEndpoint.parse("localhost:8086");

        EmulatorEndpoint deserialized =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(endpoint), getClass().getClassLoader());

        assertThat(deserialized).isEqualTo(endpoint);
        assertThat(deserialized.getHost()).isEqualTo("localhost");
        assertThat(deserialized.getPort()).isEqualTo(8086);
    }
}
