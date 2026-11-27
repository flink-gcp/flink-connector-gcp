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
        EmulatorEndpoint parsed = EmulatorEndpoint.parse(endpoint, "emulatorEndpoint");

        assertThat(parsed.getHost()).isEqualTo(host);
        assertThat(parsed.getPort()).isEqualTo(port);
    }

    @ParameterizedTest
    @ValueSource(strings = {"localhost:8086", "127.0.0.1:1", "[::1]:65535"})
    void theTargetIsTheFormItWasParsedFrom(String endpoint) {
        EmulatorEndpoint parsed = EmulatorEndpoint.parse(endpoint, "emulatorEndpoint");

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
        assertThatThrownBy(() -> EmulatorEndpoint.parse(endpoint, "emulatorEndpoint"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("emulatorEndpoint must be host:port, was '" + endpoint + "'");
    }

    /** An HTTP scheme must be followed by a host, or removing it would leave no host:port. */
    @ParameterizedTest
    @ValueSource(strings = {"http://:8086", "HTTPS://:8086", "https://:8086"})
    void rejectsAnHttpSchemeWithNoHostAfterIt(String endpoint) {
        assertThatThrownBy(() -> EmulatorEndpoint.parse(endpoint, "emulator-endpoint"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("emulator-endpoint must be host:port, was '" + endpoint + "'");
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @CsvSource({
        "http://localhost:8086, localhost:8086",
        "HTTPS://localhost:8086, localhost:8086",
        "https://[::1]:8086, [::1]:8086",
        "dns:///localhost:8086, dns:///localhost:8086",
        "http://dns:///localhost:8086, dns:///localhost:8086",
        // Matched in ASCII letter case only: a long s (U+017F) is not an s.
        "http\u017f://localhost:8086, http\u017f://localhost:8086"
    })
    void anHttpSchemeIsRemovedAndAResolverSchemeKept(String endpoint, String target) {
        assertThat(EmulatorEndpoint.parse(endpoint, "e").withoutHttpScheme().getTarget())
                .isEqualTo(target);
    }

    /** What a client taking an HTTP authority, not a gRPC target, is given (#2338, #2343). */
    @ParameterizedTest(name = "{0} -> {1}")
    @CsvSource({
        "localhost:8081, localhost:8081",
        "127.0.0.1:8081, 127.0.0.1:8081",
        "dns:///localhost:8081, localhost:8081",
        "dns:/localhost:8081, localhost:8081",
        "dns:localhost:8081, localhost:8081",
        // gRPC dials localhost:8081 for these, leaving the query out.
        "dns:localhost:8081?zone=west%20region:1, localhost:8081",
        "dns:///localhost:8081?zone=west%20region:1, localhost:8081",
        // An encoded ? or / ends the authority gRPC dials, as it ends this one.
        "dns:localhost:8081%3Ftag=one?zone=west:1, localhost:8081",
        "dns:///localhost:8081%3Ftag=one:1, localhost:8081",
        "dns:///localhost:8081%2Fextra:1, localhost:8081",
        // gRPC dials localhost:8081 for these; the user information is not part of what it dials.
        "dns:///a%2520b@localhost:8081, localhost:8081",
        "dns:///a%252Fb@localhost:8081, localhost:8081",
        // More than one path segment: gRPC dials the first where it accepts such a target, which
        // grpc-grpclb's dns provider refuses by default.
        "dns:///localhost:8081/extra:1, localhost:8081",
        "dns:///localhost:8081/zone%20a:1, localhost:8081",
        // No port in the name: gRPC's plaintext transport and HTTP both dial port 80.
        "dns:///localhost%2Fextra:1, localhost",
        "dns://8.8.8.8/localhost:8081, localhost:8081",
        "dns://[::1]/localhost:8081, localhost:8081",
        "http://localhost:8081, localhost:8081",
        "HTTPS://localhost:8081, localhost:8081",
        "http://dns:///localhost:8081, localhost:8081",
        "[::1]:8081, [::1]:8081",
        "dns:///%5B::1%5D:8081, [::1]:8081",
        // Not a URI, and gRPC refuses it too: left as it is.
        "dns:///[::1]:8081, dns:///[::1]:8081"
    })
    void theHttpAuthorityIsTheHostAndPortTheEndpointDials(String configured, String authority) {
        assertThat(EmulatorEndpoint.parse(configured, "e").httpAuthority()).isEqualTo(authority);
    }

    @Test
    void anEndpointWithoutAnHttpSchemeIsReturnedAsItIs() {
        EmulatorEndpoint endpoint = EmulatorEndpoint.parse("localhost:8086", "e");

        assertThat(endpoint.withoutHttpScheme()).isSameAs(endpoint);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {" localhost:8086", "localhost:8086 ", "localhost: 8086", "local host:8086"})
    void rejectsWhitespaceRatherThanTrimmingIt(String endpoint) {
        // Trimming would accept an endpoint other than the one that was configured, and the stray
        // space is one of the typos issue #235 exists to catch.
        assertThatThrownBy(() -> EmulatorEndpoint.parse(endpoint, "emulatorEndpoint"))
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
        assertThatThrownBy(() -> EmulatorEndpoint.parse(endpoint, "emulatorEndpoint"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("emulatorEndpoint must be host:port, was '" + endpoint + "'");
    }

    @Test
    void rejectsNull() {
        assertThatThrownBy(() -> EmulatorEndpoint.parse(null, "emulatorEndpoint"))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("emulatorEndpoint must not be null");
    }

    @ParameterizedTest
    @ValueSource(strings = {"emulatorRestEndpoint", "emulator-endpoint"})
    void bothRejectionsNameTheSettingTheyWereGiven(String name) {
        // Every other assertion in this class passes the one name a fixed message would have
        // rendered anyway, which is the blind spot that let BigQuery's emulatorRestEndpoint
        // setters name a setter the caller may never have touched (issue #895). A SQL caller is
        // the other shape: the name they typed is an option key, not a Java setter.
        assertThatThrownBy(() -> EmulatorEndpoint.parse("localhost", name))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(name + " must be host:port, was 'localhost'");
        assertThatThrownBy(() -> EmulatorEndpoint.parse(null, name))
                .isInstanceOf(NullPointerException.class)
                .hasMessage(name + " must not be null");
    }

    @Test
    void equalsAndHashCodeAreValueBased() {
        EmulatorEndpoint endpoint = EmulatorEndpoint.parse("localhost:8086", "emulatorEndpoint");

        assertThat(endpoint)
                .isEqualTo(EmulatorEndpoint.parse("localhost:8086", "emulatorEndpoint"))
                .hasSameHashCodeAs(EmulatorEndpoint.parse("localhost:8086", "emulatorEndpoint"))
                .isNotEqualTo(EmulatorEndpoint.parse("localhost:8087", "emulatorEndpoint"))
                .isNotEqualTo(EmulatorEndpoint.parse("127.0.0.1:8086", "emulatorEndpoint"))
                .isNotEqualTo(null)
                .isNotEqualTo("localhost:8086");
    }

    @Test
    void survivesSerialization() throws Exception {
        // Every consumer holds it inside a sink configuration that ships with the job graph.
        EmulatorEndpoint endpoint = EmulatorEndpoint.parse("localhost:8086", "emulatorEndpoint");

        EmulatorEndpoint deserialized =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(endpoint), getClass().getClassLoader());

        assertThat(deserialized).isEqualTo(endpoint);
        assertThat(deserialized.getHost()).isEqualTo("localhost");
        assertThat(deserialized.getPort()).isEqualTo(8086);
    }
}
