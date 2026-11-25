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

package io.github.flink.gcp.connector.datastore.source.batch.enumerator;

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

/** The emulator endpoint the splitter's HTTP client takes (#2338). */
class SplitterClientTest {

    @ParameterizedTest(name = "{0} -> {1}")
    @CsvSource({
        "localhost:8081, localhost:8081",
        "127.0.0.1:8081, 127.0.0.1:8081",
        "dns:///localhost:8081, localhost:8081",
        "dns:/localhost:8081, localhost:8081",
        "dns:localhost:8081, localhost:8081",
        "dns://8.8.8.8/localhost:8081, localhost:8081",
        "http://localhost:8081, localhost:8081",
        "HTTPS://localhost:8081, localhost:8081",
        "http://dns:///localhost:8081, localhost:8081",
        "[::1]:8081, [::1]:8081",
        "dns:///%5B::1%5D:8081, [::1]:8081",
        // Not a URI, and gRPC refuses it too: left as it is.
        "dns:///[::1]:8081, dns:///[::1]:8081"
    })
    void anEmulatorEndpointReachesTheHttpClientAsHostAndPort(String configured, String authority) {
        assertThat(SplitterClient.httpAuthority(EmulatorEndpoint.parse(configured, "e")))
                .isEqualTo(authority);
    }
}
