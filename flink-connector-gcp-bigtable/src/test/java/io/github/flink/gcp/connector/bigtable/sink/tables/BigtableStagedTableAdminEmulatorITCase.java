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

package io.github.flink.gcp.connector.bigtable.sink.tables;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.testutils.bigtable.BigtableEmulatorContainers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.BigtableEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The staged sink's metadata check sends its requests to a configured emulator. The emulator serves
 * no application profiles, so the check cannot pass there; its answer to the profile read is what
 * shows the request reached it rather than Bigtable.
 */
@Testcontainers
@Timeout(180)
class BigtableStagedTableAdminEmulatorITCase {

    @Container
    private static final BigtableEmulatorContainer EMULATOR =
            BigtableEmulatorContainers.newContainer();

    @Test
    void theProfileReadReachesTheEmulator() {
        EmulatorEndpoint endpoint =
                EmulatorEndpoint.parse(
                        EMULATOR.getHost() + ":" + EMULATOR.getEmulatorPort(), "emulatorEndpoint");

        Throwable failure =
                catchThrowable(
                        () ->
                                new BigtableStagedTableAdmin(endpoint, null)
                                        .validate(
                                                TableDestination.of(
                                                        "it-project", "it-instance", "orders"),
                                                "default",
                                                "markers",
                                                Map.of()));

        assertThat(failure).isInstanceOf(IOException.class).hasCauseInstanceOf(ApiException.class);
        assertThat(((ApiException) failure.getCause()).getStatusCode().getCode())
                .isEqualTo(StatusCode.Code.UNIMPLEMENTED);
    }
}
