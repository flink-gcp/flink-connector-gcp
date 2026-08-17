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

package io.github.flink.gcp.connector.testutils.cloudtasks;

import org.apache.flink.annotation.Internal;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * The Cloud Tasks emulator image shared by every harness that starts the emulator, so they cannot
 * drift apart.
 *
 * <p>Google publishes no official Cloud Tasks emulator and testcontainers' GCloud module has no
 * Cloud Tasks support, so this is the community aertje image on a plain {@link GenericContainer} —
 * the goccy BigQuery shape, not the Pub/Sub one.
 */
@Internal
public final class CloudTasksEmulatorContainers {

    private static final String IMAGE = "ghcr.io/aertje/cloud-tasks-emulator:1.2.0";

    private static final int PORT = 8123;

    private CloudTasksEmulatorContainers() {}

    /** Returns a new, unstarted emulator container. */
    public static GenericContainer<?> newContainer() {
        return new GenericContainer<>(IMAGE)
                // The emulator binds to localhost by default, which nothing outside the
                // container could reach.
                .withCommand("-host", "0.0.0.0", "-port", String.valueOf(PORT))
                .withExposedPorts(PORT)
                .waitingFor(Wait.forListeningPorts(PORT));
    }

    /** The emulator's gRPC endpoint as {@code host:port}, for {@code emulator-endpoint}. */
    public static String endpoint(GenericContainer<?> container) {
        return container.getHost() + ":" + container.getMappedPort(PORT);
    }
}
