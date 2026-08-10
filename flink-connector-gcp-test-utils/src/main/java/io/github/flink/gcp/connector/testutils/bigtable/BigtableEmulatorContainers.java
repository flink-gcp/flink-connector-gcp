/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.testutils.bigtable;

import org.apache.flink.annotation.Internal;

import org.testcontainers.containers.BigtableEmulatorContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The Bigtable emulator image shared by every harness that starts the emulator — the connector
 * module's own integration harness and the SQL module's smoke test — so they cannot drift apart.
 *
 * <p>The sibling of {@code testutils.pubsub.PubSubEmulatorContainers}, which pins the same gcloud
 * CLI image in a constant of its own — emulator fixtures are deliberately not unified (issue #27) —
 * so a bump here moves the two Bigtable harnesses only, and a bump meant to move every emulator
 * edits both classes.
 */
@Internal
public final class BigtableEmulatorContainers {

    private static final DockerImageName IMAGE =
            DockerImageName.parse(
                    "gcr.io/google.com/cloudsdktool/google-cloud-cli:441.0.0-emulators");

    private BigtableEmulatorContainers() {}

    /**
     * Returns a new, unstarted emulator container.
     *
     * @return the container
     */
    public static BigtableEmulatorContainer newContainer() {
        return new BigtableEmulatorContainer(IMAGE);
    }
}
