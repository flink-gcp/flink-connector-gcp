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

package io.github.flink.gcp.connector.testutils.pubsub;

import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The Pub/Sub emulator image shared by every harness that starts the emulator — the connector
 * module's sink and source harnesses and the SQL module's smoke test — so they cannot drift apart.
 */
public final class PubSubEmulatorContainers {

    private static final DockerImageName IMAGE =
            DockerImageName.parse(
                    "gcr.io/google.com/cloudsdktool/google-cloud-cli:441.0.0-emulators");

    private PubSubEmulatorContainers() {}

    /** Returns a new, unstarted emulator container. */
    public static PubSubEmulatorContainer newContainer() {
        return new PubSubEmulatorContainer(IMAGE);
    }
}
