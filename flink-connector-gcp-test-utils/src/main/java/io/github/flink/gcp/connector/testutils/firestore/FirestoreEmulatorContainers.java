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

package io.github.flink.gcp.connector.testutils.firestore;

import org.apache.flink.annotation.Internal;

import org.testcontainers.containers.FirestoreEmulatorContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The Firestore emulator image shared by every harness that starts the emulator, so they cannot
 * drift apart.
 *
 * <p>The sibling of {@code testutils.bigtable.BigtableEmulatorContainers} and {@code
 * testutils.pubsub.PubSubEmulatorContainers}, which pin the same gcloud CLI image in constants of
 * their own — emulator fixtures are deliberately not unified (issue #27) — so a bump here moves the
 * Firestore harnesses only, and a bump meant to move every emulator edits all three classes.
 *
 * <p>One emulator binary serves both of the module's API surfaces: {@code gcloud emulators
 * firestore start} runs in Native mode by default and in Datastore mode under {@code
 * --database-mode=datastore-mode}. The legacy Datastore emulator is deprecated and is not used.
 */
@Internal
public final class FirestoreEmulatorContainers {

    /**
     * Pinned to the tag {@code BigtableEmulatorContainers} pins, for the retention reason its
     * javadoc gives: gcr.io keeps roughly a year of {@code *-emulators} tags (issue #1196).
     *
     * <p>A bump has to run this module's deviation suites and say what moved.
     */
    private static final DockerImageName IMAGE =
            DockerImageName.parse(
                    "gcr.io/google.com/cloudsdktool/google-cloud-cli:587.0.0-emulators");

    private FirestoreEmulatorContainers() {}

    /**
     * Returns a new, unstarted emulator container running in Firestore Native mode.
     *
     * @return the container
     */
    public static FirestoreEmulatorContainer newContainer() {
        return new FirestoreEmulatorContainer(IMAGE);
    }
}
