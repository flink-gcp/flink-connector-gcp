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

package io.github.flink.gcp.connector.testutils.spanner;

import org.apache.flink.annotation.Internal;

import org.testcontainers.containers.SpannerEmulatorContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The Spanner emulator image shared by every harness that starts the emulator, so they cannot drift
 * apart.
 */
@Internal
public final class SpannerEmulatorContainers {

    /**
     * Pinned, and above the floor that matters: {@code BatchWrite} landed in v1.5.31 (emulator
     * issue #172, closed 2025-06-20).
     */
    private static final DockerImageName IMAGE =
            DockerImageName.parse("gcr.io/cloud-spanner-emulator/emulator:1.5.56");

    private SpannerEmulatorContainers() {}

    /** Returns a new, unstarted emulator container. */
    public static SpannerEmulatorContainer newContainer() {
        return new SpannerEmulatorContainer(IMAGE);
    }
}
