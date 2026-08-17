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

package io.github.flink.gcp.connector.testutils;

import org.apache.flink.annotation.Internal;

import java.util.UUID;

/**
 * Random resource-name helpers for tests whose resources share a namespace — the gated real-GCP
 * suites above all, where concurrent runs must not collide on resource names and a crashed run must
 * leave behind names that identify it, but also an emulator harness giving each test a database of
 * its own.
 */
@Internal
public final class TestNames {

    /**
     * Returns {@code prefix} plus a full random UUID, dash-separated — for resources whose names
     * allow dashes.
     */
    public static String unique(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }

    /**
     * Returns a short random run id (eight hex characters) to suffix onto several related resource
     * names, so one run's resources group together — for names built with underscores where dashes
     * are unwelcome.
     */
    public static String runId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    private TestNames() {}
}
