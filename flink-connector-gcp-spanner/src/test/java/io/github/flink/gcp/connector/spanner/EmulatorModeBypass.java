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

package io.github.flink.gcp.connector.spanner;

/**
 * Reaches {@link SpannerClients#bypassEmulatorModeForTesting(boolean)} from tests in other
 * packages, so that a job they run records its client metrics against the emulator. Restore it with
 * {@code false} after the job; the switch is JVM-wide.
 */
public final class EmulatorModeBypass {

    private EmulatorModeBypass() {}

    /**
     * Leaves or restores the client's emulator mode for every client built afterwards in this JVM.
     *
     * @param bypassed whether to leave the emulator mode
     */
    public static void set(boolean bypassed) {
        SpannerClients.bypassEmulatorModeForTesting(bypassed);
    }
}
