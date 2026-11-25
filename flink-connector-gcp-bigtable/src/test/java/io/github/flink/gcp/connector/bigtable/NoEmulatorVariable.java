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

package io.github.flink.gcp.connector.bigtable;

/**
 * Refuses to start a real-GCP harness while {@code BIGTABLE_EMULATOR_HOST} is set to a non-empty
 * value.
 *
 * <p>The harnesses open their admin clients as the V2 clients, whose settings never read the
 * variable, and their data clients through {@code BigtableDataSettings}, which follows it. With the
 * variable set, the admin calls would provision and delete billed instances in the project while
 * the data calls went to the variable's host.
 */
public final class NoEmulatorVariable {

    private NoEmulatorVariable() {}

    /**
     * Fails unless {@code BIGTABLE_EMULATOR_HOST} is unset or empty.
     *
     * @param harness what is starting, for the message
     * @throws IllegalStateException if the variable is set to a non-empty value
     */
    public static void require(String harness) {
        String value = System.getenv("BIGTABLE_EMULATOR_HOST");
        if (value != null && !value.isEmpty()) {
            throw new IllegalStateException(
                    harness
                            + " runs against real Bigtable, but BIGTABLE_EMULATOR_HOST is set to '"
                            + value
                            + "'; unset it.");
        }
    }
}
