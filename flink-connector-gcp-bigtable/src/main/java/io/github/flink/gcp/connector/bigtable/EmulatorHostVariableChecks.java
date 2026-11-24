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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;

import javax.annotation.Nullable;

/**
 * Refuses to open a Bigtable client while the {@code BIGTABLE_EMULATOR_HOST} environment variable
 * is set to a non-empty value and no emulator endpoint is configured.
 *
 * <p>The connector reaches an emulator only through its own emulator endpoint. The client library
 * reads the variable on its own: {@code BigtableDataSettings.newBuilder()} (google-cloud-bigtable
 * 2.89.0) returns an emulator builder when it is set, with a plaintext channel and no credentials,
 * so a value it can parse, left on a TaskManager, would point a production job's data client at
 * that host, with nothing but an INFO line from the library to say so; one it cannot parse fails in
 * its parser. The admin clients' settings never read it. Rather than let the two families diverge,
 * the connector fails before either is built. A configured emulator endpoint takes precedence, and
 * the variable is not consulted then.
 */
@Internal
final class EmulatorHostVariableChecks {

    /** The variable the client library reads. */
    @VisibleForTesting static final String NAME = "BIGTABLE_EMULATOR_HOST";

    private EmulatorHostVariableChecks() {}

    /**
     * Checks the process environment.
     *
     * @param emulatorEndpoint the configured emulator endpoint, or {@code null} for production
     * @throws IllegalStateException if the variable is set to a non-empty value and no emulator
     *     endpoint is configured
     */
    static void checkUnsetFor(@Nullable EmulatorEndpoint emulatorEndpoint) {
        // Not read at all beside an endpoint, which a security policy denying it then cannot fail.
        if (emulatorEndpoint == null) {
            check(System.getenv(NAME), null);
        }
    }

    /**
     * Checks a value of the variable; the seam {@link #checkUnsetFor(EmulatorEndpoint)} reads the
     * process environment through.
     *
     * @param value the variable's value, or {@code null} if it is unset
     * @param emulatorEndpoint the configured emulator endpoint, or {@code null} for production
     * @throws IllegalStateException if {@code value} is non-empty and no emulator endpoint is
     *     configured
     */
    @VisibleForTesting
    static void check(@Nullable String value, @Nullable EmulatorEndpoint emulatorEndpoint) {
        // An empty value is unset to the client library too.
        if (emulatorEndpoint != null || value == null || value.isEmpty()) {
            return;
        }
        throw new IllegalStateException(
                String.format(
                        "The %s environment variable is set to '%s', but no Bigtable emulator"
                                + " endpoint is configured. The Bigtable client library would point"
                                + " this connector's data client at that host, or reject the value if"
                                + " it cannot parse it, so the connector does not open a client."
                                + " To use an emulator, configure the"
                                + " connector's emulator endpoint; otherwise unset the variable.",
                        NAME, value));
    }
}
