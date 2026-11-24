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

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;

import java.util.Map;
import java.util.concurrent.Callable;

/**
 * The child-JVM half of {@link EmulatorHostVariableChecksTest}: run with {@code
 * BIGTABLE_EMULATOR_HOST} set, it prints {@link #OK} only if every settings path refuses the
 * variable without an emulator endpoint and accepts it with one, and loading credentials does not
 * fail on it first.
 */
final class EmulatorHostVariableChecksProbe {

    static final String OK = "OK every client family refuses the variable";

    private static final TableDestination TABLE = TableDestination.of("p", "i", "t");

    private EmulatorHostVariableChecksProbe() {}

    public static void main(String[] args) throws Exception {
        String value = System.getenv(EmulatorHostVariableChecks.NAME);
        check(value != null && !value.isEmpty(), "the variable is not set in the child JVM");
        if (value.indexOf(':') > 0) {
            // The variable this checks is the one the client library follows.
            check(
                    value.equals(BigtableDataSettings.newBuilder().stubSettings().getEndpoint()),
                    "the client library does not follow " + EmulatorHostVariableChecks.NAME);
        }

        // Credential loading reads only the client libraries' default scopes, which the variable
        // must not change: before #2322 every load failed on it before any settings were built.
        BigtableCredentials.loadAll(null);
        BigtableCredentials.loadDataAndTableAdmin(null);
        BigtableCredentials.loadTableAdmin(null);
        BigtableCredentials.loadData(null);

        Map<String, Callable<?>> production =
                Map.of(
                        "data", () -> BigtableDataClients.settings(TABLE, null, null, null),
                        "table admin", () -> BigtableClients.tableAdminSettings(null, null),
                        "instance admin", () -> BigtableClients.instanceAdminSettings(null, null),
                        "table admin with credentials",
                                () ->
                                        BigtableClients.tableAdminSettings(
                                                null, NoCredentialsProvider.create()));
        for (Map.Entry<String, Callable<?>> path : production.entrySet()) {
            try {
                path.getValue().call();
                fail("the " + path.getKey() + " settings accepted the variable");
            } catch (IllegalStateException expected) {
                check(
                        expected.getMessage().contains(EmulatorHostVariableChecks.NAME),
                        "the " + path.getKey() + " refusal does not name the variable");
            }
        }

        EmulatorEndpoint endpoint = EmulatorEndpoint.parse("localhost:1", "emulatorEndpoint");
        BigtableDataClients.settings(TABLE, null, endpoint, null);
        BigtableClients.tableAdminSettings(endpoint, null);
        BigtableClients.instanceAdminSettings(endpoint, null);

        System.out.println(OK);
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            fail(message);
        }
    }

    private static void fail(String message) {
        System.out.println("FAIL " + message);
        System.exit(1);
    }
}
