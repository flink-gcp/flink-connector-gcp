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

package io.github.flink.gcp.connector.firestore;

import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;

/**
 * Builds a service client's options with no credentials configured, for {@code
 * FirestoreClientsTest}, which runs it in a JVM of its own whose environment finds no Application
 * Default Credentials: that lookup reads the process environment, which a test cannot set.
 */
public final class NoCredentialsOptionsProbe {

    private NoCredentialsOptionsProbe() {}

    /**
     * Builds the options, exiting with 0 when they build and with 1, after printing the failure,
     * when they do not.
     *
     * @param args unused
     */
    public static void main(String[] args) {
        DatabaseDestination database = DatabaseDestination.of("p", "orders");
        GrpcEventLoops.Lease lease = GrpcEventLoops.acquire();
        try {
            FirestoreClients.options(
                    database,
                    null,
                    FirestoreClients.settings(database, null, null, null, variable -> null),
                    lease,
                    variable -> null);
        } catch (RuntimeException e) {
            e.printStackTrace(System.out);
            System.exit(1);
        } finally {
            lease.close();
        }
        System.exit(0);
    }
}
