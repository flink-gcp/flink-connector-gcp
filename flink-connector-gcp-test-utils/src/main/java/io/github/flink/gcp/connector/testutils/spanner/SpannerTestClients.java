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

import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;

/**
 * Stock Spanner clients pointed at an emulator endpoint.
 *
 * <p>Harness-owned and deliberately <em>unshaded</em>: in the SQL module's smoke test this runs
 * beside the uber-jar's relocated copy, so that the two demonstrably coexist on one classpath.
 */
@Internal
public final class SpannerTestClients {

    private SpannerTestClients() {}

    /**
     * Opens a client against the emulator at {@code endpoint} ({@code host:port}).
     *
     * <p>The returned service belongs to the caller and must be closed.
     *
     * @param endpoint the emulator endpoint
     * @param project the project that resources are addressed under
     * @return the stock client
     */
    public static Spanner forEmulator(String endpoint, String project) {
        return SpannerOptions.newBuilder()
                .setProjectId(project)
                .setEmulatorHost(endpoint)
                .build()
                .getService();
    }
}
