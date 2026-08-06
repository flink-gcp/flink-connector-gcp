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

package io.github.flink.gcp.connector.testutils.bigquery;

import org.apache.flink.annotation.Internal;

import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * The BigQuery emulator image and its two endpoints, shared by every harness that starts it — the
 * connector module's writer and table harnesses and the SQL module's smoke test — so they cannot
 * drift apart.
 *
 * <p>Two ports, unlike every sibling emulator here: BigQuery serves table metadata over REST and
 * the Storage Write API over gRPC, and a harness pointing only one of them at the emulator would
 * send the other half of a sink to the real service. That is the same fact the connector's two
 * {@code emulatorEndpoint} / {@code emulatorRestEndpoint} builder setters exist for.
 *
 * <p>Testcontainers has no module for this image, so the container type is a plain {@link
 * GenericContainer} rather than a purpose-built one as on the Pub/Sub side.
 */
@Internal
public final class BigQueryEmulatorContainers {

    private static final String IMAGE = "ghcr.io/goccy/bigquery-emulator:0.8.1";

    private static final int REST_PORT = 9050;

    private static final int GRPC_PORT = 9060;

    private BigQueryEmulatorContainers() {}

    /**
     * Returns a new, unstarted emulator container serving {@code project} with {@code dataset}
     * already created.
     *
     * <p>The dataset comes from a command-line flag because the emulator creates it at startup;
     * tables are created by the test, or by the connector under {@code create-if-needed}.
     */
    public static GenericContainer<?> newContainer(String project, String dataset) {
        return new GenericContainer<>(IMAGE)
                .withCommand("--project=" + project, "--dataset=" + dataset)
                .withExposedPorts(REST_PORT, GRPC_PORT)
                .waitingFor(Wait.forListeningPorts(REST_PORT, GRPC_PORT));
    }

    /** The Storage Write API endpoint as {@code host:port}, for {@code emulatorEndpoint}. */
    public static String grpcEndpoint(GenericContainer<?> container) {
        return container.getHost() + ":" + container.getMappedPort(GRPC_PORT);
    }

    /** The table-metadata endpoint as {@code host:port}, for {@code emulatorRestEndpoint}. */
    public static String restEndpoint(GenericContainer<?> container) {
        return container.getHost() + ":" + container.getMappedPort(REST_PORT);
    }

    /**
     * A stock REST client pointed at the emulator.
     *
     * <p>Stock is the point in the SQL module, where the connector under test is the relocated copy
     * in the uber-jar: that the two coexist on one classpath is what an uber-jar exists to provide.
     */
    public static BigQuery restClient(GenericContainer<?> container, String project) {
        return BigQueryOptions.newBuilder()
                .setHost("http://" + restEndpoint(container))
                .setProjectId(project)
                .setCredentials(NoCredentials.getInstance())
                .build()
                .getService();
    }
}
