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

package io.github.flink.gcp.connector.testutils.bigquery;

import org.apache.flink.annotation.Internal;

import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryEmulatorContainers.class);

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
     *
     * <p>Waiting for the two ports is enough to reach that dataset, and what makes it enough is the
     * emulator's startup order rather than timing: 0.8.1 applies {@code --dataset} in {@code
     * Server.Load} and enters {@code Server.Serve} — where {@code net.Listen} runs — only
     * afterwards, so neither socket exists until the dataset does (#439). Measured 2026-08-09 over
     * 20 starts of this image: the first HTTP response the emulator ever gave was a 200 on the
     * dataset in 20 of 20, against a control arm started without {@code --dataset} where the same
     * probe read 404 with both ports open in 3 of 3.
     *
     * <p>A host-side connect does land 0.075–0.281 s (median 0.105 s) ahead of any answer from the
     * emulator, Docker's port forwarder accepting before the process is up; the probe got no HTTP
     * response at all inside that window. {@code Wait.forListeningPorts} does not rest on the host
     * side alone in any case — testcontainers 1.21.4 also execs a listen check that blocks inside
     * the container until the port answers, and returns only once it has, which needs the {@code
     * /bin/sh} this image carries.
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
     *
     * <p>Binding a client to a container is logged here because nothing else records it. Every
     * module's {@code log4j2-test.properties} sits at {@code rootLogger.level = WARN}, so
     * testcontainers' own start-up lines never reach a build log, and each harness starts its
     * container afresh per test class — a REST failure therefore names neither the container it
     * addressed nor the port it went to. Two harnesses in the connector module run emulators that
     * differ only by project id, {@code it-project} against {@code itproject}, so a request landing
     * on the wrong one answers 404 rather than anything that reads as a mix-up (#439).
     */
    public static BigQuery restClient(GenericContainer<?> container, String project) {
        // The endpoints first: on a container nobody started, getMappedPort says so and
        // getContainerId returns null, so reading the id first would trade that message for an NPE.
        String rest = restEndpoint(container);
        String grpc = grpcEndpoint(container);
        // Docker's own short form, so the id can be pasted at a `docker` command as it stands.
        String containerId = container.getContainerId();
        LOG.info(
                "BigQuery emulator {} bound for project {}: REST {}, gRPC {}.",
                containerId.substring(0, Math.min(12, containerId.length())),
                project,
                rest,
                grpc);
        return BigQueryOptions.newBuilder()
                .setHost("http://" + restEndpoint(container))
                .setProjectId(project)
                .setCredentials(NoCredentials.getInstance())
                .build()
                .getService();
    }
}
