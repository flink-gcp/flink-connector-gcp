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
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;

import java.time.Duration;

/**
 * The BigQuery emulator image and its two endpoints, shared by every harness that starts the
 * emulator so they cannot drift apart.
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

    /**
     * Testcontainers' own per-strategy default, restated because nesting a strategy silently
     * replaces it: {@code WaitAllStrategy} hands each child whatever ceiling it is carrying, which
     * defaults to 30 s rather than the 60 s a strategy passed straight to {@code waitingFor} keeps.
     */
    private static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(60);

    private BigQueryEmulatorContainers() {}

    /**
     * Returns a new, unstarted emulator container serving {@code project} with {@code dataset}
     * already created.
     *
     * <p>The dataset comes from a command-line flag because the emulator creates it at startup;
     * tables are created by the test, or by the connector under {@code create-if-needed}.
     *
     * <p>Waiting for the two ports is enough to reach that dataset — on a container whose ports are
     * its own, which is the paragraph after next — and what makes it enough is the emulator's
     * startup order rather than timing: 0.8.1 applies {@code --dataset} in {@code Server.Load} and
     * enters {@code Server.Serve} — where {@code net.Listen} runs — only afterwards, so neither
     * socket exists until the dataset does (#439). Measured 2026-08-09 over 20 starts of this
     * image: the first HTTP response the emulator ever gave was a 200 on the dataset in 20 of 20,
     * against a control arm started without {@code --dataset} where the same probe read 404 with
     * both ports open in 3 of 3.
     *
     * <p>A host-side connect does land 0.075–0.281 s (median 0.105 s) ahead of any answer from the
     * emulator, Docker's port forwarder accepting before the process is up; the probe got no HTTP
     * response at all inside that window. {@code Wait.forListeningPorts} does not rest on the host
     * side alone in any case — testcontainers 1.21.4 also execs a listen check that blocks inside
     * the container until the port answers, and returns only once it has, which needs the {@code
     * /bin/sh} this image carries.
     *
     * <p>What that leaves open, and what the HTTP probe below closes, is <em>who</em> owns the
     * published host port. Docker publishes on the wildcard address, which coexists with a process
     * already listening on {@code 127.0.0.1:<port>} — and that process keeps the more specific
     * bind, so a client resolving {@code localhost} to the IPv4 loopback reaches it instead of the
     * container. The JVM does resolve that way by default — {@code InetAddress.getAllByName(
     * "localhost")} returns {@code 127.0.0.1} ahead of {@code ::1}, absent {@code
     * java.net.preferIPv6Addresses}. Meanwhile the container is healthy, the in-container listen
     * check passes, and the host-side connect passes because something did accept it.
     *
     * <p>Reproduced end to end 2026-08-22 on Docker Desktop for macOS with this project's JDK,
     * holding {@code 127.0.0.1:<port>} while Docker published the same port on {@code 0.0.0.0}: the
     * container ran and served, {@code curl} read 200 from it — it reached {@code ::1} — and a Java
     * client on the same URL read the other process's {@code 401}. <b>On a default setup that
     * disagreement is the diagnostic</b>: an endpoint answering correctly to {@code curl} and
     * wrongly to the test is this, not a connector bug. Each leg of it is configurable — Docker's
     * default bind address, the JVM's address preference, and whether {@code curl} has IPv6 to
     * prefer — so treat it as the tell on the setup it was measured on rather than as a law. It is
     * what #1003 turned out to be, an unrelated desktop application's loopback API answering {@code
     * 401 Unauthorized} to a {@code tables.insert} the emulator never saw. The probe below is
     * itself a Java client of the same URL, so it resolves the way the test does and sees what the
     * test would see, whichever way that is. Asking the emulator to identify itself turns that into
     * a container that fails to start, naming the URL it could not satisfy, instead of an
     * unattributable failure inside a test. The 2026-08-09 control arm above <em>is</em> this
     * probe, which is why no second measurement is recorded here: a 200 on the dataset the
     * container was started with is exactly what that arm showed a dataset-carrying emulator gives
     * and a dataset-less one does not.
     *
     * <p>The body is checked as well as the status, because a status alone identifies nothing: any
     * server answering 200 to an unknown path would pass. Verified by dropping {@code --dataset}
     * from the command below, which fails the container's start with {@code
     * ContainerLaunchException: Timed out waiting for URL to be accessible (…/datasets/… should
     * return HTTP [200])} rather than handing a test an endpoint that leads elsewhere.
     *
     * <p>The gRPC port has no equivalent probe, so a {@code GRPC_PORT} shadowed the same way still
     * surfaces as whatever the Storage Write API client makes of a stranger's answer.
     */
    public static GenericContainer<?> newContainer(String project, String dataset) {
        return new GenericContainer<>(IMAGE)
                .withCommand("--project=" + project, "--dataset=" + dataset)
                .withExposedPorts(REST_PORT, GRPC_PORT)
                .waitingFor(
                        // Individual timeouts rather than an outer one: WITH_OUTER_TIMEOUT wraps
                        // both children in a ducttape timeout that cannot fire later than theirs
                        // and discards whichever one it interrupted, so what reaches a reader is a
                        // bare TimeoutException. Measured: under that mode the failure names
                        // nothing, under this one it is "Timed out waiting for URL to be accessible
                        // (… should return HTTP [200])". It would have swallowed the port check's
                        // own message too, which reaches a reader today. The cost is that a
                        // container hanging both checks takes them in sequence.
                        new WaitAllStrategy(WaitAllStrategy.Mode.WITH_INDIVIDUAL_TIMEOUTS_ONLY)
                                .withStrategy(
                                        Wait.forListeningPorts(REST_PORT, GRPC_PORT)
                                                .withStartupTimeout(STARTUP_TIMEOUT))
                                .withStrategy(
                                        Wait.forHttp(
                                                        "/bigquery/v2/projects/"
                                                                + project
                                                                + "/datasets/"
                                                                + dataset)
                                                .forPort(REST_PORT)
                                                .forStatusCode(200)
                                                // The emulator echoes a datasetReference back, so
                                                // the body is what names it; a status alone would
                                                // be satisfied by anything answering 200 to an
                                                // unknown path, which is the same defect in a
                                                // narrower form. The field name is checked beside
                                                // the two ids because those two appear in the
                                                // request path as well, so a service that merely
                                                // echoes what it was asked for would otherwise
                                                // pass. Substrings rather than parsed JSON: this
                                                // module has no JSON dependency, and the pinned
                                                // image's response shape is pinned with it.
                                                .forResponsePredicate(
                                                        body ->
                                                                body.contains("datasetReference")
                                                                        && body.contains(project)
                                                                        && body.contains(dataset))
                                                .withStartupTimeout(STARTUP_TIMEOUT)));
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
