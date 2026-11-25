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

package io.github.flink.gcp.connector.datastore.source.batch.enumerator;

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.datastore.source.DatastoreSourceConfig;
import io.github.flink.gcp.connector.datastore.source.TestSources;
import io.github.flink.gcp.connector.testutils.GrpcEventLoopThreads;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The planner's close ends a plan's call in flight: the planner runs on the JobManager, where Flink
 * closes the enumerator on a thread of its own, and a call left waiting would hold the plan's
 * client, and its lease, past that close (docs/adr/0208).
 */
@Timeout(60)
class ClientQueryPlannerCloseTest {

    @BeforeEach
    void noLeaseIsOpenBeforehand() {
        assertThat(GrpcEventLoops.isRunning())
                .as("a lease is open before the test: an earlier class in this fork leaked one")
                .isFalse();
    }

    @AfterEach
    void noLeaseIsLeftOpen() {
        assertThat(GrpcEventLoops.isRunning()).as("a lease was left open").isFalse();
    }

    /**
     * Against an endpoint that accepts the connection and never answers, the plan's first call
     * waits for as long as the library's retry settings allow, tens of seconds; the planner's close
     * cancels it at once and releases the client's lease.
     */
    @Test
    void closeEndsAPlanWhoseCallIsInFlight() throws Exception {
        try (ServerSocket silent = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
            CountDownLatch connected = new CountDownLatch(1);
            List<Socket> accepted = new ArrayList<>();
            Thread acceptor =
                    new Thread(
                            () -> {
                                try {
                                    while (true) {
                                        Socket socket = silent.accept();
                                        synchronized (accepted) {
                                            accepted.add(socket);
                                        }
                                        connected.countDown();
                                    }
                                } catch (IOException e) {
                                    // The server socket closed: the test is over.
                                }
                            },
                            "silent-endpoint");
            acceptor.setDaemon(true);
            acceptor.start();
            String target =
                    InetAddress.getLoopbackAddress().getHostAddress() + ":" + silent.getLocalPort();
            DatastoreSourceConfig<String> config =
                    TestSources.kindConfig(
                            builder -> builder.emulatorEndpoint(target).splitCount(1));
            ClientQueryPlanner planner =
                    new ClientQueryPlanner(EmulatorEndpoint.parse(target, "emulatorEndpoint"));
            CompletableFuture<Throwable> plan =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    planner.plan(config, 1);
                                    return null;
                                } catch (Throwable t) {
                                    return t;
                                }
                            });
            try {
                assertThat(connected.await(20, TimeUnit.SECONDS))
                        .as("the plan's client connected")
                        .isTrue();
                assertThat(GrpcEventLoops.isRunning())
                        .as("the plan's client holds a lease while its call waits")
                        .isTrue();
                assertThat(GrpcEventLoopThreads.awaitAlive())
                        .as("a %s* thread carries the plan's call", GrpcEventLoopThreads.PREFIX)
                        .isTrue();

                long closing = System.nanoTime();
                planner.close();

                // The connection opens as the call starts, and the runner records the call under
                // its monitor before close() can take it, so the close cancels that call through
                // its future rather than leaving it to the client's shutdown.
                assertThat(plan.get(10, TimeUnit.SECONDS))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining("was closed while a call was in flight");
                assertThat(Duration.ofNanos(System.nanoTime() - closing))
                        .as(
                                "the close and the plan's failure, against the client's 5 s shutdown wait")
                        .isLessThan(Duration.ofSeconds(4));
                assertThat(GrpcEventLoops.isRunning())
                        .as("the plan's lease was released")
                        .isFalse();
            } finally {
                planner.close();
                synchronized (accepted) {
                    for (Socket socket : accepted) {
                        socket.close();
                    }
                }
            }
        }
    }
}
