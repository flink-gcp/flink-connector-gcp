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

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.spanner.admin.instance.v1.InstanceAdminGrpc;
import com.google.spanner.admin.instance.v1.ListInstanceConfigsRequest;
import com.google.spanner.admin.instance.v1.ListInstanceConfigsResponse;
import com.google.spanner.v1.BatchCreateSessionsRequest;
import com.google.spanner.v1.BatchCreateSessionsResponse;
import com.google.spanner.v1.BeginTransactionRequest;
import com.google.spanner.v1.CreateSessionRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.PartialResultSet;
import com.google.spanner.v1.Session;
import com.google.spanner.v1.SpannerGrpc;
import com.google.spanner.v1.Transaction;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A plaintext Spanner endpoint that answers the client's emulator check and leaves the calls a
 * component starts with open until the server is closed: {@code CreateSession} and {@code
 * BatchCreateSessions} (unless the server was started to answer sessions), {@code BeginTransaction}
 * and {@code ExecuteStreamingSql}. Every other Spanner method fails at once as unimplemented. For
 * tests that close a component while its call is in flight; {@link #calls()} names what arrived.
 */
public final class StallingSpannerServer implements AutoCloseable {

    private final Server server;
    private final List<String> calls = new CopyOnWriteArrayList<>();
    private final CountDownLatch firstCall = new CountDownLatch(1);
    private final boolean answerSessions;

    private StallingSpannerServer(boolean answerSessions) throws IOException {
        this.answerSessions = answerSessions;
        server =
                NettyServerBuilder.forAddress(new InetSocketAddress("localhost", 0))
                        .addService(new InstanceAdmin())
                        .addService(new Stalling())
                        .build()
                        .start();
    }

    /**
     * Starts a server on a free port.
     *
     * @return the running server
     * @throws IOException if it cannot be started
     */
    public static StallingSpannerServer start() throws IOException {
        return new StallingSpannerServer(false);
    }

    /**
     * Starts a server on a free port that creates the sessions it is asked for, so that the call
     * left open is the first one made on a session.
     *
     * @return the running server
     * @throws IOException if it cannot be started
     */
    public static StallingSpannerServer startAnsweringSessions() throws IOException {
        return new StallingSpannerServer(true);
    }

    /** Returns the endpoint a component reaches the server through, as an emulator. */
    public EmulatorEndpoint endpoint() {
        return EmulatorEndpoint.parse("localhost:" + server.getPort(), "emulatorEndpoint");
    }

    /**
     * Waits for the first data call to arrive.
     *
     * @param seconds how long to wait
     * @return whether one arrived
     * @throws InterruptedException if interrupted while waiting
     */
    public boolean awaitStalledCall(long seconds) throws InterruptedException {
        return firstCall.await(seconds, TimeUnit.SECONDS);
    }

    /** Returns the data calls received so far, by method name. */
    public List<String> calls() {
        return calls;
    }

    /**
     * Asserts that a failure is the client's answer to a cancelled context: a {@link
     * SpannerException} with {@link ErrorCode#CANCELLED} somewhere in its cause chain.
     *
     * @param failure the failure a component's call ended with
     */
    public static void assertCancelled(Throwable failure) {
        Throwable cause = failure;
        while (cause != null && !(cause instanceof SpannerException)) {
            cause = cause.getCause();
        }
        assertThat(cause)
                .as("a SpannerException in the cause chain of %s", failure)
                .isInstanceOfSatisfying(
                        SpannerException.class,
                        spanner ->
                                assertThat(spanner.getErrorCode()).isEqualTo(ErrorCode.CANCELLED));
    }

    @Override
    public void close() throws InterruptedException {
        server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
    }

    private void stall(String method) {
        calls.add(method);
        firstCall.countDown();
    }

    /** Answers the client's check that an emulator is listening. */
    private static final class InstanceAdmin extends InstanceAdminGrpc.InstanceAdminImplBase {
        @Override
        public void listInstanceConfigs(
                ListInstanceConfigsRequest request,
                StreamObserver<ListInstanceConfigsResponse> responseObserver) {
            responseObserver.onNext(ListInstanceConfigsResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }
    }

    /** Leaves the calls a component starts with open. */
    private final class Stalling extends SpannerGrpc.SpannerImplBase {
        @Override
        public void createSession(
                CreateSessionRequest request, StreamObserver<Session> responseObserver) {
            if (answerSessions) {
                responseObserver.onNext(
                        Session.newBuilder()
                                .setName(request.getDatabase() + "/sessions/stalling")
                                .setMultiplexed(request.getSession().getMultiplexed())
                                .build());
                responseObserver.onCompleted();
                return;
            }
            stall("CreateSession");
        }

        @Override
        public void batchCreateSessions(
                BatchCreateSessionsRequest request,
                StreamObserver<BatchCreateSessionsResponse> responseObserver) {
            stall("BatchCreateSessions");
        }

        @Override
        public void beginTransaction(
                BeginTransactionRequest request, StreamObserver<Transaction> responseObserver) {
            stall("BeginTransaction");
        }

        @Override
        public void executeStreamingSql(
                ExecuteSqlRequest request, StreamObserver<PartialResultSet> responseObserver) {
            stall("ExecuteStreamingSql");
        }
    }
}
