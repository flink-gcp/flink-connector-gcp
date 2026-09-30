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

package io.github.flink.gcp.connector.firestore.sink.writer;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.WriteResult;
import com.google.firestore.v1.BatchWriteRequest;
import io.github.flink.gcp.connector.firestore.AbstractFirestoreEmulatorITCase;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWrite;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWriterOptions;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.MethodDescriptor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Holds the estimate equal to the size of the {@code Write} the client library actually sends,
 * captured from the {@code BatchWrite} requests of the production {@link BulkWriterDatabaseAccess}.
 * {@link DocumentSizeEstimatorTest} compares the same writes with a reconstruction of the library's
 * conversion; this test is what would catch a misreading the reconstruction and the estimator
 * share. Whether the emulator accepts each write does not matter: only the request is compared.
 */
class DocumentSizeEstimatorITCase extends AbstractFirestoreEmulatorITCase {

    private final List<BatchWriteRequest> requests = new CopyOnWriteArrayList<>();

    private BulkWriterDatabaseAccess access;

    @BeforeEach
    void openCapturingAccess() {
        // The library reads this variable whenever setEmulatorHost was not called, and then
        // replaces the channel below, interceptor included, with one of its own.
        assertThat(System.getenv("FIRESTORE_EMULATOR_HOST"))
                .as(
                        "FIRESTORE_EMULATOR_HOST is set, and it would replace this test's"
                                + " capturing channel; unset it to run this test")
                .isNull();
        ClientInterceptor capture =
                new ClientInterceptor() {
                    @Override
                    public <Q, R> ClientCall<Q, R> interceptCall(
                            MethodDescriptor<Q, R> method, CallOptions options, Channel next) {
                        return new ForwardingClientCall.SimpleForwardingClientCall<Q, R>(
                                next.newCall(method, options)) {
                            @Override
                            public void sendMessage(Q message) {
                                if (message instanceof BatchWriteRequest) {
                                    requests.add((BatchWriteRequest) message);
                                }
                                super.sendMessage(message);
                            }
                        };
                    }
                };
        // What setEmulatorHost configures, plus the interceptor it leaves no room for.
        FirestoreOptions options =
                FirestoreOptions.newBuilder()
                        .setProjectId(PROJECT)
                        .setChannelProvider(
                                InstantiatingGrpcChannelProvider.newBuilder()
                                        .setEndpoint(emulatorEndpoint())
                                        .setChannelConfigurator(
                                                builder ->
                                                        builder.usePlaintext().intercept(capture))
                                        .build())
                        .setCredentialsProvider(
                                FixedCredentialsProvider.create(
                                        new FirestoreOptions.EmulatorCredentials()))
                        .build();
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        access =
                new BulkWriterDatabaseAccess(
                        options.getService(),
                        executor,
                        DefaultFirestoreDatabaseAccessFactory.bulkWriterOptions(
                                FirestoreWriterOptions.builder().build(), executor),
                        new BulkWriterRetryPolicy(
                                1,
                                new BulkWriterRetryPolicy.Observer() {
                                    @Override
                                    public void attemptFailed() {}

                                    @Override
                                    public void retrying() {}
                                }));
    }

    @AfterEach
    void closeAccess() throws Exception {
        access.close();
    }

    @Test
    void theEstimateIsTheSizeOfTheWriteTheLibrarySends() throws Exception {
        DocumentSizeEstimator estimator = new DocumentSizeEstimator(database());
        for (FirestoreWrite write : DocumentSizeEstimatorTest.writesOfEveryShape()) {
            requests.clear();
            ApiFuture<WriteResult> result = access.submit(write);
            access.sendOutstanding();
            try {
                result.get(30, TimeUnit.SECONDS);
            } catch (ExecutionException refused) {
                // The request was sent; its answer is not what this test compares.
            }

            assertThat(requests).as("the requests sent for %s", write).hasSize(1);
            BatchWriteRequest sent = requests.get(0);
            assertThat(sent.getWritesCount()).isEqualTo(1);
            assertThat(estimator.estimate(write))
                    .as("%s", write)
                    .isEqualTo(
                            BatchWriteRequest.newBuilder()
                                    .addWrites(sent.getWrites(0))
                                    .build()
                                    .getSerializedSize());
        }
    }
}
