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

package io.github.flink.gcp.connector.bigquery.sink.storage;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.UserCodeClassLoader;

import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.testutils.StubWriterInitContext;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Does the default-stream sink offer its job's class loader to the gRPC event-loop group?
 *
 * <p>Base's {@code GrpcEventLoopsTest} covers what the pin does. A test can only observe that the
 * writer asked, since the connector here is loaded by the test's class loader, not the job's, and
 * the pin then leaves the group alone; without the call, the group would stop under the Storage
 * Write API pool's connections in a job that ships the connector (ADR-0208, #2268).
 */
class BigQueryDefaultStreamSinkEventLoopPinTest {

    @Test
    void creatingAWriterOffersTheJobsClassLoaderToThePin() throws Exception {
        AtomicInteger asked = new AtomicInteger();
        List<String> registered = new CopyOnWriteArrayList<>();
        StubWriterInitContext context =
                new StubWriterInitContext(0) {
                    @Override
                    public UserCodeClassLoader getUserCodeClassLoader() {
                        UserCodeClassLoader loader = super.getUserCodeClassLoader();
                        return new UserCodeClassLoader() {
                            @Override
                            public ClassLoader asClassLoader() {
                                asked.incrementAndGet();
                                return loader.asClassLoader();
                            }

                            @Override
                            public void registerReleaseHookIfAbsent(
                                    String releaseHookName, Runnable releaseHook) {
                                registered.add(releaseHookName);
                                loader.registerReleaseHookIfAbsent(releaseHookName, releaseHook);
                            }
                        };
                    }
                };

        SinkWriter<String> writer =
                BigQuerySink.<String>builder()
                        .writeMethod(WriteMethod.STORAGE_API_AT_LEAST_ONCE)
                        .table(TableDestination.of("p", "d", "t"))
                        .serializer(new NameValueRowSerializer())
                        // Nothing listens there; the writer opens no client before its first row,
                        // and both endpoints keep any client that does open off the service.
                        .emulatorEndpoint("localhost:1")
                        .emulatorRestEndpoint("localhost:1")
                        .build()
                        .createWriter(context);
        writer.close();

        assertThat(asked).as("the pin read the job's class loader").hasPositiveValue();
        assertThat(registered)
                .as("the connector is not the job's here, so nothing was pinned")
                .isEmpty();
    }
}
