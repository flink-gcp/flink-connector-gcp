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

package io.github.flink.gcp.connector.bigtable.sink.singlerow.writer;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.failure.FailureHandlerContext;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.FailedRequest;
import io.github.flink.gcp.connector.testutils.StubWriterInitContext;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SingleRowRequestSinks}' production writer-creation path. */
class SingleRowRequestSinksTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "orders");

    private static final RowRequestSerializer<String> SERIALIZER =
            (element, context) ->
                    new CheckAndMutateRowRequest(
                            ByteString.copyFromUtf8(element),
                            null,
                            Mutation.create().setCell("cf", "q", element),
                            null);

    @Test
    void opensTheSerializerAndTheHandlerAndHandsTheWriterTheContextsMailboxAndMetrics()
            throws Exception {
        LifecycleRecordingHandler handler = new LifecycleRecordingHandler();
        OpenRecordingSerializer serializer = new OpenRecordingSerializer();
        FakeSingleRowClientFactory factory = new FakeSingleRowClientFactory();
        StubWriterInitContext context = new StubWriterInitContext(0);

        SinkWriter<String> writer =
                SingleRowRequestSinks.createWriter(config(serializer, handler), context, factory);

        assertThat(serializer.opens).isEqualTo(1);
        assertThat(handler.opens).isEqualTo(1);
        assertThat(handler.closes).isZero();
        assertThat(factory.closeCalls).isZero();
        // The writer registered its gauges on the context's group: the path handed the context's
        // metric group through, not a fresh one.
        assertThat(context.getSinkWriterMetricGroup().hasMetric("inFlightRequests")).isTrue();
        writer.close();
        assertThat(handler.closes).isEqualTo(1);
        assertThat(factory.closeCalls).isEqualTo(1);
    }

    @Test
    void closesTheHandlerAndTheFactoryWhenTheWriterCannotBeCreated() throws Exception {
        LifecycleRecordingHandler handler = new LifecycleRecordingHandler();
        // The writer's own precondition is what fails here, after the handler has been opened and
        // the factory built. A non-positive cap is the case that precondition exists for: the
        // options builder rejects it, and Java deserialization does not run the builder.
        FakeSingleRowClientFactory factory = new FakeSingleRowClientFactory();
        SingleRowRequestConfig<String> config =
                new SingleRowRequestConfig<>(
                        (element, context) -> TABLE,
                        SERIALIZER,
                        null,
                        corrupted(BigtableRequestOptions.builder().build()),
                        handler,
                        null,
                        null);

        assertThatThrownBy(
                        () ->
                                SingleRowRequestSinks.createWriter(
                                        config, new StubWriterInitContext(0), factory))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxInFlightRequests must be positive");

        // No writer exists to close either of them, and a restart would otherwise open one more
        // handler and leave one more factory holding whatever it had built per attempt.
        assertThat(handler.opens).isEqualTo(1);
        assertThat(handler.closes).isEqualTo(1);
        assertThat(factory.closeCalls).isEqualTo(1);
    }

    @Test
    void aSerializerThatCannotOpenFailsBeforeTheHandlerIsOpened() {
        LifecycleRecordingHandler handler = new LifecycleRecordingHandler();
        FakeSingleRowClientFactory factory = new FakeSingleRowClientFactory();
        RowRequestSerializer<String> serializer =
                new RowRequestSerializer<String>() {
                    @Override
                    public void open(SerializationSchema.InitializationContext context)
                            throws Exception {
                        throw new IllegalStateException("no schema registry");
                    }

                    @Override
                    public RowRequest<?> serialize(String element, SinkWriter.Context context) {
                        throw new AssertionError("not opened");
                    }
                };

        assertThatThrownBy(
                        () ->
                                SingleRowRequestSinks.createWriter(
                                        config(serializer, handler),
                                        new StubWriterInitContext(0),
                                        factory))
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to open the Bigtable request serializer.")
                .cause()
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("no schema registry");

        assertThat(handler.opens).isZero();
        assertThat(handler.closes).isZero();
        // The factory was built by the caller and handed in; with nothing opened after it, the
        // seam leaves it to the caller as the production overload's own factory would be.
        assertThat(factory.closeCalls).isZero();
    }

    @Test
    void anInterruptedSerializerOpenKeepsTheInterrupt() {
        RowRequestSerializer<String> serializer =
                new RowRequestSerializer<String>() {
                    @Override
                    public void open(SerializationSchema.InitializationContext context)
                            throws Exception {
                        throw new InterruptedException();
                    }

                    @Override
                    public RowRequest<?> serialize(String element, SinkWriter.Context context) {
                        throw new AssertionError("not opened");
                    }
                };

        try {
            assertThatThrownBy(
                            () ->
                                    SingleRowRequestSinks.createWriter(
                                            config(serializer, new LifecycleRecordingHandler()),
                                            new StubWriterInitContext(0),
                                            new FakeSingleRowClientFactory()))
                    .isInstanceOf(IOException.class)
                    .hasMessage("Interrupted while opening the Bigtable request serializer.");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            // Clears it, so the flag does not leak into the next test of this fork.
            Thread.interrupted();
        }
    }

    @Test
    void loadsTheConfiguredCredentialBeforeOpeningSinkRuntimeState() {
        LifecycleRecordingHandler handler = new LifecycleRecordingHandler();
        OpenRecordingSerializer serializer = new OpenRecordingSerializer();
        SingleRowRequestConfig<String> config =
                new SingleRowRequestConfig<>(
                        (element, context) -> TABLE,
                        serializer,
                        null,
                        BigtableRequestOptions.builder().build(),
                        handler,
                        "/missing/mounted-bigtable-key.json",
                        null);

        // The credential is loaded before the factory and before anything is opened, so a
        // missing key file fails with nothing to release and the context is never touched.
        assertThatThrownBy(() -> SingleRowRequestSinks.createWriter(config, null))
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to load the configured Bigtable service-account key file.")
                .hasNoCause();

        assertThat(serializer.opens).isZero();
        assertThat(handler.opens).isZero();
    }

    private static SingleRowRequestConfig<String> config(
            RowRequestSerializer<String> serializer,
            FailureHandler<? super FailedRequest> handler) {
        return new SingleRowRequestConfig<>(
                (element, context) -> TABLE,
                serializer,
                null,
                BigtableRequestOptions.builder().build(),
                handler,
                null,
                null);
    }

    /** Forges what a hand-edited or version-skewed serialized form could carry past the builder. */
    private static BigtableRequestOptions corrupted(BigtableRequestOptions options)
            throws Exception {
        Field field = BigtableRequestOptions.class.getDeclaredField("maxInFlightRequests");
        field.setAccessible(true);
        field.setInt(options, 0);
        return options;
    }

    private static final class OpenRecordingSerializer implements RowRequestSerializer<String> {

        private static final long serialVersionUID = 1L;

        private int opens;

        @Override
        public void open(SerializationSchema.InitializationContext context) {
            opens++;
        }

        @Override
        public RowRequest<?> serialize(String element, SinkWriter.Context context)
                throws IOException {
            return SERIALIZER.serialize(element, context);
        }
    }

    private static final class LifecycleRecordingHandler implements FailureHandler<FailedRequest> {

        private static final long serialVersionUID = 1L;

        private int opens;
        private int closes;

        @Override
        public void open(FailureHandlerContext context) {
            opens++;
        }

        @Override
        public void handle(FailedRequest element) {}

        @Override
        public void close() {
            closes++;
        }
    }
}
