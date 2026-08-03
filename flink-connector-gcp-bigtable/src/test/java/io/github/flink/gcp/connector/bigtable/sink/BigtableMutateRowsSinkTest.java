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

package io.github.flink.gcp.connector.bigtable.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.failure.FailureHandlerContext;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.serializer.BigtableSerializationSchema;
import io.github.flink.gcp.connector.testutils.StubWriterInitContext;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BigtableMutateRowsSink}'s production writer-creation path. */
class BigtableMutateRowsSinkTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "orders");

    private static final BigtableSerializationSchema<String> SERIALIZER =
            (element, context) -> RowMutationEntry.create(element).setCell("cf", "q", element);

    @Test
    void closesTheFailureHandlerWhenTheWriterCannotBeCreated() throws Exception {
        LifecycleRecordingHandler handler = new LifecycleRecordingHandler();
        // The writer's own precondition is what fails here, after the handler has been opened and
        // the batcher built. A non-positive in-flight cap is the case that precondition exists
        // for: the options builder rejects it, and Java deserialization does not run the builder.
        Sink<String> sink =
                BigtableSink.<String>builder()
                        .table(TABLE)
                        .serializer(SERIALIZER)
                        .failedMutationHandler(handler)
                        .writerOptions(forgedOptions("maxInFlightMutations", 0))
                        // A well-formed endpoint the client never has to reach: gRPC connects
                        // lazily, so the batcher is built offline and the failure stays the
                        // writer's.
                        .emulatorEndpoint("localhost:8086")
                        .build();

        assertThatThrownBy(() -> sink.createWriter(new StubWriterInitContext(0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxInFlightMutations must be positive");

        // No writer exists to close it, and a restart would otherwise open one more per attempt.
        // The batcher built just before the failure is released on the same path.
        assertThat(handler.opens).isEqualTo(1);
        assertThat(handler.closes).isEqualTo(1);
    }

    @Test
    void failsWithoutTouchingTheHandlerWhenTheSerializerCannotBeOpened() {
        LifecycleRecordingHandler handler = new LifecycleRecordingHandler();
        Sink<String> sink =
                BigtableSink.<String>builder()
                        .table(TABLE)
                        .serializer(new FailingSerializationSchema())
                        .failedMutationHandler(handler)
                        .emulatorEndpoint("localhost:8086")
                        .build();

        assertThatThrownBy(() -> sink.createWriter(new StubWriterInitContext(0)))
                .hasMessageContaining("Failed to open the Bigtable serialization schema.");

        assertThat(handler.opens).isZero();
        assertThat(handler.closes).isZero();
    }

    /**
     * Returns options carrying a value their builder rejects, as a deserialized options object can.
     */
    private static BigtableWriterOptions forgedOptions(String name, int value) throws Exception {
        BigtableWriterOptions options = BigtableWriterOptions.defaults();
        Field field = BigtableWriterOptions.class.getDeclaredField(name);
        field.setAccessible(true);
        field.setInt(options, value);
        return options;
    }

    /** A serialization schema whose {@code open} fails, so nothing after it may run. */
    private static final class FailingSerializationSchema
            implements BigtableSerializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void open(SerializationSchema.InitializationContext context) {
            throw new IllegalStateException("boom");
        }

        @Override
        public RowMutationEntry serialize(String element, SinkWriter.Context context) {
            return RowMutationEntry.create(element);
        }
    }

    /** A handler that records its lifecycle calls and drops everything. */
    private static final class LifecycleRecordingHandler implements FailureHandler<FailedMutation> {

        private static final long serialVersionUID = 1L;

        private int opens;
        private int closes;

        @Override
        public void open(FailureHandlerContext context) {
            opens++;
        }

        @Override
        public void handle(FailedMutation element) {}

        @Override
        public void close() {
            closes++;
        }
    }
}
