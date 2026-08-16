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

package io.github.flink.gcp.connector.spanner.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.MutationGroup;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.sink.serializer.SpannerMutationSerializationSchema;
import io.github.flink.gcp.connector.spanner.sink.writer.CellWeights;
import io.github.flink.gcp.connector.spanner.sink.writer.SpannerDatabaseAccess;
import io.github.flink.gcp.connector.spanner.sink.writer.SpannerDatabaseAccessFactory;
import io.github.flink.gcp.connector.testutils.StubWriterInitContext;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link SpannerMutationsSink}.
 *
 * <p>All of these drive the injectable creation path. The production {@code
 * createWriter(WriterInitContext)} is covered by the emulator integration tests instead of by a
 * unit test pointed at a closed port: unlike the sibling sinks, this one reads the database schema
 * while creating the writer, so a closed port costs the client's whole retry budget — 27 seconds,
 * measured 2026-08-09 — to prove less than a real emulator does.
 */
class SpannerMutationsSinkTest {

    private static final SpannerDatabase DATABASE = SpannerDatabase.of("p", "i", "d");

    @Test
    void opensTheSerializerAndTheFailureHandlerBeforeTheWriterExists() throws Exception {
        RecordingSerializer serializer = new RecordingSerializer();
        RecordingHandler handler = new RecordingHandler();
        CountingAccess access = new CountingAccess();

        sink(serializer, handler).createWriter(new StubWriterInitContext(0), () -> access);

        assertThat(serializer.opened).isEqualTo(1);
        assertThat(handler.opened).isEqualTo(1);
        // Read once at creation: a database whose schema the sink cannot read is a job that should
        // never start, rather than one that dies at its first record.
        assertThat(access.weightsReads.get()).isEqualTo(1);
    }

    @Test
    void releasesTheDatabaseAccessWhenTheWeightsCannotBeRead() {
        RecordingHandler handler = new RecordingHandler();
        CountingAccess access = new CountingAccess();
        access.weightsFailure = new IOException("no permission to read INFORMATION_SCHEMA");

        assertThatThrownBy(
                        () ->
                                sink(new RecordingSerializer(), handler)
                                        .createWriter(new StubWriterInitContext(0), () -> access))
                // Only the type is asserted: the message here is the one this test just wrote, and
                // the production text a misconfigured user actually meets is asserted where it is
                // produced, in SpannerServiceAdapterTest.
                .isInstanceOf(IOException.class);

        // Nothing downstream would ever close these: no writer exists to do it, and a restart
        // would otherwise open one more per attempt.
        assertThat(access.closeCalls.get()).isEqualTo(1);
        assertThat(handler.closed).isEqualTo(1);
    }

    @Test
    void releasesTheFailureHandlerWhenTheAccessCannotBeOpened() {
        RecordingHandler handler = new RecordingHandler();
        SpannerDatabaseAccessFactory failing =
                () -> {
                    throw new IOException("no credentials");
                };

        assertThatThrownBy(
                        () ->
                                sink(new RecordingSerializer(), handler)
                                        .createWriter(new StubWriterInitContext(0), failing))
                .isInstanceOf(IOException.class);

        assertThat(handler.closed).isEqualTo(1);
    }

    @Test
    void wrapsASerializerThatFailsToOpen() {
        SpannerMutationSerializationSchema<String> failing =
                new SpannerMutationSerializationSchema<String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public void open(SerializationSchema.InitializationContext context) {
                        throw new IllegalStateException("no schema");
                    }

                    @Override
                    public Mutation serialize(String element, SinkWriter.Context context) {
                        return null;
                    }
                };

        assertThatThrownBy(
                        () ->
                                sink(failing, new RecordingHandler())
                                        .createWriter(
                                                new StubWriterInitContext(0),
                                                () -> new CountingAccess()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Spanner serialization schema");
    }

    @Test
    void productionWriterLoadsCredentialsBeforeUsingTheContext() {
        String path = "/missing/spanner-service-account.json";
        SpannerMutationsSink<String> sink =
                (SpannerMutationsSink<String>)
                        SpannerSink.<String>builder()
                                .database(DATABASE)
                                .serializer(new RecordingSerializer())
                                .serviceAccountKeyFile(path)
                                .build();

        assertThatThrownBy(() -> sink.createWriter((WriterInitContext) null))
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to load the configured Spanner service-account key file.")
                .hasNoCause();
    }

    private static SpannerMutationsSink<String> sink(
            SpannerMutationSerializationSchema<String> serializer,
            FailureHandler<? super FailedMutation> handler) {
        return (SpannerMutationsSink<String>)
                SpannerSink.<String>builder()
                        .database(DATABASE)
                        .serializer(serializer)
                        .failedMutationHandler(handler)
                        .build();
    }

    /** Counts what the sink did to the access it was handed. */
    private static final class CountingAccess implements SpannerDatabaseAccess {

        private final AtomicInteger weightsReads = new AtomicInteger();
        private final AtomicInteger closeCalls = new AtomicInteger();
        @Nullable private IOException weightsFailure;

        @Override
        public CellWeights readCellWeights() throws IOException {
            weightsReads.incrementAndGet();
            if (weightsFailure != null) {
                throw weightsFailure;
            }
            return CellWeights.empty();
        }

        @Override
        public void batchWrite(List<MutationGroup> groups, GroupOutcomes outcomes) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            closeCalls.incrementAndGet();
        }
    }

    /** Counts its lifecycle calls. */
    private static final class RecordingSerializer
            implements SpannerMutationSerializationSchema<String> {

        private static final long serialVersionUID = 1L;

        private transient int opened;

        @Override
        public void open(SerializationSchema.InitializationContext context) {
            opened++;
        }

        @Override
        public Mutation serialize(String element, SinkWriter.Context context) {
            return Mutation.newInsertOrUpdateBuilder("Orders").set("Id").to(element).build();
        }
    }

    /** Counts its lifecycle calls. */
    private static final class RecordingHandler implements FailureHandler<FailedElement> {

        private static final long serialVersionUID = 1L;

        private transient int opened;
        private transient int closed;

        @Override
        public void open(io.github.flink.gcp.connector.base.failure.FailureHandlerContext context) {
            opened++;
        }

        @Override
        public void handle(FailedElement element) {}

        @Override
        public void close() {
            closed++;
        }
    }
}
