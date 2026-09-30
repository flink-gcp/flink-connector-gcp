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

package io.github.flink.gcp.connector.firestore.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;
import org.apache.flink.util.InstantiationUtil;

import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.FirestoreMetricNames;
import io.github.flink.gcp.connector.firestore.sink.serializer.FirestoreWriteSerializationSchema;
import io.github.flink.gcp.connector.testutils.StubWriterInitContext;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FirestoreBulkWriterSinkTest {

    private static final DatabaseDestination DATABASE = DatabaseDestination.of("p");

    /**
     * A closed port: the production path builds the real client, and without an emulator endpoint
     * that client would demand application-default credentials — green on a workstation that has
     * them, red in CI (ADR-0064). The client connects lazily, so no request is ever sent here.
     */
    private static final String UNUSED_ENDPOINT = "localhost:1";

    private static final List<String> EVENTS = new ArrayList<>();

    private static final FirestoreWriteSerializationSchema<String> SERIALIZER =
            (element, context) -> FirestoreWrite.set("c/" + element, Map.of());

    @Test
    void theProductionPathOpensAWriterWithItsMetrics() throws Exception {
        StubWriterInitContext context = new StubWriterInitContext(0);
        FirestoreBulkWriterSink<String> sink =
                (FirestoreBulkWriterSink<String>)
                        FirestoreSink.<String>builder()
                                .database(DATABASE)
                                .serializer(SERIALIZER)
                                .emulatorEndpoint(UNUSED_ENDPOINT)
                                .build();

        try (SinkWriter<String> writer = sink.createWriter(context)) {
            assertThat(writer).isNotNull();
            assertThat(
                            context.getSinkWriterMetricGroup()
                                    .<Integer>gaugeValue(FirestoreMetricNames.IN_FLIGHT_WRITES))
                    .isZero();
        }
    }

    @Test
    void aFailedCreationClosesTheHandlerItOpened() {
        EVENTS.clear();
        FirestoreBulkWriterSink<String> sink = sinkWithHandler(recordingHandler());

        assertThatThrownBy(
                        () ->
                                sink.createWriter(
                                        new StubWriterInitContext(0),
                                        policy -> {
                                            throw new IOException("no client");
                                        }))
                .isInstanceOf(IOException.class)
                .hasMessage("no client");
        assertThat(EVENTS).containsExactly("open", "close");
    }

    @Test
    void aSerializerThatCannotOpenFailsTheCreationBeforeAnythingElseOpens() {
        EVENTS.clear();
        FirestoreBulkWriterSink<String> sink =
                (FirestoreBulkWriterSink<String>)
                        FirestoreSink.<String>builder()
                                .database(DATABASE)
                                .serializer(
                                        new FirestoreWriteSerializationSchema<String>() {
                                            @Override
                                            public void open(
                                                    SerializationSchema.InitializationContext c)
                                                    throws Exception {
                                                throw new Exception("cannot open");
                                            }

                                            @Override
                                            public FirestoreWrite serialize(
                                                    String element, SinkWriter.Context context) {
                                                return null;
                                            }
                                        })
                                .failedWriteHandler(recordingHandler())
                                .build();

        assertThatThrownBy(() -> sink.createWriter(new StubWriterInitContext(0), p -> null))
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to open the Firestore serialization schema.");
        assertThat(EVENTS).isEmpty();
    }

    @Test
    void theProductionPathLoadsCredentialsBeforeUsingTheContext() {
        Sink<String> sink =
                FirestoreSink.<String>builder()
                        .database(DATABASE)
                        .serializer(SERIALIZER)
                        .serviceAccountKeyFile("/does/not/exist.json")
                        .build();

        assertThatThrownBy(() -> sink.createWriter((WriterInitContext) null))
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to load the configured Firestore service-account key file.");
    }

    @Test
    void lineageNamesNoDatasetBeforeOrAfterSerialization() throws Exception {
        Sink<String> sink =
                FirestoreSink.<String>builder()
                        .database(DATABASE)
                        .serializer(SERIALIZER)
                        .serviceAccountKeyFile("/lineage-test/must-not-read-credentials.json")
                        .build();

        for (Sink<String> candidate : List.of(sink, InstantiationUtil.clone(sink))) {
            LineageVertex vertex = ((LineageVertexProvider) candidate).getLineageVertex();
            assertThat(vertex).isNotInstanceOf(SourceLineageVertex.class);
            assertThat(vertex.datasets()).isEmpty();
        }
    }

    private static FirestoreBulkWriterSink<String> sinkWithHandler(
            FailureHandler<FailedWrite> handler) {
        return (FirestoreBulkWriterSink<String>)
                FirestoreSink.<String>builder()
                        .database(DATABASE)
                        .serializer(SERIALIZER)
                        .failedWriteHandler(handler)
                        .build();
    }

    private static FailureHandler<FailedWrite> recordingHandler() {
        return new FailureHandler<FailedWrite>() {
            @Override
            public void open(io.github.flink.gcp.connector.base.failure.FailureHandlerContext c) {
                EVENTS.add("open");
            }

            @Override
            public void handle(FailedWrite element) {}

            @Override
            public void close() {
                EVENTS.add("close");
            }
        };
    }
}
