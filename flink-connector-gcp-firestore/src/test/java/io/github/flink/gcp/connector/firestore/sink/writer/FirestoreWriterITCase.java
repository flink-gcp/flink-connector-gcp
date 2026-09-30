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

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.cloud.firestore.DocumentSnapshot;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.firestore.AbstractFirestoreEmulatorITCase;
import io.github.flink.gcp.connector.firestore.FirestoreMetricNames;
import io.github.flink.gcp.connector.firestore.sink.FailedWrite;
import io.github.flink.gcp.connector.firestore.sink.FirestoreBulkWriterSink;
import io.github.flink.gcp.connector.firestore.sink.FirestoreSink;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWrite;
import io.github.flink.gcp.connector.testutils.StubWriterInitContext;
import io.github.flink.gcp.connector.testutils.TestContexts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Drives the production writer — the sink's own {@code createWriter(WriterInitContext)}, the real
 * client and {@code BulkWriter} — against the emulator.
 */
class FirestoreWriterITCase extends AbstractFirestoreEmulatorITCase {

    private final List<FailedWrite> routed = new ArrayList<>();
    private StubWriterInitContext context;
    private String collection;

    @BeforeEach
    void setUp() {
        context = new StubWriterInitContext(0);
        collection = uniqueCollection();
        routed.clear();
    }

    @Test
    void everyOperationHasItsDocumentedEffect() throws Exception {
        client().document(path("set")).set(Map.of("old", 1L)).get();
        client().document(path("merge")).set(Map.of("m", Map.of("kept", 1L), "other", 1L)).get();
        client().document(path("update")).set(Map.of("a", Map.of("b", 1L))).get();
        client().document(path("delete")).set(Map.of("v", 1L)).get();

        try (SinkWriter<FirestoreWrite> writer = writer()) {
            write(writer, FirestoreWrite.set(path("set"), Map.of("new", 1L)));
            write(writer, FirestoreWrite.setMerge(path("merge"), Map.of("m", Map.of("added", 2L))));
            write(writer, FirestoreWrite.create(path("create"), Map.of("v", "created")));
            write(writer, FirestoreWrite.update(path("update"), Map.of("a.b", 2L)));
            write(writer, FirestoreWrite.delete(path("delete")));
            writer.flush(false);
        }

        assertThat(read(path("set")).getData()).isEqualTo(Map.of("new", 1L));
        assertThat(read(path("merge")).getData())
                .isEqualTo(Map.of("m", Map.of("kept", 1L, "added", 2L), "other", 1L));
        assertThat(read(path("create")).getString("v")).isEqualTo("created");
        // A dotted key is one literal top-level field, never a path into map "a".
        DocumentSnapshot updated = read(path("update"));
        assertThat(updated.getData()).containsKeys("a", "a.b");
        assertThat(updated.getData().get("a")).isEqualTo(Map.of("b", 1L));
        assertThat(read(path("delete")).exists()).isFalse();
        assertThat(routed).isEmpty();
    }

    @Test
    void aConditionalUpdateAppliesAgainstTheCurrentUpdateTime() throws Exception {
        client().document(path("doc")).set(Map.of("v", 1L)).get();
        DocumentSnapshot current = read(path("doc"));

        try (SinkWriter<FirestoreWrite> writer = writer()) {
            write(
                    writer,
                    FirestoreWrite.update(path("doc"), Map.of("v", 2L), current.getUpdateTime()));
            writer.flush(false);
        }

        assertThat(read(path("doc")).getLong("v")).isEqualTo(2L);
    }

    @Test
    void anInvalidWriteIsIsolatedFromTheRequestItFailedAndOnlyItIsRouted() throws Exception {
        try (SinkWriter<FirestoreWrite> writer = writer()) {
            for (int i = 0; i < 4; i++) {
                write(writer, FirestoreWrite.set(path("good" + i), Map.of("v", 1L)));
            }
            write(writer, FirestoreWrite.set(path("bad"), Map.of("__reserved__", 1L)));
            writer.flush(false);
        }

        assertThat(routed)
                .extracting(f -> f.getWrite().getDocumentPath())
                .containsExactly(path("bad"));
        for (int i = 0; i < 4; i++) {
            assertThat(read(path("good" + i)).exists()).isTrue();
        }
    }

    @Test
    void theWriterKeepsWritingPastTheLibrarysFailedWriteLeak() throws Exception {
        // 500 refused creates would leave the library's BulkWriter unable to send anything more
        // (BulkWriterDefectsITCase); the writer replaces it before that happens.
        int refusals = FirestoreWriter.PENDING_OPERATION_LIMIT;
        try (SinkWriter<FirestoreWrite> writer = writer()) {
            for (int i = 0; i < refusals; i++) {
                write(writer, FirestoreWrite.set(path("d" + i), Map.of("v", 1L)));
            }
            writer.flush(false);
            assertTimeoutPreemptively(
                    Duration.ofSeconds(120),
                    () -> {
                        for (int i = 0; i < refusals; i++) {
                            write(writer, FirestoreWrite.create(path("d" + i), Map.of("v", 2L)));
                        }
                        writer.flush(false);
                        write(writer, FirestoreWrite.set(path("after"), Map.of("v", 1L)));
                        writer.flush(false);
                    },
                    "The writer stopped sending after refused writes: the library's pending-slot"
                            + " leak (BulkWriterDefectsITCase) was not worked around.");
        }

        assertThat(routed).hasSize(refusals);
        assertThat(read(path("after")).exists()).isTrue();
        assertThat(
                        context.getSinkWriterMetricGroup()
                                .counterValue(FirestoreMetricNames.BULK_WRITERS_REPLACED))
                .isEqualTo(1);
    }

    private SinkWriter<FirestoreWrite> writer() throws Exception {
        FailureHandler<FailedWrite> collecting = routed::add;
        FirestoreBulkWriterSink<FirestoreWrite> sink =
                (FirestoreBulkWriterSink<FirestoreWrite>)
                        FirestoreSink.<FirestoreWrite>builder()
                                .database(database())
                                .serializer((write, c) -> write)
                                .failedWriteHandler(collecting)
                                .emulatorEndpoint(emulatorEndpoint())
                                .build();
        return sink.createWriter(context);
    }

    private static void write(SinkWriter<FirestoreWrite> writer, FirestoreWrite write)
            throws Exception {
        writer.write(write, TestContexts.NO_OP);
    }

    private String path(String id) {
        return collection + "/" + id;
    }
}
