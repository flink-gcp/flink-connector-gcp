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

package io.github.flink.gcp.connector.datastore.sink.writer;

import com.google.cloud.datastore.Entity;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.datastore.AbstractDatastoreEmulatorITCase;
import io.github.flink.gcp.connector.datastore.DatastoreMetricNames;
import io.github.flink.gcp.connector.datastore.sink.DatastoreCommitSink;
import io.github.flink.gcp.connector.datastore.sink.DatastoreMutation;
import io.github.flink.gcp.connector.datastore.sink.DatastoreSink;
import io.github.flink.gcp.connector.datastore.sink.DatastoreWriterOptions;
import io.github.flink.gcp.connector.datastore.sink.FailedMutation;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives the writer against the emulator through the production access, so that solo confirmation
 * meets the service's own all-or-nothing refusal rather than the unit tests' fake.
 */
class DatastoreWriterITCase extends AbstractDatastoreEmulatorITCase {

    private static final List<FailedMutation> ROUTED = new ArrayList<>();

    private String kind;

    @BeforeEach
    void freshKind() {
        kind = uniqueKind();
        ROUTED.clear();
    }

    @Test
    void appliesEveryOperation() throws Exception {
        client().put(Entity.newBuilder(key(kind, "to-update")).set("v", 0L).build());
        client().put(Entity.newBuilder(key(kind, "to-delete")).set("v", 0L).build());

        try (DatastoreWriter<DatastoreMutation> writer = writer()) {
            writer.write(DatastoreMutation.upsert(entity("upserted", 1)), TestContexts.NO_OP);
            writer.write(DatastoreMutation.insert(entity("inserted", 2)), TestContexts.NO_OP);
            writer.write(DatastoreMutation.update(entity("to-update", 3)), TestContexts.NO_OP);
            writer.write(DatastoreMutation.delete(key(kind, "to-delete")), TestContexts.NO_OP);
            writer.flush(false);
        }

        assertThat(read(key(kind, "upserted")).getLong("v")).isEqualTo(1);
        assertThat(read(key(kind, "inserted")).getLong("v")).isEqualTo(2);
        assertThat(read(key(kind, "to-update")).getLong("v")).isEqualTo(3);
        assertThat(read(key(kind, "to-delete"))).isNull();
        assertThat(ROUTED).isEmpty();
    }

    @Test
    void routesOnlyTheRefusedWritesAndAppliesTheRestOfTheirCommit() throws Exception {
        client().put(Entity.newBuilder(key(kind, "existing")).set("v", 0L).build());

        try (DatastoreWriter<DatastoreMutation> writer = writer()) {
            writer.write(DatastoreMutation.upsert(entity("a", 1)), TestContexts.NO_OP);
            writer.write(
                    DatastoreMutation.upsert(
                            Entity.newBuilder(key(kind, "long"))
                                    .set("s", "x".repeat(1_600))
                                    .build()),
                    TestContexts.NO_OP);
            writer.write(DatastoreMutation.insert(entity("existing", 2)), TestContexts.NO_OP);
            writer.write(DatastoreMutation.update(entity("missing", 3)), TestContexts.NO_OP);
            writer.write(DatastoreMutation.upsert(entity("b", 4)), TestContexts.NO_OP);
            writer.flush(false);
        }

        assertThat(ROUTED)
                .extracting(failed -> failed.getMutation().getKey().getName())
                .containsExactly("long", "existing", "missing");
        assertThat(ROUTED.get(0).getErrorMessage()).contains("INVALID_ARGUMENT");
        assertThat(ROUTED.get(1).getErrorMessage()).contains("ALREADY_EXISTS");
        assertThat(ROUTED.get(2).getErrorMessage()).contains("NOT_FOUND");
        assertThat(read(key(kind, "a"))).isNotNull();
        assertThat(read(key(kind, "b"))).isNotNull();
        assertThat(read(key(kind, "existing")).getLong("v")).isZero();
        assertThat(read(key(kind, "missing"))).isNull();
    }

    @Test
    void writesToOneKeyLandInTheOrderTheyWereGiven() throws Exception {
        // The client's Batch would merge a repeated key on its own and land on the same final
        // state, so the commit count is what shows the writer committed before each repeat.
        TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();
        try (DatastoreWriter<DatastoreMutation> writer = writer(metrics)) {
            for (long v = 1; v <= 5; v++) {
                writer.write(DatastoreMutation.upsert(entity("k", v)), TestContexts.NO_OP);
                writer.write(DatastoreMutation.upsert(entity("other" + v, v)), TestContexts.NO_OP);
            }
            writer.write(DatastoreMutation.delete(key(kind, "other5")), TestContexts.NO_OP);
            writer.flush(false);
        }

        assertThat(read(key(kind, "k")).getLong("v")).isEqualTo(5);
        assertThat(read(key(kind, "other5")))
                .isNull(); // Five commits of {k, other} and one for the delete of other5.
        assertThat(metrics.counterValue(DatastoreMetricNames.BATCHES_SENT)).isEqualTo(6);
    }

    private DatastoreWriter<DatastoreMutation> writer() throws Exception {
        return writer(TestSinkWriterMetricGroup.create());
    }

    private DatastoreWriter<DatastoreMutation> writer(TestSinkWriterMetricGroup metrics)
            throws Exception {
        DatastoreCommitSink<DatastoreMutation> sink =
                (DatastoreCommitSink<DatastoreMutation>)
                        DatastoreSink.<DatastoreMutation>builder()
                                .database(database())
                                .serializer((element, context) -> element)
                                .writerOptions(
                                        DatastoreWriterOptions.builder()
                                                .throttlingEnabled(false)
                                                .build())
                                .failedMutationHandler((FailureHandler<FailedMutation>) ROUTED::add)
                                .emulatorEndpoint(emulatorEndpoint())
                                .build();
        return new DatastoreWriter<>(
                sink.getConfig(),
                new DefaultDatastoreDatabaseAccessFactory(
                        database(),
                        Duration.ofSeconds(30),
                        EmulatorEndpoint.parse(emulatorEndpoint(), "emulatorEndpoint"),
                        null),
                1,
                metrics);
    }

    private Entity entity(String name, long value) {
        return Entity.newBuilder(key(kind, name)).set("v", value).build();
    }
}
