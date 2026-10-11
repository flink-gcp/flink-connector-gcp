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
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Key;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.datastore.AbstractDatastoreRealGcpITCase;
import io.github.flink.gcp.connector.datastore.sink.DatastoreCommitSink;
import io.github.flink.gcp.connector.datastore.sink.DatastoreMutation;
import io.github.flink.gcp.connector.datastore.sink.DatastoreSink;
import io.github.flink.gcp.connector.datastore.sink.DatastoreWriterOptions;
import io.github.flink.gcp.connector.datastore.sink.FailedMutation;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives the writer against the real service through the production access, where a commit refused
 * with {@code ALREADY_EXISTS} or {@code NOT_FOUND} applies every other write it carries (unlike the
 * emulator, which {@code DatastoreWriterITCase} drives), so the confirmation pass meets inserts the
 * refused commit already wrote.
 */
@EnabledIfEnvironmentVariable(named = "FIRESTORE_IT_PROJECT", matches = ".+")
@Tag("gated")
class DatastoreWriterRealGcpITCase extends AbstractDatastoreRealGcpITCase {

    private static final List<FailedMutation> ROUTED = new CopyOnWriteArrayList<>();

    private static final List<Key> LOOKUPS = new CopyOnWriteArrayList<>();

    private String kind;

    @BeforeEach
    void freshKind() {
        kind = uniqueKind();
        ROUTED.clear();
        LOOKUPS.clear();
    }

    @Test
    void onlyTheDuplicateInsertIsRoutedAlthoughTheRefusedCommitWroteTheOthers() throws Exception {
        client().put(Entity.newBuilder(key(kind, "existing")).set("v", 0L).build());

        try (DatastoreWriter<DatastoreMutation> writer = writer()) {
            writer.write(DatastoreMutation.insert(entity("new1", 1)), TestContexts.NO_OP);
            writer.write(DatastoreMutation.insert(entity("existing", 2)), TestContexts.NO_OP);
            writer.write(DatastoreMutation.insert(entity("new2", 3)), TestContexts.NO_OP);
            writer.write(DatastoreMutation.upsert(entity("b", 4)), TestContexts.NO_OP);
            writer.flush(false);
        }

        assertThat(ROUTED)
                .extracting(failed -> failed.getMutation().getKey().getName())
                .containsExactly("existing");
        assertThat(ROUTED.get(0).getErrorMessage()).contains("ALREADY_EXISTS");
        assertThat(read(key(kind, "new1")).getLong("v")).isEqualTo(1);
        assertThat(read(key(kind, "new2")).getLong("v")).isEqualTo(3);
        assertThat(read(key(kind, "b")).getLong("v")).isEqualTo(4);
        assertThat(read(key(kind, "existing")).getLong("v")).isZero();
        // The new inserts reached the lookup, so the refused commit had written them and their
        // re-sends answered ALREADY_EXISTS.
        assertThat(LOOKUPS)
                .extracting(Key::getName)
                .containsExactlyInAnyOrder("new1", "existing", "new2");
    }

    @Test
    void anInsertBesideARefusedUpdateIsNotRouted() throws Exception {
        try (DatastoreWriter<DatastoreMutation> writer = writer()) {
            writer.write(DatastoreMutation.insert(entity("new", 1)), TestContexts.NO_OP);
            writer.write(DatastoreMutation.update(entity("missing", 2)), TestContexts.NO_OP);
            writer.flush(false);
        }

        assertThat(ROUTED)
                .extracting(failed -> failed.getMutation().getKey().getName())
                .containsExactly("missing");
        assertThat(ROUTED.get(0).getErrorMessage()).contains("NOT_FOUND");
        assertThat(read(key(kind, "new")).getLong("v")).isEqualTo(1);
        assertThat(LOOKUPS).extracting(Key::getName).contains("new");
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

    private DatastoreWriter<DatastoreMutation> writer() throws Exception {
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
                                .build();
        DefaultDatastoreDatabaseAccessFactory production = RejectionProbes.service(database());
        return new DatastoreWriter<>(
                sink.getConfig(),
                () -> new RecordingLookups(production.create()),
                1,
                TestSinkWriterMetricGroup.create());
    }

    private Entity entity(String name, long value) {
        return Entity.newBuilder(key(kind, name)).set("v", value).build();
    }

    /** Records every key the writer looks up, and otherwise defers to the production access. */
    private static final class RecordingLookups implements DatastoreDatabaseAccess {

        private final DatastoreDatabaseAccess delegate;

        private RecordingLookups(DatastoreDatabaseAccess delegate) {
            this.delegate = delegate;
        }

        @Override
        public void commit(List<DatastoreMutation> writes) {
            delegate.commit(writes);
        }

        @Override
        @Nullable
        public Entity lookup(Key key) {
            LOOKUPS.add(key);
            return delegate.lookup(key);
        }

        @Override
        public List<Key> allocateIds(List<IncompleteKey> keys) {
            return delegate.allocateIds(keys);
        }

        @Override
        public void close() throws Exception {
            delegate.close();
        }
    }
}
