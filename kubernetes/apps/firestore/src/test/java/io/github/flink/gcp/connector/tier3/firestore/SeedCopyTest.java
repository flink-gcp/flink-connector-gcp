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

package io.github.flink.gcp.connector.tier3.firestore;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.Key;
import io.github.flink.gcp.connector.datastore.sink.DatastoreMutation;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWrite;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SeedCopyTest {
    private static final Tuple3<String, String, Long> DELIVERY =
            Tuple3.of("a-0000042", "upgrade:u:3", 42L);

    @Test
    void aNativeCopyIsNamedByKeyAndDeliveryAndHoldsAllThree() {
        FirestoreWrite write = SeedCopy.nativeCopy(DELIVERY);
        assertThat(write.getOperation()).isEqualTo(FirestoreWrite.Operation.SET);
        assertThat(write.getDocumentPath()).isEqualTo("batch_out/a-0000042@upgrade:u:3");
        assertThat(write.getFields())
                .containsOnly(
                        Map.entry("k", "a-0000042"),
                        Map.entry("delivery", "upgrade:u:3"),
                        Map.entry("n", 42L));
    }

    @Test
    void aDatastoreCopyIsAnUpsertInTheRunsDatabase() {
        var options = RecoveryOptions.parse(RecoveryOptionsTest.arguments());
        DatastoreMutation mutation = SeedCopy.datastoreCopy(options, DELIVERY);
        assertThat(mutation.getOperation()).isEqualTo(DatastoreMutation.Operation.UPSERT);
        Key key = mutation.getKey();
        assertThat(List.of(key.getProjectId(), key.getDatabaseId(), key.getKind(), key.getName()))
                .containsExactly(
                        "flink-gcp", "fs1879-firestore-it-d", "batch_out", "a-0000042@upgrade:u:3");
        FullEntity<Key> entity = mutation.getEntity();
        assertThat(entity.getString("k")).isEqualTo("a-0000042");
        assertThat(entity.getString("delivery")).isEqualTo("upgrade:u:3");
        assertThat(entity.getLong("n")).isEqualTo(42L);
        assertThat(entity.getNames()).containsExactlyInAnyOrder("k", "delivery", "n");
    }

    @Test
    void theDatastoreDeserializerKeepsTheKeyNameAndSequence() throws IOException {
        var options = RecoveryOptions.parse(RecoveryOptionsTest.arguments());
        var out = new ListCollector();
        new SeedCopy.DatastoreDeserializer()
                .deserialize(
                        Entity.newBuilder(options.datastoreKey("seed", "s3-0000003"))
                                .set("n", 3L)
                                .build(),
                        out);
        assertThat(out.records).containsExactly(Tuple2.of("s3-0000003", 3L));
        // An entity that is not a seed record fails the read rather than vanishing.
        assertThatThrownBy(
                        () ->
                                new SeedCopy.DatastoreDeserializer()
                                        .deserialize(
                                                Entity.newBuilder(
                                                                options.datastoreKey(
                                                                        "seed", "s3-0000003"))
                                                        .build(),
                                                out))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(
                        () ->
                                new SeedCopy.DatastoreDeserializer()
                                        .deserialize(
                                                Entity.newBuilder(
                                                                Key.newBuilder(
                                                                                "flink-gcp",
                                                                                "seed",
                                                                                7L,
                                                                                "fs1879-firestore-it-d")
                                                                        .build())
                                                        .set("n", 3L)
                                                        .build(),
                                                out))
                .isInstanceOf(IOException.class);
    }

    private static final class ListCollector implements Collector<Tuple2<String, Long>> {
        final List<Tuple2<String, Long>> records = new ArrayList<>();

        @Override
        public void collect(Tuple2<String, Long> record) {
            records.add(record);
        }

        @Override
        public void close() {}
    }
}
