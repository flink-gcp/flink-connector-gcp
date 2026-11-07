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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import com.google.cloud.datastore.Entity;
import com.google.cloud.firestore.DocumentSnapshot;
import io.github.flink.gcp.connector.datastore.sink.DatastoreMutation;
import io.github.flink.gcp.connector.datastore.source.serializer.DatastoreEntityDeserializationSchema;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWrite;
import io.github.flink.gcp.connector.firestore.source.serializer.FirestoreDocumentDeserializationSchema;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Copies each database's seed, through the production batch read, into its batch output, which the
 * supervisor reads back.
 *
 * <p>Each copied record is one delivery of one seed record, named {@code <key>@<delivery>}, so a
 * record the source reads again after a restore adds a copy, and a key's copies count its
 * deliveries. Both sources resume a split after the last record they emitted before the restored
 * checkpoint, so a restore adds a copy only for what was emitted after that checkpoint.
 */
@Internal
final class SeedCopy {
    private SeedCopy() {}

    /** The name of one delivery's copy, unique per key and delivery. */
    private static String copyName(Tuple3<String, String, Long> delivery) {
        return delivery.f0 + "@" + delivery.f1;
    }

    /** The Native-mode copy of one delivery; a retried write repeats it. */
    static FirestoreWrite nativeCopy(Tuple3<String, String, Long> delivery) {
        return FirestoreWrite.set(
                RecoveryOptions.BATCH + "/" + copyName(delivery),
                Map.of(
                        RecoveryOptions.KEY_FIELD, delivery.f0,
                        RecoveryOptions.DELIVERY_FIELD, delivery.f1,
                        RecoveryOptions.SEQUENCE_FIELD, delivery.f2));
    }

    /** The Datastore-mode copy of one delivery; a retried write repeats it. */
    static DatastoreMutation datastoreCopy(
            RecoveryOptions options, Tuple3<String, String, Long> delivery) {
        return DatastoreMutation.upsert(
                Entity.newBuilder(options.datastoreKey(RecoveryOptions.BATCH, copyName(delivery)))
                        .set(RecoveryOptions.KEY_FIELD, delivery.f0)
                        .set(RecoveryOptions.DELIVERY_FIELD, delivery.f1)
                        .set(RecoveryOptions.SEQUENCE_FIELD, delivery.f2)
                        .build());
    }

    /** Keeps the document id and sequence of each Native-mode seed document. */
    static final class NativeDeserializer
            implements FirestoreDocumentDeserializationSchema<Tuple2<String, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(DocumentSnapshot document, Collector<Tuple2<String, Long>> out)
                throws IOException {
            Long sequence = document.getLong(RecoveryOptions.SEQUENCE_FIELD);
            if (sequence == null) {
                throw new IOException("Seed document " + document.getId() + " has no sequence");
            }
            out.collect(Tuple2.of(document.getId(), sequence));
        }

        @Override
        public TypeInformation<Tuple2<String, Long>> getProducedType() {
            return Delivery.RECORD;
        }
    }

    /** Keeps the key name and sequence of each Datastore-mode seed entity. */
    static final class DatastoreDeserializer
            implements DatastoreEntityDeserializationSchema<Tuple2<String, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(Entity entity, Collector<Tuple2<String, Long>> out)
                throws IOException {
            String name = entity.getKey().getName();
            if (name == null || !entity.contains(RecoveryOptions.SEQUENCE_FIELD)) {
                throw new IOException("Seed entity " + entity.getKey() + " is not a seed record");
            }
            out.collect(Tuple2.of(name, entity.getLong(RecoveryOptions.SEQUENCE_FIELD)));
        }

        @Override
        public TypeInformation<Tuple2<String, Long>> getProducedType() {
            return Delivery.RECORD;
        }
    }

    /**
     * Holds each reading subtask's copy to a fixed pace, so the batch reads' splits stay unfinished
     * through both recoveries of a trial. It is chained to the source in one task, so a checkpoint
     * barrier waits at most one pause, and a record late after a stall is not followed by a burst
     * that would use up the seed sooner.
     */
    static final class Throttle
            extends RichMapFunction<Tuple2<String, Long>, Tuple2<String, Long>> {
        private static final long serialVersionUID = 1L;
        private final long intervalNanos;
        private transient long next;

        Throttle(long recordsPerSecond) {
            this.intervalNanos = 1_000_000_000L / recordsPerSecond;
        }

        @Override
        public void open(OpenContext context) {
            next = System.nanoTime();
        }

        @Override
        public Tuple2<String, Long> map(Tuple2<String, Long> record) throws InterruptedException {
            long now = System.nanoTime();
            if (next > now) {
                TimeUnit.NANOSECONDS.sleep(next - now);
            } else {
                next = now;
            }
            next += intervalNanos;
            return record;
        }
    }
}
