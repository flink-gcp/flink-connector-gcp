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

package io.github.flink.gcp.connector.tier3.spanner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerStructDeserializationSchema;

import java.util.concurrent.TimeUnit;

/**
 * Copies the seed table, through the production batch read, into the batch output table, which the
 * supervisor reads back.
 *
 * <p>Each copied row is one delivery of one seed row, so a partition the source reads again after a
 * restore adds a row per seed row it holds, and a key's rows count its deliveries.
 */
@Internal
final class SeedCopy {
    private SeedCopy() {}

    /** The batch output row of one delivery; a retried write repeats it. */
    static Mutation mutation(Tuple3<String, String, Long> delivery) {
        return Mutation.newInsertOrUpdateBuilder(RecoveryOptions.BATCH_TABLE)
                .set(RecoveryOptions.KEY_COLUMN)
                .to(delivery.f0)
                .set(RecoveryOptions.DELIVERY_COLUMN)
                .to(delivery.f1)
                .set(RecoveryOptions.SEQUENCE_COLUMN)
                .to(delivery.f2)
                .build();
    }

    /** Keeps the key and sequence of each seed row. */
    static final class Deserializer
            implements SpannerStructDeserializationSchema<Tuple2<String, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(Struct row, Collector<Tuple2<String, Long>> out) {
            out.collect(
                    Tuple2.of(
                            row.getString(RecoveryOptions.KEY_COLUMN),
                            row.getLong(RecoveryOptions.SEQUENCE_COLUMN)));
        }

        @Override
        public TypeInformation<Tuple2<String, Long>> getProducedType() {
            return Delivery.RECORD;
        }
    }

    /**
     * Holds each reading subtask's copy to a fixed pace, so the batch read's partitions stay
     * unfinished through both recoveries of a trial. It is chained to the source in one task, so a
     * checkpoint barrier waits at most one pause, and a row late after a stall is not followed by a
     * burst that would use up the seed sooner.
     */
    static final class Throttle
            extends RichMapFunction<Tuple2<String, Long>, Tuple2<String, Long>> {
        private static final long serialVersionUID = 1L;
        private final long intervalNanos;
        private transient long next;

        Throttle(long rowsPerSecond) {
            this.intervalNanos = 1_000_000_000L / rowsPerSecond;
        }

        @Override
        public void open(OpenContext context) {
            next = System.nanoTime();
        }

        @Override
        public Tuple2<String, Long> map(Tuple2<String, Long> row) throws InterruptedException {
            long now = System.nanoTime();
            if (next > now) {
                TimeUnit.NANOSECONDS.sleep(next - now);
            } else {
                next = now;
            }
            next += intervalNanos;
            return row;
        }
    }
}
