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

package io.github.flink.gcp.connector.tier3.bigtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutation;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableChangeStreamDeserializationSchema;

import java.time.Instant;
import java.util.UUID;

/**
 * Relays the eo table's change stream into the relay table, which the supervisor reads back.
 *
 * <p>Each relayed row is an eo row key with one cell per delivery of its change, holding the
 * change's commit time. The qualifier names the phase, the task instance and the delivery's
 * position in that instance, so a change the source delivers again, after a restore or within one
 * instance, adds a qualifier, and the qualifiers of a row count its deliveries.
 */
@Internal
final class ChangeRelay {
    static final TypeInformation<Tuple2<String, Long>> CHANGE =
            Types.TUPLE(Types.STRING, Types.LONG);

    private ChangeRelay() {}

    /** The relay row of one delivery; the cell timestamp is fixed so a retried write repeats it. */
    static RowMutationEntry mutation(Tuple3<String, String, Long> delivery) {
        return RowMutationEntry.create(delivery.f0)
                .setCell(RecoveryOptions.RELAY_FAMILY, delivery.f1, 0L, Long.toString(delivery.f2));
    }

    /**
     * Keeps the row key and commit time, in microseconds, of each change that adds to the SUM
     * family. The staged sink's commit applies a record's contribution and its marker in one
     * conditional write, so this skips nothing a contribution carried; a change holding only other
     * families is skipped. A row outside the input is relayed, for the oracle to report.
     */
    static final class Deserializer
            implements BigtableChangeStreamDeserializationSchema<Tuple2<String, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(
                BigtableChangeStreamMutation mutation, Collector<Tuple2<String, Long>> out) {
            boolean contribution =
                    mutation.getEntries().stream()
                            .anyMatch(
                                    entry ->
                                            entry
                                                            instanceof
                                                            BigtableChangeStreamMutation
                                                                    .AddToCellEntry
                                                    && entry.getFamilyName()
                                                            .equals(RecoveryOptions.SUM_FAMILY));
            if (contribution) {
                out.collect(
                        Tuple2.of(
                                mutation.getRowKey().toStringUtf8(),
                                micros(mutation.getCommitTime())));
            }
        }

        @Override
        public TypeInformation<Tuple2<String, Long>> getProducedType() {
            return CHANGE;
        }

        private static long micros(Instant instant) {
            return Math.addExact(
                    Math.multiplyExact(instant.getEpochSecond(), 1_000_000L),
                    instant.getNano() / 1_000L);
        }
    }

    /** Names each delivery by its phase, its task instance and its position in that instance. */
    static final class Delivery
            extends RichMapFunction<Tuple2<String, Long>, Tuple3<String, String, Long>> {
        private static final long serialVersionUID = 1L;
        private final String phase;
        private transient String instance;
        private transient long deliveries;

        Delivery(String phase) {
            this.phase = phase;
        }

        @Override
        public void open(OpenContext context) {
            instance = phase + "/" + UUID.randomUUID() + "/";
            deliveries = 0;
        }

        @Override
        public Tuple3<String, String, Long> map(Tuple2<String, Long> change) {
            return Tuple3.of(change.f0, instance + deliveries++, change.f1);
        }
    }
}
