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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import com.google.cloud.spanner.Mutation;
import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;
import io.github.flink.gcp.connector.spanner.source.changestream.Mod;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerChangeStreamDeserializationSchema;

import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Relays the input table's change stream into the relay table, which the supervisor reads back.
 *
 * <p>Each relay row is one delivery of one input key's change, holding the change's commit time.
 * Every write the sink executes commits under a timestamp of its own, so the distinct commit times
 * of a key count the sink's executions of its write; a change the source delivers again carries a
 * commit time already seen for that key, and counts as the source's replay.
 */
@Internal
final class ChangeRelay {
    // The keys of an input row, as the source normalizes them: one string member, k. Anything
    // else is relayed under its raw keys, which match no input key, for the oracle to report.
    private static final Pattern INPUT_KEYS = Pattern.compile("\\{\"k\":\"([0-9a-f]/[0-9]{7})\"}");

    private ChangeRelay() {}

    /** The relay row of one delivery; a retried write repeats it. */
    static Mutation mutation(Tuple3<String, String, Long> delivery) {
        return Mutation.newInsertOrUpdateBuilder(RecoveryOptions.RELAY_TABLE)
                .set(RecoveryOptions.KEY_COLUMN)
                .to(delivery.f0)
                .set(RecoveryOptions.DELIVERY_COLUMN)
                .to(delivery.f1)
                .set(RecoveryOptions.COMMIT_COLUMN)
                .to(delivery.f2)
                .build();
    }

    /**
     * Keeps the key and commit time, in microseconds, of every row each change modifies. The input
     * sink writes one row per mutation group, so a change usually modifies one row, but a record
     * carrying several mods is relayed once per mod.
     */
    static final class Deserializer
            implements SpannerChangeStreamDeserializationSchema<Tuple2<String, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(DataChangeRecord record, Collector<Tuple2<String, Long>> out) {
            long committed = micros(record.getCommitTimestamp());
            for (Mod mod : record.getMods()) {
                out.collect(Tuple2.of(key(mod.getKeysJson()), committed));
            }
        }

        @Override
        public TypeInformation<Tuple2<String, Long>> getProducedType() {
            return Delivery.RECORD;
        }

        static String key(String keysJson) {
            Matcher matcher = INPUT_KEYS.matcher(keysJson);
            return matcher.matches() ? matcher.group(1) : keysJson;
        }

        private static long micros(Instant instant) {
            return Math.addExact(
                    Math.multiplyExact(instant.getEpochSecond(), 1_000_000L),
                    instant.getNano() / 1_000L);
        }
    }
}
