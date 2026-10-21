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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;
import io.github.flink.gcp.connector.spanner.source.changestream.Mod;
import io.github.flink.gcp.connector.spanner.source.changestream.ModType;
import io.github.flink.gcp.connector.spanner.source.changestream.ValueCaptureType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ChangeRelayTest {
    private static final Instant COMMIT = Instant.parse("2026-10-21T00:00:00.123456789Z");

    @Test
    void relaysTheKeyAndCommitMicrosOfEveryModOfAChange() {
        var relayed =
                deserialize(
                        record(
                                new Mod("{\"k\":\"a/0000042\"}", "{\"n\":\"42\"}", null),
                                new Mod("{\"k\":\"b/0000043\"}", "{\"n\":\"43\"}", null)));

        long micros = COMMIT.getEpochSecond() * 1_000_000L + 123_456L;
        assertThat(relayed)
                .containsExactly(Tuple2.of("a/0000042", micros), Tuple2.of("b/0000043", micros));
    }

    @Test
    void relaysAKeyOutsideTheInputUnderItsRawKeysForTheOracleToReport() {
        assertThat(deserialize(record(new Mod("{\"k\":\"other\"}", "{}", null))))
                .extracting(change -> change.f0)
                .containsExactly("{\"k\":\"other\"}");
        assertThat(ChangeRelay.Deserializer.key("{\"k\":\"a/0000042\",\"x\":\"1\"}"))
                .isEqualTo("{\"k\":\"a/0000042\",\"x\":\"1\"}");
        assertThat(ChangeRelay.Deserializer.key("{\"k\":\"a/0000042\"}")).isEqualTo("a/0000042");
    }

    @Test
    void aRelayRowIsKeyedByKeyAndDeliveryAndHoldsTheCommitTime() {
        Mutation mutation = ChangeRelay.mutation(Tuple3.of("a/0000042", "initial/u/0", 7L));
        assertThat(mutation.getOperation()).isEqualTo(Mutation.Op.INSERT_OR_UPDATE);
        assertThat(mutation.getTable()).isEqualTo("relay");
        assertThat(mutation.asMap())
                .containsOnly(
                        Map.entry("k", Value.string("a/0000042")),
                        Map.entry("delivery", Value.string("initial/u/0")),
                        Map.entry("commit_micros", Value.int64(7)));
    }

    private static List<Tuple2<String, Long>> deserialize(DataChangeRecord record) {
        List<Tuple2<String, Long>> out = new ArrayList<>();
        new ChangeRelay.Deserializer()
                .deserialize(
                        record,
                        new Collector<>() {
                            @Override
                            public void collect(Tuple2<String, Long> change) {
                                out.add(change);
                            }

                            @Override
                            public void close() {}
                        });
        return out;
    }

    private static DataChangeRecord record(Mod... mods) {
        return DataChangeRecord.builder()
                .commitTimestamp(COMMIT)
                .recordSequence("00000000")
                .serverTransactionId("tx")
                .lastRecordInTransactionInPartition(true)
                .tableName("input")
                .columnTypes(
                        List.of(
                                new DataChangeRecord.ColumnType(
                                        "k", "{\"code\":\"STRING\"}", true, 1)))
                .mods(List.of(mods))
                .modType(ModType.INSERT)
                .valueCaptureType(ValueCaptureType.OLD_AND_NEW_VALUES)
                .numberOfRecordsInTransaction(1)
                .numberOfPartitionsInTransaction(1)
                .transactionTag("")
                .systemTransaction(false)
                .build();
    }
}
