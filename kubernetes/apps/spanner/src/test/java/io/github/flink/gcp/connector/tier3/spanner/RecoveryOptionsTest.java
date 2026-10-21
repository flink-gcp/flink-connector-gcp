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

import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecoveryOptionsTest {
    static final String END = "2026-10-18T02:00:00Z";

    static String[] arguments(String... overrides) {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("--run-id", "spanner-it");
        values.put("--instance", "sp1360-spanner-it");
        values.put("--records", "100");
        values.put("--records-per-second", "100");
        values.put("--change-stream-end", END);
        values.put("--seed-rows-per-second", "50");
        for (int i = 0; i < overrides.length; i += 2) {
            values.put(overrides[i], overrides[i + 1]);
        }
        return values.entrySet().stream()
                .flatMap(e -> Stream.of(e.getKey(), e.getValue()))
                .toArray(String[]::new);
    }

    @Test
    void parsesATrialAndDefaultsToAnInitialFreshStart() {
        var options = RecoveryOptions.parse(arguments());
        assertThat(options.runId).isEqualTo("spanner-it");
        assertThat(options.instance).isEqualTo("sp1360-spanner-it");
        assertThat(options.records).isEqualTo(100);
        assertThat(options.recordsPerSecond).isEqualTo(100);
        assertThat(options.changeStreamEnd).isEqualTo(Instant.parse(END));
        assertThat(options.seedRowsPerSecond).isEqualTo(50);
        assertThat(options.phase).isEqualTo("initial");
        assertThat(options.requireRestored).isFalse();
        assertThat(options.parallelism).isEqualTo(2);
        var database = options.database();
        assertThat(List.of(database.getProject(), database.getInstance(), database.getDatabase()))
                .containsExactly("flink-gcp", "sp1360-spanner-it", "recovery");
        var upgrade =
                RecoveryOptions.parse(
                        arguments("--phase", "upgrade", "--require-restored", "true"));
        assertThat(upgrade.phase).isEqualTo("upgrade");
        assertThat(upgrade.requireRestored).isTrue();
    }

    @ParameterizedTest
    @CsvSource({
        "records,1",
        "records,360001",
        "records,9223372036854775807",
        "records,many",
        "records-per-second,0",
        "records-per-second,101",
        "run-id,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "run-id,invalid_id",
        "run-id,/path",
        "run-id,trailing-",
        "instance,spanner-it",
        "instance,bt1316-spanner-it",
        "instance,sp1360-",
        "instance,sp1360-Upper",
        "instance,sp1360-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "instance,projects/flink-gcp/instances/sp1360-spanner-it",
        "change-stream-end,2026-10-18",
        "change-stream-end,later",
        "change-stream-start,2026-10-18T00:00:00Z",
        "seed-rows-per-second,0",
        "seed-rows-per-second,1001",
        "phase,other",
        "phase,upgrade",
        "require-restored,yes",
        "parallelism,0",
        "parallelism,3",
        "parallelism,two",
        "extra,value"
    })
    void refusesInputsOutsideTheTrial(String name, String value) {
        assertThatThrownBy(() -> RecoveryOptions.parse(arguments("--" + name, value)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void acceptsTheBoundsThemselves() {
        assertThat(
                        RecoveryOptions.parse(
                                        arguments("--records", "2", "--records-per-second", "1"))
                                .records)
                .isEqualTo(2);
        assertThat(
                        RecoveryOptions.parse(
                                        arguments(
                                                "--records",
                                                "360000",
                                                "--records-per-second",
                                                "100"))
                                .recordsPerSecond)
                .isEqualTo(100);
        assertThat(RecoveryOptions.parse(arguments("--run-id", "a".repeat(40))).runId).hasSize(40);
        assertThat(
                        RecoveryOptions.parse(arguments("--instance", "sp1360-" + "a".repeat(40)))
                                .instance)
                .hasSize(47);
        assertThat(
                        RecoveryOptions.parse(arguments("--seed-rows-per-second", "1000"))
                                .seedRowsPerSecond)
                .isEqualTo(1000);
        assertThat(
                        RecoveryOptions.parse(
                                        arguments("--change-stream-end", "2026-10-19T00:00:00Z"))
                                .changeStreamEnd)
                .isEqualTo(Instant.parse("2026-10-19T00:00:00Z"));
        assertThat(RecoveryOptions.parse(arguments("--parallelism", "1")).parallelism).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "run-id",
                "instance",
                "records",
                "records-per-second",
                "change-stream-end",
                "seed-rows-per-second"
            })
    void requiresEveryTrialInput(String name) {
        String[] all = arguments();
        String[] without =
                Stream.iterate(0, i -> i < all.length, i -> i + 2)
                        .filter(i -> !all[i].equals("--" + name))
                        .flatMap(i -> Stream.of(all[i], all[i + 1]))
                        .toArray(String[]::new);
        assertThatThrownBy(() -> RecoveryOptions.parse(without))
                .hasMessageContaining("Missing --" + name);
    }

    @Test
    void refusesDuplicateAndUnpairedArguments() {
        assertThatThrownBy(
                        () ->
                                RecoveryOptions.parse(
                                        Stream.concat(
                                                        Stream.of(arguments()),
                                                        Stream.of("--records", "10"))
                                                .toArray(String[]::new)))
                .hasMessageContaining("Unknown or duplicate");
        assertThatThrownBy(() -> RecoveryOptions.parse("--run-id"))
                .hasMessageContaining("--name value pairs");
    }

    @Test
    void rowKeysMapOneSequenceOfThisRunAndSpreadOverSixteenPrefixes() {
        var options = RecoveryOptions.parse(arguments());
        assertThat(options.rowKey(0)).isEqualTo("0/0000000");
        assertThat(options.rowKey(17)).isEqualTo("1/0000017");
        assertThat(options.rowKey(99)).isEqualTo("3/0000099");
        assertThatThrownBy(() -> options.rowKey(-1)).hasMessageContaining("outside");
        assertThatThrownBy(() -> options.rowKey(100)).hasMessageContaining("outside");
        assertThat(LongStream.range(0, 16).mapToObj(s -> options.rowKey(s).substring(0, 2)))
                .containsExactly(
                        "0/", "1/", "2/", "3/", "4/", "5/", "6/", "7/", "8/", "9/", "a/", "b/",
                        "c/", "d/", "e/", "f/");
        // The widest key still sorts by its prefix, then its sequence.
        var widest = RecoveryOptions.parse(arguments("--records", "360000"));
        assertThat(widest.rowKey(359_999)).isEqualTo("f/0359999");
    }

    @Test
    void anInputRowWritesTheKeyAndSequenceOfItsRecordAndIsReplaySafe() {
        var options = RecoveryOptions.parse(arguments());
        Mutation mutation = options.input(42);
        assertThat(mutation.getOperation()).isEqualTo(Mutation.Op.INSERT_OR_UPDATE);
        assertThat(mutation.getTable()).isEqualTo("input");
        assertThat(mutation.asMap())
                .containsOnly(
                        Map.entry("k", Value.string("a/0000042")), Map.entry("n", Value.int64(42)));
        assertThat(options.input(42)).isEqualTo(mutation);
    }

    @Test
    void identityExcludesThePhaseAndParallelismButCoversTheInputAndInstance() {
        var initial = RecoveryOptions.parse(arguments());
        var upgrade =
                RecoveryOptions.parse(
                        arguments("--phase", "upgrade", "--require-restored", "true"));
        assertThat(upgrade.identity()).isEqualTo(initial.identity());
        // A rescale keeps the trial: its state must restore into the other parallelism.
        assertThat(RecoveryOptions.parse(arguments("--parallelism", "1")).identity())
                .isEqualTo(initial.identity());
        assertThat(RecoveryOptions.parse(arguments("--seed-rows-per-second", "10")).identity())
                .isEqualTo(initial.identity());
        for (String[] change :
                new String[][] {
                    {"--run-id", "other"},
                    {"--instance", "sp1360-other"},
                    {"--records", "101"},
                    {"--records-per-second", "99"},
                    {"--change-stream-end", "2026-10-18T02:00:01Z"}
                }) {
            assertThat(RecoveryOptions.parse(arguments(change)).identity())
                    .isNotEqualTo(initial.identity());
        }
    }

    @Test
    void optionsSurviveJavaSerialization() throws Exception {
        var options = RecoveryOptions.parse(arguments());
        var copy =
                InstantiationUtil.<RecoveryOptions>deserializeObject(
                        InstantiationUtil.serializeObject(options),
                        RecoveryOptionsTest.class.getClassLoader());
        assertThat(copy.identity()).isEqualTo(options.identity());
        assertThat(copy.rowKey(5)).isEqualTo(options.rowKey(5));
    }
}
