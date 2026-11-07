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

import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.Key;
import io.github.flink.gcp.connector.datastore.sink.DatastoreMutation;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWrite;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecoveryOptionsTest {

    static String[] arguments(String... overrides) {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("--run-id", "firestore-it");
        values.put("--native-database", "fs1879-firestore-it-n");
        values.put("--datastore-database", "fs1879-firestore-it-d");
        values.put("--records", "100");
        values.put("--records-per-second", "100");
        values.put("--seed-records-per-second", "50");
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
        assertThat(options.runId).isEqualTo("firestore-it");
        assertThat(options.nativeDatabase).isEqualTo("fs1879-firestore-it-n");
        assertThat(options.datastoreDatabase).isEqualTo("fs1879-firestore-it-d");
        assertThat(options.records).isEqualTo(100);
        assertThat(options.recordsPerSecond).isEqualTo(100);
        assertThat(options.seedRecordsPerSecond).isEqualTo(50);
        assertThat(options.phase).isEqualTo("initial");
        assertThat(options.requireRestored).isFalse();
        assertThat(options.parallelism).isEqualTo(2);
        var nativeDestination = options.nativeDestination();
        assertThat(List.of(nativeDestination.getProject(), nativeDestination.getDatabaseId()))
                .containsExactly("flink-gcp", "fs1879-firestore-it-n");
        var datastoreDestination = options.datastoreDestination();
        assertThat(List.of(datastoreDestination.getProject(), datastoreDestination.getDatabaseId()))
                .containsExactly("flink-gcp", "fs1879-firestore-it-d");
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
        "native-database,firestore-it",
        "native-database,sp1360-firestore-it",
        "native-database,fs1879-",
        "native-database,fs1879-Upper",
        "native-database,fs1879-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "native-database,(default)",
        "native-database,projects/flink-gcp/databases/fs1879-firestore-it-n",
        "native-database,fs1879-firestore-it-d",
        "datastore-database,fs1879-",
        "datastore-database,(default)",
        "datastore-database,fs1879-firestore-it-n",
        "seed-records-per-second,0",
        "seed-records-per-second,201",
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
        // 47 characters, within Firestore's 63.
        assertThat(
                        RecoveryOptions.parse(
                                        arguments("--native-database", "fs1879-" + "a".repeat(40)))
                                .nativeDatabase)
                .hasSize(47);
        assertThat(
                        RecoveryOptions.parse(arguments("--seed-records-per-second", "200"))
                                .seedRecordsPerSecond)
                .isEqualTo(200);
        assertThat(RecoveryOptions.parse(arguments("--parallelism", "1")).parallelism).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "run-id",
                "native-database",
                "datastore-database",
                "records",
                "records-per-second",
                "seed-records-per-second"
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
    void keysMapOneSequenceOfThisRunAndSpreadOverSixteenPrefixes() {
        var options = RecoveryOptions.parse(arguments());
        assertThat(options.key(0)).isEqualTo("0-0000000");
        assertThat(options.key(17)).isEqualTo("1-0000017");
        assertThat(options.key(99)).isEqualTo("3-0000099");
        assertThatThrownBy(() -> options.key(-1)).hasMessageContaining("outside");
        assertThatThrownBy(() -> options.key(100)).hasMessageContaining("outside");
        assertThat(LongStream.range(0, 16).mapToObj(s -> options.key(s).substring(0, 2)))
                .containsExactly(
                        "0-", "1-", "2-", "3-", "4-", "5-", "6-", "7-", "8-", "9-", "a-", "b-",
                        "c-", "d-", "e-", "f-");
        var widest = RecoveryOptions.parse(arguments("--records", "360000"));
        assertThat(widest.key(359_999)).isEqualTo("f-0359999");
    }

    @Test
    void inputWritesNameTheirRecordAndAreReplaySafe() {
        var options = RecoveryOptions.parse(arguments());
        FirestoreWrite write = options.nativeInput(42);
        assertThat(write.getOperation()).isEqualTo(FirestoreWrite.Operation.SET);
        assertThat(write.getDocumentPath()).isEqualTo("input/a-0000042");
        assertThat(write.getFields()).containsOnly(Map.entry("n", 42L));

        DatastoreMutation mutation = options.datastoreInput(42);
        assertThat(mutation.getOperation()).isEqualTo(DatastoreMutation.Operation.UPSERT);
        Key key = mutation.getKey();
        assertThat(List.of(key.getProjectId(), key.getDatabaseId(), key.getKind(), key.getName()))
                .containsExactly("flink-gcp", "fs1879-firestore-it-d", "input", "a-0000042");
        FullEntity<Key> entity = mutation.getEntity();
        assertThat(entity.getNames()).containsExactly("n");
        assertThat(entity.getLong("n")).isEqualTo(42L);
    }

    @Test
    void identityExcludesThePhaseAndParallelismButCoversTheInputAndDatabases() {
        var initial = RecoveryOptions.parse(arguments());
        var upgrade =
                RecoveryOptions.parse(
                        arguments("--phase", "upgrade", "--require-restored", "true"));
        assertThat(upgrade.identity()).isEqualTo(initial.identity());
        // A rescale keeps the trial: its state must restore into the other parallelism.
        assertThat(RecoveryOptions.parse(arguments("--parallelism", "1")).identity())
                .isEqualTo(initial.identity());
        assertThat(RecoveryOptions.parse(arguments("--seed-records-per-second", "10")).identity())
                .isEqualTo(initial.identity());
        for (String[] change :
                new String[][] {
                    {"--run-id", "other"},
                    {"--native-database", "fs1879-other-n"},
                    {"--datastore-database", "fs1879-other-d"},
                    {"--records", "101"},
                    {"--records-per-second", "99"}
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
        assertThat(copy.key(5)).isEqualTo(options.key(5));
    }
}
