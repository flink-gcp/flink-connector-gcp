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

package io.github.flink.gcp.connector.tier3.cloudtasks;

import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecoveryOptionsTest {
    static String[] arguments(String... overrides) {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("--run-id", "cloudtasks-it");
        values.put("--queue", "ct1362-cloudtasks-it");
        values.put("--records", "100");
        values.put("--records-per-second", "100");
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
        assertThat(options.runId).isEqualTo("cloudtasks-it");
        assertThat(options.queue).isEqualTo("ct1362-cloudtasks-it");
        assertThat(options.records).isEqualTo(100);
        assertThat(options.recordsPerSecond).isEqualTo(100);
        assertThat(options.phase).isEqualTo("initial");
        assertThat(options.requireRestored).isFalse();
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
        "run-id,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "queue,ct1362-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "records,9223372036854775807",
        "records,many",
        "records-per-second,0",
        "records-per-second,101",
        "phase,other",
        "phase,upgrade",
        "require-restored,yes",
        "run-id,invalid_id",
        "run-id,/path",
        "run-id,trailing-",
        "queue,cloudtasks-it",
        "queue,ct1362-",
        "queue,ct1362-Upper",
        "queue,projects/flink-gcp/locations/us-central1/queues/ct1362-cloudtasks-it",
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
        assertThat(
                        RecoveryOptions.parse(
                                        arguments(
                                                "--run-id",
                                                "a".repeat(40),
                                                "--queue",
                                                "ct1362-" + "a".repeat(40)))
                                .runId)
                .hasSize(40);
    }

    @ParameterizedTest
    @ValueSource(strings = {"run-id", "queue", "records", "records-per-second"})
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
    void taskKeyMapsOneSequenceOfThisRunAndTheBodyRepeatsIt() {
        var options = RecoveryOptions.parse(arguments());
        assertThat(options.taskKey(0)).isEqualTo("cloudtasks-it/0");
        assertThat(options.taskKey(99)).isEqualTo("cloudtasks-it/99");
        assertThatThrownBy(() -> options.taskKey(-1)).hasMessageContaining("outside");
        assertThatThrownBy(() -> options.taskKey(100)).hasMessageContaining("outside");
        var task = options.task(7);
        assertThat(task.getName()).isEmpty();
        assertThat(task.getHttpRequest().getUrl()).isEqualTo(RecoveryOptions.TARGET);
        assertThat(task.getHttpRequest().getBody().toString(StandardCharsets.UTF_8))
                .isEqualTo("cloudtasks-it/7");
    }

    @Test
    void identityExcludesThePhaseButCoversTheInputAndQueue() {
        var initial = RecoveryOptions.parse(arguments());
        var upgrade =
                RecoveryOptions.parse(
                        arguments("--phase", "upgrade", "--require-restored", "true"));
        assertThat(upgrade.identity()).isEqualTo(initial.identity());
        for (String[] change :
                new String[][] {
                    {"--run-id", "other"},
                    {"--queue", "ct1362-other"},
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
        assertThat(copy.taskKey(5)).isEqualTo(options.taskKey(5));
    }
}
