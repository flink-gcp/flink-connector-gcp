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

package io.github.flink.gcp.connector.tier3.bigquery;

import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecoveryOptionsTest {
    static String[] arguments(String... overrides) {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("--run-id", "bigquery-it");
        values.put("--mode", "EO");
        values.put("--destinations", "10");
        for (int i = 0; i < overrides.length; i += 2) {
            values.put(overrides[i], overrides[i + 1]);
        }
        return values.entrySet().stream()
                .flatMap(e -> java.util.stream.Stream.of(e.getKey(), e.getValue()))
                .toArray(String[]::new);
    }

    @ParameterizedTest
    @EnumSource(RecoveryOptions.Mode.class)
    void defaultInputIsThirtyMinutesAtOneMiBPerSecond(RecoveryOptions.Mode mode) {
        var options = RecoveryOptions.parse(arguments("--mode", mode.name()));
        assertThat(options.records * mode.rowBytes).isEqualTo(1800L * 1024 * 1024);
        assertThat(options.bytesPerSecond).isEqualTo(1024 * 1024);
        assertThat(options.records * mode.rowBytes).isLessThan(RecoveryOptions.MAX_BYTES);
    }

    @ParameterizedTest
    @CsvSource({
        "destinations,9",
        "destinations,51",
        "records,19",
        "records,2097153",
        "records,9223372036854775807",
        "bytes-per-second,0",
        "bytes-per-second,1048577",
        "mode,FILE_LOADS",
        "phase,other",
        "phase,upgrade",
        "require-restored,yes",
        "run-id,invalid_id",
        "run-id,/path",
        "extra,value"
    })
    void rejectsInvalidOrUnboundedInputs(String name, String value) {
        assertThatThrownBy(() -> RecoveryOptions.parse(arguments("--" + name, value)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsDuplicateAndMissingArguments() {
        assertThatThrownBy(() -> RecoveryOptions.parse("--run-id", "a", "--run-id", "b"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RecoveryOptions.parse("--run-id"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void everyDestinationUsesAnIsolatedFixedDatasetTable() throws Exception {
        var options = RecoveryOptions.parse(arguments("--destinations", "50"));
        assertThat(IntStream.range(0, 100).mapToObj(options::table).distinct().toList())
                .hasSize(50)
                .allSatisfy(
                        table -> {
                            assertThat(table.getProject()).isEqualTo("flink-gcp");
                            assertThat(table.getDataset()).isEqualTo("flink_gcp_tier3_bigquery");
                            assertThat(table.getTable()).matches("bq_bigquery_it_d[0-9]+");
                        });
        assertThat(options.table(50)).isSameAs(options.table(0));
        var copy = InstantiationUtil.clone(options);
        assertThat(copy.table(50)).isSameAs(copy.table(0)).isEqualTo(options.table(0));
        assertThatThrownBy(() -> options.table(-1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> options.table(options.records))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(RecoveryOptions.parse(arguments("--run-id", "bigqueryit")).table(0))
                .isNotEqualTo(options.table(0));
    }

    @Test
    void upgradePreservesTheInputIdentity() {
        var initial = RecoveryOptions.parse(arguments());
        var upgrade =
                RecoveryOptions.parse(
                        arguments("--phase", "upgrade", "--require-restored", "true"));
        assertThat(upgrade.identity()).isEqualTo(initial.identity());
        assertThat(RecoveryOptions.parse(arguments("--records", "100")).identity())
                .isNotEqualTo(initial.identity());
    }
}
