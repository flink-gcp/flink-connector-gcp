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

package io.github.flink.gcp.connector.tier3.smoke;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SmokeOptionsTest {
    @Test
    void defaultsDescribeTheBoundedSmokeInput() {
        SmokeOptions options = SmokeOptions.parse("--run-id", "smoke-test");
        assertThat(options.records).isEqualTo(18000);
        assertThat(options.recordsPerSecond).isEqualTo(10);
        assertThat(options.phase).isEqualTo("initial");
        assertThat(options.requireRestored).isFalse();
    }

    @ParameterizedTest
    @CsvSource({
        "records,0",
        "records,18001",
        "records-per-second,0",
        "records-per-second,NaN",
        "records-per-second,Infinity",
        "records-per-second,11",
        "phase,unknown",
        "require-restored,yes",
        "run-id,UPPER",
        "unknown,value"
    })
    void invalidArgumentsNameTheInput(String name, String value) {
        assertThatThrownBy(() -> SmokeOptions.parse("--run-id", "smoke-test", "--" + name, value))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--" + name);
    }
}
