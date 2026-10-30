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

package io.github.flink.gcp.connector.bigtable.table.sink;

import org.apache.flink.table.api.ValidationException;

import io.github.flink.gcp.connector.bigtable.table.WriteMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for where {@code sink.ordering-mode} is accepted, per write mode and delivery. */
class WriteModeOptionChecksTest {

    private static final Map<String, String> PER_KEY = Map.of("sink.ordering-mode", "per-key");

    @ParameterizedTest
    @EnumSource(value = WriteMode.class, names = "AGGREGATE", mode = EnumSource.Mode.EXCLUDE)
    void everyOrderableWriteModeAcceptsTheOrderingMode(WriteMode mode) {
        assertThatCode(() -> WriteModeOptionChecks.validate(PER_KEY, mode))
                .doesNotThrowAnyException();
    }

    @ParameterizedTest
    @EnumSource(value = WriteMode.class, names = "AGGREGATE")
    void theAggregateModeRefusesTheOrderingModeEvenAtItsDefault(WriteMode mode) {
        // AddToCell contributions commute, so an order changes no result. An explicit 'none' is
        // refused too, as every explicit key the mode cannot use is.
        for (String value : new String[] {"per-key", "none"}) {
            assertThatThrownBy(
                            () ->
                                    WriteModeOptionChecks.validate(
                                            Map.of("sink.ordering-mode", value), mode))
                    .isInstanceOf(ValidationException.class)
                    .hasMessage(
                            "Option 'sink.ordering-mode' cannot be used with"
                                    + " 'sink.write-mode' = 'aggregate'.");
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = WriteMode.class,
            names = {"UPSERT", "KEEP_LATEST"})
    void theStagedSinkRefusesTheOrderingMode(WriteMode mode) {
        assertThatThrownBy(() -> WriteModeOptionChecks.validate(PER_KEY, mode, true))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Option 'sink.ordering-mode' cannot be used with")
                .hasMessageContaining("'sink.delivery-guarantee' = 'exactly-once'");
    }
}
