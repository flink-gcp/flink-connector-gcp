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

package io.github.flink.gcp.connector.base.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link OptionSetters}: what each method renames, and what it leaves alone. The
 * connectors' mapper-level rejection tests hold that each mapper line goes through it (ADR-0133).
 */
class OptionSettersTest {

    private static final ConfigOption<Integer> MAX_CELLS =
            ConfigOptions.key("sink.buffer-flush.max-cells").intType().noDefaultValue();

    private static final Consumer<Integer> POSITIVE_SETTER =
            value -> {
                if (value <= 0) {
                    throw new IllegalArgumentException("maxBatchCells must be positive");
                }
            };

    @Test
    void applyPassesAPresentValueToTheSetter() {
        Configuration config = new Configuration();
        config.set(MAX_CELLS, 7);
        List<Integer> applied = new ArrayList<>();

        OptionSetters.apply(config, MAX_CELLS, applied::add);

        assertThat(applied).containsExactly(7);
    }

    @Test
    void applyLeavesTheSetterUncalledForAnAbsentOption() {
        List<Integer> applied = new ArrayList<>();

        OptionSetters.apply(new Configuration(), MAX_CELLS, applied::add);

        assertThat(applied).isEmpty();
    }

    @Test
    void applyRenamesARejectionToTheOptionKeyAndKeepsTheSettersSentence() {
        Configuration config = new Configuration();
        config.set(MAX_CELLS, 0);

        assertThatThrownBy(() -> OptionSetters.apply(config, MAX_CELLS, POSITIVE_SETTER))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Option 'sink.buffer-flush.max-cells' is invalid:"
                                + " maxBatchCells must be positive")
                .cause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxBatchCells must be positive");
    }

    @Test
    void acceptPassesAValueToTheSetter() {
        List<Integer> applied = new ArrayList<>();

        OptionSetters.accept("sink.buffer-flush.max-cells", 7, applied::add);

        assertThat(applied).containsExactly(7);
    }

    @Test
    void acceptRenamesARejectionToTheGivenKey() {
        assertThatThrownBy(
                        () ->
                                OptionSetters.accept(
                                        "sink.buffer-flush.max-cells", -1, POSITIVE_SETTER))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Option 'sink.buffer-flush.max-cells' is invalid:"
                                + " maxBatchCells must be positive")
                .hasCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void acceptAppliesNothingForANullValue() {
        List<Integer> applied = new ArrayList<>();

        OptionSetters.<Integer>accept("sink.buffer-flush.max-cells", null, applied::add);

        assertThat(applied).isEmpty();
    }

    @Test
    void convertReturnsTheConvertersResult() {
        Integer converted = OptionSetters.convert("scan.start-time", "42", Integer::parseInt);

        assertThat(converted).isEqualTo(42);
    }

    @Test
    void convertRenamesARejectionToTheGivenKey() {
        assertThatThrownBy(
                        () ->
                                OptionSetters.convert(
                                        "scan.start-time",
                                        "soon",
                                        value -> {
                                            throw new IllegalArgumentException(
                                                    "startTime must be an instant");
                                        }))
                .isInstanceOf(ValidationException.class)
                .hasMessage("Option 'scan.start-time' is invalid: startTime must be an instant")
                .hasCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void convertRenamesASubclassOfIllegalArgumentException() {
        // A parser's NumberFormatException is an IllegalArgumentException, so it is renamed too.
        assertThatThrownBy(
                        () -> OptionSetters.convert("scan.start-time", "soon", Integer::parseInt))
                .isInstanceOf(ValidationException.class)
                .hasMessageStartingWith("Option 'scan.start-time' is invalid: ")
                .hasCauseInstanceOf(NumberFormatException.class);
    }

    /**
     * The contract is {@code IllegalArgumentException} only: a cross-field {@code build()} check's
     * {@code IllegalStateException} names two knobs and has no single key to carry (ADR-0133).
     */
    @Test
    void otherExceptionsPassThroughUnrenamed() {
        IllegalStateException crossField = new IllegalStateException("a must not exceed b");
        Consumer<Integer> setter =
                value -> {
                    throw crossField;
                };

        Configuration config = new Configuration();
        config.set(MAX_CELLS, 1);

        assertThatThrownBy(() -> OptionSetters.apply(config, MAX_CELLS, setter))
                .isSameAs(crossField);
        assertThatThrownBy(() -> OptionSetters.accept("project", 1, setter)).isSameAs(crossField);
        assertThatThrownBy(
                        () ->
                                OptionSetters.convert(
                                        "project",
                                        1,
                                        value -> {
                                            throw crossField;
                                        }))
                .isSameAs(crossField);
    }
}
