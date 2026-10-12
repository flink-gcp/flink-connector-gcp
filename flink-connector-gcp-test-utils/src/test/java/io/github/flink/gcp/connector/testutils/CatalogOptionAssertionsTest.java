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

package io.github.flink.gcp.connector.testutils;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Set;
import java.util.stream.Stream;

import static io.github.flink.gcp.connector.testutils.CatalogOptionAssertions.assertOptionsSurviveCatalogRoundTrip;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Holds the rejection paths that the clean connector option sets cannot exercise. */
class CatalogOptionAssertionsTest {

    @Test
    void anEmptyRequiredSetDoesNotHideOptionalOptions() {
        assertThatThrownBy(
                        () ->
                                assertOptionsSurviveCatalogRoundTrip(
                                        Set.of(), Set.of(option("schema.test-option"))))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("schema.test-option");
    }

    @Test
    void anEmptyOptionalSetDoesNotHideRequiredOptions() {
        assertThatThrownBy(
                        () ->
                                assertOptionsSurviveCatalogRoundTrip(
                                        Set.of(option("schema.test-option")), Set.of()))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("schema.test-option");
    }

    @Test
    void anEmptyOptionSetIsRejected() {
        assertThatThrownBy(() -> assertOptionsSurviveCatalogRoundTrip(Set.of(), Set.of()))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("factory option set must not be empty");
    }

    @ParameterizedTest
    @MethodSource("reservedKeys")
    void aDroppedRequiredOptionIsRejected(String key) {
        assertThatThrownBy(
                        () ->
                                assertOptionsSurviveCatalogRoundTrip(
                                        Set.of(option(key)), Set.of(option("project"))))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(key);
    }

    @ParameterizedTest
    @MethodSource("reservedKeys")
    void aDroppedOptionalOptionIsRejected(String key) {
        assertThatThrownBy(
                        () ->
                                assertOptionsSurviveCatalogRoundTrip(
                                        Set.of(option("project")), Set.of(option(key))))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(key);
    }

    // Every key must be reserved on each Flink version CI runs: Flink 2.3 replaced the reserved
    // "definition-query" with "original-query" and "expanded-query", while "refresh-mode" is
    // reserved from 1.20 through the 2.4 snapshot.
    private static Stream<String> reservedKeys() {
        return Stream.of(
                "schema.test-option", "partition.keys.test-option", "comment", "refresh-mode");
    }

    private static ConfigOption<String> option(String key) {
        return ConfigOptions.key(key).stringType().noDefaultValue();
    }
}
