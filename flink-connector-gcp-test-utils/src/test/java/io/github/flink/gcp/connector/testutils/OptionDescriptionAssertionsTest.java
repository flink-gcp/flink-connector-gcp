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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Holds the rejection path that no clean connector option description can exercise.
 *
 * <p>The five consumers prove that their current descriptions pass and that they call the shared
 * assertion. These fixtures prove that each recorded phrase can still make the shared assertion
 * fail.
 */
class OptionDescriptionAssertionsTest {

    private static final List<RestatementFixture> RECORDED_RESTATEMENT_FORMS =
            List.of(
                    fixture("by default", "By default, the test value is used."),
                    fixture("defaults to", "Defaults to the test value."),
                    fixture("when unset", "When unset, the test value is used."),
                    fixture("unset means", "Unset means the test value is used."),
                    fixture("when absent", "The test value is used when absent."),
                    fixture("absent uses", "Absent uses the test value."),
                    fixture("absent,", "Absent, the test value is used."),
                    fixture("unset uses", "Unset uses the test value."),
                    fixture("unset keeps", "Unset keeps the test value."),
                    fixture("unset leaves", "Unset leaves the test value unchanged."),
                    fixture("is the default", "The test value is the default."),
                    fixture("and the default", "The test value and the default are identical."));

    @Test
    void directFixturesMatchTheSharedPhraseInventory() {
        assertThat(OptionDescriptionAssertions.DEFAULT_RESTATEMENT_PHRASES)
                .containsExactlyElementsOf(
                        RECORDED_RESTATEMENT_FORMS.stream()
                                .map(RestatementFixture::phrase)
                                .toList());
    }

    @Test
    void cleanDescriptionIsAccepted() {
        assertThatCode(
                        () ->
                                OptionDescriptionAssertions.assertNoDefaultRestatement(
                                        "test.option",
                                        "Uses the credentials resolved by the client.",
                                        "reference/test.md"))
                .doesNotThrowAnyException();
    }

    @Test
    void everyFixtureDescriptionContainsOnlyItsRecordedPhrase() {
        for (RestatementFixture fixture : RECORDED_RESTATEMENT_FORMS) {
            assertThat(fixture.description())
                    .as("fixture description for phrase '%s'", fixture.phrase())
                    .containsIgnoringCase(fixture.phrase());
            for (RestatementFixture other : RECORDED_RESTATEMENT_FORMS) {
                if (fixture != other) {
                    assertThat(fixture.description())
                            .as(
                                    "fixture for phrase '%s' is shadowed by '%s'",
                                    fixture.phrase(), other.phrase())
                            .doesNotContainIgnoringCase(other.phrase());
                }
            }
        }
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("recordedRestatementForms")
    void everyRecordedRestatementFormIsRejected(RestatementFixture fixture) {
        Throwable thrown =
                catchThrowable(
                        () ->
                                OptionDescriptionAssertions.assertNoDefaultRestatement(
                                        "test.option", fixture.description(), "reference/test.md"));

        assertThat(thrown)
                .as("recorded phrase '%s'", fixture.phrase())
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("test.option")
                .hasMessageContaining("reference/test.md");
    }

    private static Stream<RestatementFixture> recordedRestatementForms() {
        return RECORDED_RESTATEMENT_FORMS.stream();
    }

    private static RestatementFixture fixture(String phrase, String description) {
        return new RestatementFixture(phrase, description);
    }

    private static final class RestatementFixture {

        private final String phrase;
        private final String description;

        private RestatementFixture(String phrase, String description) {
            this.phrase = phrase;
            this.description = description;
        }

        private String phrase() {
            return phrase;
        }

        private String description() {
            return description;
        }

        @Override
        public String toString() {
            return phrase;
        }
    }
}
