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

package io.github.flink.gcp.connector.base.options;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ResourceNames}. */
class ResourceNamesTest {

    @Test
    void checkComponentReturnsTheValueItAccepted() {
        assertThat(ResourceNames.checkComponent("my-project", "project")).isEqualTo("my-project");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "\t", "\n"})
    void checkComponentRejectsABlankValue(String value) {
        assertThatThrownBy(() -> ResourceNames.checkComponent(value, "project"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("project must not be blank");
    }

    @Test
    void checkComponentRejectsANullValue() {
        assertThatThrownBy(() -> ResourceNames.checkComponent(null, "project"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("project must not be blank");
    }

    @ParameterizedTest
    @ValueSource(strings = {" p", "p ", "\tp", "p\n"})
    void checkComponentRejectsEdgeWhitespace(String value) {
        assertThatThrownBy(() -> ResourceNames.checkComponent(value, "project"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "project must not have leading or trailing whitespace: '" + value + "'");
    }

    /**
     * A C0 control character at either edge is rejected, and this is the check that does it.
     *
     * <p>{@code String.trim()} strips every character at or below {@code U+0020}, while {@code
     * isBlank()} asks {@code Character.isWhitespace}, which is false for 23 of them. The blankness
     * check above therefore passes such a value through and this one refuses it, which is why
     * ADR-0127 records that unifying the blank checks lost no coverage at these names.
     */
    @Test
    void checkComponentRejectsAnEdgeControlCharacter() {
        for (String value :
                new String[] {"\u0000p", "p\u0000", "\u001Bp", "p\u0008", "\u0000", "\u0008"}) {
            assertThatThrownBy(() -> ResourceNames.checkComponent(value, "project"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageStartingWith(
                            "project must not have leading or trailing whitespace:");
        }
    }

    /** Interior control characters are the service's to judge, so they pass. */
    @Test
    void checkComponentAcceptsAnInteriorControlCharacter() {
        assertThat(ResourceNames.checkComponent("a\u0000b", "project")).isEqualTo("a\u0000b");
    }

    @ParameterizedTest
    @ValueSource(strings = {"a/b", "/a", "a/", "projects/p/topics/t"})
    void checkComponentRejectsASlash(String value) {
        assertThatThrownBy(() -> ResourceNames.checkComponent(value, "topic"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("topic must not contain '/': '" + value + "'");
    }

    @Test
    void checkNotBlankReturnsTheValueItAccepted() {
        assertThat(ResourceNames.checkNotBlank("US", "location")).isEqualTo("US");
    }

    /** The forwarded-value check is blankness only: a separator is the service's to judge. */
    @Test
    void checkNotBlankAcceptsASlashAndEdgeWhitespace() {
        assertThat(ResourceNames.checkNotBlank("projects/p/cryptoKeys/k", "kmsKeyName"))
                .isEqualTo("projects/p/cryptoKeys/k");
        assertThat(ResourceNames.checkNotBlank(" us ", "location")).isEqualTo(" us ");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "\t"})
    void checkNotBlankRejectsABlankValue(String value) {
        assertThatThrownBy(() -> ResourceNames.checkNotBlank(value, "location"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("location must not be blank");
    }

    @Test
    void checkNotBlankRejectsANullValue() {
        assertThatThrownBy(() -> ResourceNames.checkNotBlank(null, "location"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("location must not be blank");
    }
}
