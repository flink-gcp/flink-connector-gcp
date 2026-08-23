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

package io.github.flink.gcp.connector.spanner.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.description.HtmlFormatter;

import org.junit.jupiter.api.Test;

import java.util.List;

import static io.github.flink.gcp.connector.testutils.OptionDescriptionAssertions.assertNoDefaultRestatement;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards on the option set as a whole.
 *
 * <p>{@code SpannerOptionParityTest} holds the option set and the builder setters to each other;
 * this class holds the two halves of the no-restated-default rule: which options may carry a {@code
 * defaultValue()}, and that no description states one in prose.
 */
class SpannerConnectorOptionsTest {

    private static List<ConfigOption<?>> declaredOptions() {
        return DeclaredOptions.all();
    }

    @Test
    void onlyTheRecordedOptionsCarryADefault() {
        // A mapped option's default lives on the connector's own builder and is applied by not
        // calling a setter, so a copy here would usually be a second copy that nothing keeps in
        // step. The recorded exceptions are of two kinds: table-owned selectors the factory reads
        // with get() (dialect, scan.mode, scan.startup.mode, lookup.async), and three change-stream
        // knobs whose defaultValue() references the builder's own constant, so the compiler keeps
        // the two in step. Anything joining this list does so deliberately.
        assertThat(declaredOptions()).isNotEmpty();
        assertThat(declaredOptions())
                .filteredOn(ConfigOption::hasDefaultValue)
                .containsExactlyInAnyOrder(
                        SpannerConnectorOptions.DIALECT,
                        SpannerConnectorOptions.SCAN_MODE,
                        SpannerConnectorOptions.SCAN_STARTUP_MODE,
                        SpannerConnectorOptions.SCAN_CHANGE_STREAM_ABSENT_RETENTION_FALLBACK,
                        SpannerConnectorOptions.SCAN_CHANGE_STREAM_HEARTBEAT_INTERVAL,
                        SpannerConnectorOptions.SCAN_MAX_CONCURRENT_QUERIES_PER_SUBTASK,
                        SpannerConnectorOptions.LOOKUP_ASYNC);
    }

    @Test
    void noDescriptionRestatesADefault() {
        // The half of the rule above a ConfigOption cannot express: a default written into prose —
        // a builder's, an option's own defaultValue(), or the value absence selects — is a second
        // copy that nothing keeps in step. "Unset fails the source instead" is not in that class:
        // absence selecting a failure is the option's contract, not a default. The shared
        // assertion owns the #1045 cross-module sweep's recorded phrases.
        //
        // When this fires, the description is what changes. reference/spanner.md is where a
        // mapped option's default is written — a derived one included, carrying both its
        // derivation and its resolved value — and the table page's option row is where a
        // table-owned option's default is written.
        HtmlFormatter formatter = new HtmlFormatter();
        assertThat(declaredOptions()).isNotEmpty();
        assertThat(declaredOptions())
                .allSatisfy(
                        option ->
                                assertNoDefaultRestatement(
                                        option.key(),
                                        formatter.format(option.description()),
                                        "the spanner reference or table docs page"));
    }
}
