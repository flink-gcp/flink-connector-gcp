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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Assertions shared by the Table API option-set tests. */
@Internal
public final class OptionDescriptionAssertions {

    @VisibleForTesting
    static final List<String> DEFAULT_RESTATEMENT_PHRASES =
            List.of(
                    "by default",
                    "defaults to",
                    "when unset",
                    "unset means",
                    "when absent",
                    "absent uses",
                    "absent,",
                    "unset uses",
                    "unset keeps",
                    "unset leaves",
                    "is the default",
                    "and the default");

    /**
     * Rejects the default-restatement forms that the cross-connector sweep found.
     *
     * <p>Pub/Sub supplied the first two phrases through #778/#838: {@code Off by default:} and two
     * {@code Defaults to twice the effective flow-control ... limit} descriptions accumulated while
     * every existing check stayed green. #1001 added two absent-value forms, and #1045 found the
     * remaining eight in the sibling connectors. The list is a regression guard over those recorded
     * forms, not a semantic parser for arbitrary prose. Keeping it here makes the five connector
     * guards use one list while leaving their different option-discovery rules local.
     *
     * <p>The caller formats the joined Flink description before this assertion. That keeps
     * string-literal boundaries from hiding a phrase and keeps Flink's unannotated {@code
     * HtmlFormatter} out of this module's API-tier-audited main sources.
     *
     * @param optionKey the option whose description is checked
     * @param formattedDescription the joined and formatted option description
     * @param documentationHome where the option's default is documented
     */
    public static void assertNoDefaultRestatement(
            String optionKey, String formattedDescription, String documentationHome) {
        for (String phrase : DEFAULT_RESTATEMENT_PHRASES) {
            assertThat(formattedDescription)
                    .as(
                            "option '%s' restates a default; %s is where a default is written",
                            optionKey, documentationHome)
                    .doesNotContainIgnoringCase(phrase);
        }
    }

    private OptionDescriptionAssertions() {}
}
