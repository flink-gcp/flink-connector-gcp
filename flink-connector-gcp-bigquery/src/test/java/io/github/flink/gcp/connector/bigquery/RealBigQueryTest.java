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

package io.github.flink.gcp.connector.bigquery;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RealBigQueryTest {
    @Test
    void everyTableDeletionIsAttemptedAndFailuresRemainVisible() {
        List<String> attempted = new ArrayList<>();
        RuntimeException first = new RuntimeException("first");
        RuntimeException second = new RuntimeException("second");
        assertThatThrownBy(
                        () ->
                                RealBigQuery.deleteTables(
                                        table -> {
                                            attempted.add(table);
                                            if (table.equals("a")) {
                                                throw first;
                                            }
                                            if (table.equals("b")) {
                                                throw second;
                                            }
                                        },
                                        "a",
                                        "b",
                                        "c"))
                .isSameAs(first)
                .hasSuppressedException(second);
        assertThat(attempted).containsExactly("a", "b", "c");
    }
}
