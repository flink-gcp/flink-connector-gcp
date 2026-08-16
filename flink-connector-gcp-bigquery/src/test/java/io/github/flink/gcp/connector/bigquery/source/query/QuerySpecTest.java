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

package io.github.flink.gcp.connector.bigquery.source.query;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The one query this connector writes rather than forwards.
 *
 * <p>Pure, so the shape of the generated SQL is pinned without a client: what a materialized view
 * costs is decided by the columns this SELECT names.
 */
class QuerySpecTest {

    private static final TableDestination VIEW = TableDestination.of("my-project", "ds", "v");

    @Test
    void selectsEveryColumnWhenNoProjectionWasAskedFor() {
        assertThat(spec(Collections.emptyList()).getSql())
                .isEqualTo("SELECT * FROM `my-project.ds.v`");
    }

    @Test
    void foldsTheProjectionIntoTheSelect() {
        // Not decoration: a view's SELECT * scans every column and the query is billed for the
        // scan, so leaving this to the read session would prune the transfer after paying for it.
        assertThat(spec(Arrays.asList("id", "name")).getSql())
                .isEqualTo("SELECT `id`, `name` FROM `my-project.ds.v`");
    }

    @Test
    void escapesABacktickInAColumnName() {
        // BigQuery allows one in a flexible column name. Dropping it would name a different
        // column; leaving it unescaped would end the quoted identifier early.
        assertThat(spec(Collections.singletonList("we`ird")).getSql())
                .isEqualTo("SELECT `we\\`ird` FROM `my-project.ds.v`");
    }

    @Test
    void carriesTheJobSettingsThroughUnchanged() {
        QuerySpec spec =
                QuerySpec.forView(
                        VIEW, Collections.emptyList(), "payer", "asia-northeast1", "scratch");

        assertThat(spec.getProject()).isEqualTo("payer");
        assertThat(spec.getLocation()).isEqualTo("asia-northeast1");
        assertThat(spec.getResultDataset()).isEqualTo("scratch");
    }

    private static QuerySpec spec(java.util.List<String> selectedFields) {
        return QuerySpec.forView(VIEW, selectedFields, "my-project", null, null);
    }
}
