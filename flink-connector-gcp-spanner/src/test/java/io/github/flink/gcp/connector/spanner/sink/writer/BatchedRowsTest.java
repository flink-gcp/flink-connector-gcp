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

package io.github.flink.gcp.connector.spanner.sink.writer;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BatchedRows}. */
class BatchedRowsTest {

    @Test
    void aWholeTableOverlapsEveryRowOfItsTableAndNoOther() {
        BatchedRows batch = new BatchedRows();
        batch.add(PrimaryKeys.Footprint.wholeTable("t"));

        assertThat(batch.overlaps(row("t", "1"))).isTrue();
        assertThat(batch.overlaps(PrimaryKeys.Footprint.wholeTable("t"))).isTrue();
        assertThat(batch.overlaps(row("u", "1"))).isFalse();
    }

    @Test
    void aWholeTableOverlapsABatchHoldingAnyRowOfIt() {
        BatchedRows batch = new BatchedRows();
        batch.add(row("t", "1"));

        assertThat(batch.overlaps(PrimaryKeys.Footprint.wholeTable("t"))).isTrue();
        assertThat(batch.overlaps(PrimaryKeys.Footprint.wholeTable("u"))).isFalse();
        assertThat(batch.overlaps(row("t", "2"))).isFalse();
    }

    @Test
    void theWholeDatabaseOverlapsEveryMutation() {
        BatchedRows batch = new BatchedRows();
        assertThat(batch.overlaps(PrimaryKeys.Footprint.wholeDatabase())).isFalse();

        batch.add(row("t", "1"));
        assertThat(batch.overlaps(PrimaryKeys.Footprint.wholeDatabase())).isTrue();

        BatchedRows database = new BatchedRows();
        database.add(PrimaryKeys.Footprint.wholeDatabase());
        assertThat(database.overlaps(row("u", "1"))).isTrue();
        assertThat(database.overlaps(PrimaryKeys.Footprint.wholeTable("u"))).isTrue();
    }

    @Test
    void clearForgetsEverything() {
        BatchedRows batch = new BatchedRows();
        batch.add(row("t", "1"));
        batch.add(PrimaryKeys.Footprint.wholeTable("u"));
        batch.add(PrimaryKeys.Footprint.wholeDatabase());

        batch.clear();

        assertThat(batch.overlaps(row("t", "1"))).isFalse();
        assertThat(batch.overlaps(row("u", "1"))).isFalse();
    }

    private static PrimaryKeys.Footprint row(String table, Object key) {
        Set<List<Object>> rows = Collections.singleton(Collections.singletonList(key));
        return PrimaryKeys.Footprint.rows(table, rows);
    }
}
