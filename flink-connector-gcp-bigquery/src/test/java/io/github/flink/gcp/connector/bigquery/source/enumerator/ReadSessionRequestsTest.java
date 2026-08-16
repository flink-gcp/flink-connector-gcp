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

package io.github.flink.gcp.connector.bigquery.source.enumerator;

import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import io.github.flink.gcp.connector.bigquery.source.TestSources;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/** The whole of the connector's push-down surface, as a pure mapping. */
class ReadSessionRequestsTest {

    @Test
    void readsTheWholeTableByDefault() {
        CreateReadSessionRequest request =
                ReadSessionRequests.of(TestSources.config(), TestSources.TABLE);

        assertThat(request.getParent()).isEqualTo("projects/p");
        assertThat(request.getReadSession().getTable()).isEqualTo("projects/p/datasets/d/tables/t");
        assertThat(request.getReadSession().getDataFormat()).isEqualTo(DataFormat.AVRO);
        assertThat(request.getReadSession().getReadOptions().getSelectedFieldsList()).isEmpty();
        assertThat(request.getReadSession().getReadOptions().getRowRestriction()).isEmpty();
        assertThat(request.getReadSession().hasTableModifiers()).isFalse();
    }

    @Test
    void leavesBothStreamCountsUnsetWhenBigQueryIsToDecide() {
        CreateReadSessionRequest request =
                ReadSessionRequests.of(TestSources.config(), TestSources.TABLE);

        // Zero is how the API spells "the server decides", and an unset field is how the field is
        // left at zero — a source that sent an explicit zero would say the same thing, but the
        // emulator rejects any explicit count above one, so the distinction is worth keeping.
        assertThat(request.getMaxStreamCount()).isZero();
        assertThat(request.getPreferredMinStreamCount()).isZero();
    }

    @Test
    void carriesTheProjectionAndTheRestriction() {
        CreateReadSessionRequest request =
                ReadSessionRequests.of(
                        TestSources.config(
                                builder ->
                                        builder.selectedFields("id", "name")
                                                .rowRestriction("id > 3")),
                        TestSources.TABLE);

        assertThat(request.getReadSession().getReadOptions().getSelectedFieldsList())
                .containsExactly("id", "name");
        assertThat(request.getReadSession().getReadOptions().getRowRestriction())
                .isEqualTo("id > 3");
    }

    @Test
    void carriesTheSnapshotTimeAsATableModifier() {
        Instant snapshot = Instant.parse("2026-08-01T00:00:00.000000123Z");

        CreateReadSessionRequest request =
                ReadSessionRequests.of(
                        TestSources.config(builder -> builder.snapshotTime(snapshot)),
                        TestSources.TABLE);

        assertThat(request.getReadSession().getTableModifiers().getSnapshotTime().getSeconds())
                .isEqualTo(snapshot.getEpochSecond());
        assertThat(request.getReadSession().getTableModifiers().getSnapshotTime().getNanos())
                .isEqualTo(123);
    }

    @Test
    void carriesTheStreamCountsWhenTheyAreSet() {
        CreateReadSessionRequest request =
                ReadSessionRequests.of(
                        TestSources.config(
                                builder -> builder.maxStreamCount(20).preferredMinStreamCount(8)),
                        TestSources.TABLE);

        assertThat(request.getMaxStreamCount()).isEqualTo(20);
        assertThat(request.getPreferredMinStreamCount()).isEqualTo(8);
    }

    @Test
    void billsTheReadToTheParentProjectWhenOneIsSet() {
        CreateReadSessionRequest request =
                ReadSessionRequests.of(
                        TestSources.config(builder -> builder.parentProject("payer")),
                        TestSources.TABLE);

        assertThat(request.getParent()).isEqualTo("projects/payer");
        assertThat(request.getReadSession().getTable()).isEqualTo("projects/p/datasets/d/tables/t");
    }
}
