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

package io.github.flink.gcp.connector.bigquery.table.source;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.source.BigQueryStorageReadSource;
import io.github.flink.gcp.connector.bigquery.source.TestSources;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the nested projection a source advertises and the session fields it requests. */
class BigQueryDynamicSourceNestedProjectionTest {

    private static final DataType PHYSICAL =
            DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.BIGINT()),
                    DataTypes.FIELD(
                            "r",
                            DataTypes.ROW(
                                    DataTypes.FIELD("a", DataTypes.BIGINT()),
                                    DataTypes.FIELD("b", DataTypes.STRING()))));

    @Test
    void advertisesNestedProjectionUnlessANotNullFieldSitsBelowANullableRow() {
        assertThat(source(PHYSICAL).supportsNestedProjection()).isTrue();
        assertThat(
                        source(
                                        DataTypes.ROW(
                                                DataTypes.FIELD(
                                                        "r",
                                                        DataTypes.ROW(
                                                                        DataTypes.FIELD(
                                                                                "a",
                                                                                DataTypes.BIGINT()
                                                                                        .notNull()))
                                                                .notNull())))
                                .supportsNestedProjection())
                .isTrue();
        assertThat(
                        source(
                                        DataTypes.ROW(
                                                DataTypes.FIELD(
                                                        "r",
                                                        DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "a",
                                                                        DataTypes.BIGINT()
                                                                                .notNull())))))
                                .supportsNestedProjection())
                .isFalse();
    }

    @Test
    void nestedPathsBecomeDottedSelectedFieldsAndSurviveACopy() {
        BigQueryDynamicSource source = source(PHYSICAL);
        source.applyProjection(
                new int[][] {{1, 1}, {0}, {1, 0}},
                DataTypes.ROW(
                        DataTypes.FIELD("r_b", DataTypes.STRING()),
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("r_a", DataTypes.BIGINT())));

        assertThat(selectedFields(source)).containsExactly("r.b", "id", "r.a");
        BigQueryDynamicSource copy = (BigQueryDynamicSource) source.copy();
        assertThat(copy).isEqualTo(source).hasSameHashCodeAs(source);
        assertThat(selectedFields(copy)).containsExactly("r.b", "id", "r.a");
    }

    @Test
    void aQuerySourceRequestsTheSameDottedFieldsFromItsResult() {
        BigQueryDynamicSource source =
                BigQueryDynamicSource.builder()
                        .physicalDataType(PHYSICAL)
                        .query("SELECT * FROM `p.d.t`")
                        .parentProject("p")
                        .emulatorEndpoint("localhost:9060")
                        .emulatorRestEndpoint("localhost:9050")
                        .build();
        source.applyProjection(
                new int[][] {{1, 0}}, DataTypes.ROW(DataTypes.FIELD("r_a", DataTypes.BIGINT())));

        assertThat(selectedFields(source)).containsExactly("r.a");
    }

    @Test
    void comparesNestedProjectionsByTheirPaths() {
        DataType produced = DataTypes.ROW(DataTypes.FIELD("r_a", DataTypes.BIGINT()));
        BigQueryDynamicSource first = source(PHYSICAL);
        first.applyProjection(new int[][] {{1, 0}}, produced);
        BigQueryDynamicSource same = source(PHYSICAL);
        same.applyProjection(new int[][] {{1, 0}}, produced);
        BigQueryDynamicSource other = source(PHYSICAL);
        other.applyProjection(new int[][] {{1, 1}}, produced);

        assertThat(first).isEqualTo(same).hasSameHashCodeAs(same);
        assertThat(other).isNotEqualTo(first);
    }

    private static List<String> selectedFields(BigQueryDynamicSource source) {
        SourceProvider provider =
                (SourceProvider) source.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        return TestSources.configOf((BigQueryStorageReadSource<?>) provider.createSource())
                .getSelectedFields();
    }

    private static BigQueryDynamicSource source(DataType physical) {
        return BigQueryDynamicSource.builder()
                .physicalDataType(physical)
                .table(TableDestination.of("p", "d", "t"))
                .parentProject("p")
                .emulatorEndpoint("localhost:9060")
                .build();
    }
}
