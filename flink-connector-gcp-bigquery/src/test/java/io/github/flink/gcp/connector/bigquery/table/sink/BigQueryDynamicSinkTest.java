/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.bigquery.table.sink;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.types.DataType;

import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.SchemaUpdateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.storage.DefaultStreamOptions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BigQueryDynamicSink}. */
class BigQueryDynamicSinkTest {

    private static final DataType ROW =
            DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.STRING()),
                    DataTypes.FIELD("amount", DataTypes.BIGINT()));

    private static final TableDestination DESTINATION =
            TableDestination.of("my-project", "my_dataset", "my_table");

    private static BigQueryDynamicSink sink() {
        return new BigQueryDynamicSink(
                ROW,
                DESTINATION,
                RowDataSchemaOptions.defaults(),
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    @Test
    void isInsertOnlyWhateverThePlannerAsksFor() {
        assertThat(sink().getChangelogMode(ChangelogMode.all()))
                .isEqualTo(ChangelogMode.insertOnly());
        assertThat(sink().getChangelogMode(ChangelogMode.upsert()))
                .isEqualTo(ChangelogMode.insertOnly());
    }

    @Test
    void describesItselfByName() {
        assertThat(sink().asSummaryString()).isEqualTo("BigQuery table sink");
    }

    @Test
    void aCopyEqualsTheOriginal() {
        DynamicTableSink copy = sink().copy();
        assertThat(copy).isEqualTo(sink()).hasSameHashCodeAs(sink());
        assertThat(copy).isNotSameAs(sink());
    }

    @Test
    void everyFieldOfTheSinkIsPartOfItsIdentity() {
        BigQueryDynamicSink base = sink();

        assertThat(
                        new BigQueryDynamicSink(
                                DataTypes.ROW(DataTypes.FIELD("id", DataTypes.STRING())),
                                DESTINATION,
                                RowDataSchemaOptions.defaults(),
                                null,
                                null,
                                null,
                                null,
                                null,
                                null,
                                null))
                .isNotEqualTo(base);
        assertThat(
                        new BigQueryDynamicSink(
                                ROW,
                                TableDestination.of("my-project", "my_dataset", "other_table"),
                                RowDataSchemaOptions.defaults(),
                                null,
                                null,
                                null,
                                null,
                                null,
                                null,
                                null))
                .isNotEqualTo(base);
        assertThat(
                        new BigQueryDynamicSink(
                                ROW,
                                DESTINATION,
                                RowDataSchemaOptions.builder()
                                        .jsonFieldPaths(Collections.singletonList("id"))
                                        .build(),
                                null,
                                null,
                                null,
                                null,
                                null,
                                null,
                                null))
                .isNotEqualTo(base);
        assertThat(
                        new BigQueryDynamicSink(
                                ROW,
                                DESTINATION,
                                RowDataSchemaOptions.defaults(),
                                CreateDisposition.CREATE_NEVER,
                                null,
                                null,
                                null,
                                null,
                                null,
                                null))
                .isNotEqualTo(base);
        assertThat(
                        new BigQueryDynamicSink(
                                ROW,
                                DESTINATION,
                                RowDataSchemaOptions.defaults(),
                                null,
                                "US",
                                null,
                                null,
                                null,
                                null,
                                null))
                .isNotEqualTo(base);
        assertThat(
                        new BigQueryDynamicSink(
                                ROW,
                                DESTINATION,
                                RowDataSchemaOptions.defaults(),
                                null,
                                null,
                                SchemaUpdateOptions.builder().allowNewFields().build(),
                                null,
                                null,
                                null,
                                null))
                .isNotEqualTo(base);
        assertThat(
                        new BigQueryDynamicSink(
                                ROW,
                                DESTINATION,
                                RowDataSchemaOptions.defaults(),
                                null,
                                null,
                                null,
                                DefaultStreamOptions.builder().maxInflightRequests(5).build(),
                                null,
                                null,
                                null))
                .isNotEqualTo(base);
        assertThat(
                        new BigQueryDynamicSink(
                                ROW,
                                DESTINATION,
                                RowDataSchemaOptions.defaults(),
                                null,
                                null,
                                null,
                                null,
                                "localhost:9060",
                                null,
                                null))
                .isNotEqualTo(base);
        assertThat(
                        new BigQueryDynamicSink(
                                ROW,
                                DESTINATION,
                                RowDataSchemaOptions.defaults(),
                                null,
                                null,
                                null,
                                null,
                                null,
                                "localhost:9050",
                                null))
                .isNotEqualTo(base);
        assertThat(
                        new BigQueryDynamicSink(
                                ROW,
                                DESTINATION,
                                RowDataSchemaOptions.defaults(),
                                null,
                                null,
                                null,
                                null,
                                null,
                                null,
                                3))
                .isNotEqualTo(base);
    }
}
