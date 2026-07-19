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

package io.github.flink.gcp.connector.bigquery.sink.writer;

import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BigQueryTableCreator}. */
class BigQueryTableCreatorTest {

    private static final TableDestination DESTINATION = TableDestination.of("p", "d", "t");

    private static final TableSchema SCHEMA =
            TableSchema.newBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("event_ts")
                                    .setType(TableFieldSchema.Type.TIMESTAMP)
                                    .setMode(TableFieldSchema.Mode.NULLABLE)
                                    .build())
                    .build();

    @Test
    void buildsPlainTableByDefault() {
        TableInfo tableInfo =
                BigQueryTableCreator.buildTableInfo(
                        DESTINATION, SCHEMA, TableCreateOptions.defaults());

        assertThat(tableInfo.getTableId().getProject()).isEqualTo("p");
        assertThat(tableInfo.getTableId().getDataset()).isEqualTo("d");
        assertThat(tableInfo.getTableId().getTable()).isEqualTo("t");
        StandardTableDefinition definition = tableInfo.getDefinition();
        assertThat(definition.getSchema().getFields().get("event_ts")).isNotNull();
        assertThat(definition.getTimePartitioning()).isNull();
        assertThat(definition.getClustering()).isNull();
    }

    @Test
    void appliesPartitioningAndClustering() {
        TableCreateOptions options =
                TableCreateOptions.builder()
                        .timePartitioning(TableCreateOptions.TimePartitioningType.DAY, "event_ts")
                        .timePartitioningExpiration(Duration.ofDays(90))
                        .clusteredFields(Arrays.asList("event_ts"))
                        .build();

        TableInfo tableInfo = BigQueryTableCreator.buildTableInfo(DESTINATION, SCHEMA, options);

        StandardTableDefinition definition = tableInfo.getDefinition();
        TimePartitioning partitioning = definition.getTimePartitioning();
        assertThat(partitioning.getType()).isEqualTo(TimePartitioning.Type.DAY);
        assertThat(partitioning.getField()).isEqualTo("event_ts");
        assertThat(partitioning.getExpirationMs()).isEqualTo(Duration.ofDays(90).toMillis());
        assertThat(definition.getClustering().getFields()).containsExactly("event_ts");
    }

    @Test
    void appliesIngestionTimePartitioning() {
        TableCreateOptions options =
                TableCreateOptions.builder()
                        .timePartitioning(TableCreateOptions.TimePartitioningType.MONTH)
                        .build();

        TableInfo tableInfo = BigQueryTableCreator.buildTableInfo(DESTINATION, SCHEMA, options);

        StandardTableDefinition definition = tableInfo.getDefinition();
        assertThat(definition.getTimePartitioning().getType())
                .isEqualTo(TimePartitioning.Type.MONTH);
        assertThat(definition.getTimePartitioning().getField()).isNull();
    }
}
