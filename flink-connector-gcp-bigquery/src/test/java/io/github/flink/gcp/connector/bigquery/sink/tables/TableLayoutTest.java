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

package io.github.flink.gcp.connector.bigquery.sink.tables;

import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.ViewDefinition;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TableLayout}. */
class TableLayoutTest {

    private static final Schema SCHEMA =
            Schema.of(
                    Field.of("ts", StandardSQLTypeName.TIMESTAMP),
                    Field.of("n", StandardSQLTypeName.INT64));

    private static final RangePartitioning RANGE =
            RangePartitioning.newBuilder()
                    .setField("n")
                    .setRange(
                            RangePartitioning.Range.newBuilder()
                                    .setStart(0L)
                                    .setEnd(100L)
                                    .setInterval(10L)
                                    .build())
                    .build();

    @Test
    void aTablesLayoutDropsPartitionExpirationAndThePartitionFilterRequirement() {
        StandardTableDefinition definition =
                StandardTableDefinition.newBuilder()
                        .setSchema(SCHEMA)
                        .setTimePartitioning(
                                TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
                                        .setField("ts")
                                        .setExpirationMs(86_400_000L)
                                        .setRequirePartitionFilter(true)
                                        .build())
                        .setClustering(Clustering.newBuilder().setFields(List.of("n")).build())
                        .build();

        TableLayout layout = TableLayout.of(definition);

        assertThat(layout.getTimePartitioning())
                .isEqualTo(
                        TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
                                .setField("ts")
                                .build());
        assertThat(layout.getRangePartitioning()).isNull();
        assertThat(layout.getClustering().getFields()).containsExactly("n");
        assertThat(layout.isEmpty()).isFalse();
    }

    @Test
    void ingestionTimePartitioningHasNoField() {
        TableLayout layout =
                TableLayout.of(
                        StandardTableDefinition.newBuilder()
                                .setSchema(SCHEMA)
                                .setTimePartitioning(
                                        TimePartitioning.of(TimePartitioning.Type.HOUR))
                                .build());

        assertThat(layout.getTimePartitioning().getType()).isEqualTo(TimePartitioning.Type.HOUR);
        assertThat(layout.getTimePartitioning().getField()).isNull();
        assertThat(layout.fingerprint()).isEqualTo("time=HOUR:_PARTITIONTIME");
    }

    @Test
    void anUnpartitionedUnclusteredOrNonStandardTableHasNoLayout() {
        assertThat(TableLayout.of(StandardTableDefinition.of(SCHEMA))).isSameAs(TableLayout.NONE);
        assertThat(TableLayout.of(ViewDefinition.of("SELECT 1"))).isSameAs(TableLayout.NONE);
        assertThat(TableLayout.of((StandardTableDefinition) null)).isSameAs(TableLayout.NONE);
        assertThat(TableLayout.of(null, null, Clustering.newBuilder().setFields(List.of()).build()))
                .isSameAs(TableLayout.NONE);
        assertThat(TableLayout.NONE.isEmpty()).isTrue();
        assertThat(TableLayout.NONE.fingerprint()).isEmpty();
    }

    @Test
    void theFingerprintNamesEveryPart() {
        TableLayout layout =
                TableLayout.of(
                        TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
                                .setField("ts")
                                .build(),
                        null,
                        Clustering.newBuilder().setFields(List.of("a", "b")).build());

        assertThat(layout.fingerprint()).isEqualTo("time=DAY:ts;cluster=a,b");
        assertThat(TableLayout.of(null, RANGE, null).fingerprint()).isEqualTo("range=n:0:100:10");
        // Clustering order is significant to BigQuery, so it is to the fingerprint too.
        assertThat(
                        TableLayout.of(
                                        null,
                                        null,
                                        Clustering.newBuilder()
                                                .setFields(List.of("b", "a"))
                                                .build())
                                .fingerprint())
                .isEqualTo("cluster=b,a");
    }

    @Test
    void aCopyRequiresLaidOutSourcesForColumnOrRangePartitioningOrClustering() {
        TimePartitioning column =
                TimePartitioning.newBuilder(TimePartitioning.Type.YEAR).setField("ts").build();
        Clustering clustering = Clustering.newBuilder().setFields(List.of("n")).build();

        assertThat(TableLayout.of(column, null, null).requiresLaidOutSources()).isTrue();
        assertThat(TableLayout.of(null, RANGE, null).requiresLaidOutSources()).isTrue();
        assertThat(TableLayout.of(null, null, clustering).requiresLaidOutSources()).isTrue();
        assertThat(
                        TableLayout.of(
                                        TimePartitioning.of(TimePartitioning.Type.DAY),
                                        null,
                                        clustering)
                                .requiresLaidOutSources())
                .isTrue();
        // Ingestion-time partitioning alone accepts an unpartitioned source (measured).
        assertThat(
                        TableLayout.of(TimePartitioning.of(TimePartitioning.Type.DAY), null, null)
                                .requiresLaidOutSources())
                .isFalse();
        assertThat(TableLayout.NONE.requiresLaidOutSources()).isFalse();
    }

    @Test
    void layoutsReadFromEqualTablesAreEqual() {
        StandardTableDefinition definition =
                StandardTableDefinition.newBuilder()
                        .setSchema(SCHEMA)
                        .setRangePartitioning(RANGE)
                        .setClustering(Clustering.newBuilder().setFields(List.of("ts")).build())
                        .build();

        assertThat(TableLayout.of(definition))
                .isEqualTo(TableLayout.of(definition.toBuilder().build()))
                .hasSameHashCodeAs(TableLayout.of(definition.toBuilder().build()))
                .isNotEqualTo(TableLayout.of(null, RANGE, null));
    }

    @Test
    void aSnapshotWithoutATableHasNoLayout() {
        TableSchema schema = TableSchema.getDefaultInstance();

        assertThat(TableSchemaSnapshot.of(schema, null).getLayout()).isSameAs(TableLayout.NONE);
        assertThat(
                        TableSchemaSnapshot.of(schema, null, TableLayout.of(null, RANGE, null))
                                .getLayout())
                .isEqualTo(TableLayout.of(null, RANGE, null));
    }
}
