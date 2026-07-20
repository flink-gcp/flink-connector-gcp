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

package io.github.flink.gcp.connector.bigquery.sink.tables;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
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

/** Tests for {@link BigQueryTableAdmin}. */
class BigQueryTableAdminTest {

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
                BigQueryTableAdmin.buildTableInfo(
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

        TableInfo tableInfo = BigQueryTableAdmin.buildTableInfo(DESTINATION, SCHEMA, options);

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

        TableInfo tableInfo = BigQueryTableAdmin.buildTableInfo(DESTINATION, SCHEMA, options);

        StandardTableDefinition definition = tableInfo.getDefinition();
        assertThat(definition.getTimePartitioning().getType())
                .isEqualTo(TimePartitioning.Type.MONTH);
        assertThat(definition.getTimePartitioning().getField()).isNull();
    }

    @Test
    void mergeSchemaPreservesRestOnlyAttributesOfExistingFields() {
        com.google.cloud.bigquery.Schema existing =
                com.google.cloud.bigquery.Schema.of(
                        com.google.cloud.bigquery.Field.newBuilder(
                                        "name",
                                        com.google.cloud.bigquery.StandardSQLTypeName.STRING)
                                .setMode(com.google.cloud.bigquery.Field.Mode.REQUIRED)
                                .setDescription("the name")
                                .setPolicyTags(
                                        com.google.cloud.bigquery.PolicyTags.newBuilder()
                                                .setNames(
                                                        java.util.List.of(
                                                                "projects/p/locations/l/taxonomies"
                                                                        + "/t/policyTags/pii"))
                                                .build())
                                .build());
        TableSchema proposed =
                TableSchema.newBuilder()
                        .addFields(
                                TableFieldSchema.newBuilder()
                                        .setName("name")
                                        .setType(TableFieldSchema.Type.STRING)
                                        .setMode(TableFieldSchema.Mode.NULLABLE))
                        .addFields(
                                TableFieldSchema.newBuilder()
                                        .setName("email")
                                        .setType(TableFieldSchema.Type.STRING)
                                        .setMode(TableFieldSchema.Mode.NULLABLE))
                        .build();

        com.google.cloud.bigquery.Schema merged =
                BigQueryTableAdmin.mergeSchema(existing, proposed);

        com.google.cloud.bigquery.Field name = merged.getFields().get(0);
        // Relaxation applied, everything the Storage form cannot express preserved.
        assertThat(name.getMode()).isEqualTo(com.google.cloud.bigquery.Field.Mode.NULLABLE);
        assertThat(name.getDescription()).isEqualTo("the name");
        assertThat(name.getPolicyTags().getNames()).isNotEmpty();
        assertThat(merged.getFields().get(1).getName()).isEqualTo("email");
        assertThat(merged.getFields()).hasSize(2);
    }

    @Test
    void mergeSchemaRecursesIntoStructs() {
        com.google.cloud.bigquery.Schema existing =
                com.google.cloud.bigquery.Schema.of(
                        com.google.cloud.bigquery.Field.newBuilder(
                                        "address",
                                        com.google.cloud.bigquery.StandardSQLTypeName.STRUCT,
                                        com.google.cloud.bigquery.FieldList.of(
                                                com.google.cloud.bigquery.Field.newBuilder(
                                                                "city",
                                                                com.google.cloud.bigquery
                                                                        .StandardSQLTypeName.STRING)
                                                        .setDescription("the city")
                                                        .build()))
                                .setMode(com.google.cloud.bigquery.Field.Mode.NULLABLE)
                                .build());
        TableSchema proposed =
                TableSchema.newBuilder()
                        .addFields(
                                TableFieldSchema.newBuilder()
                                        .setName("address")
                                        .setType(TableFieldSchema.Type.STRUCT)
                                        .setMode(TableFieldSchema.Mode.NULLABLE)
                                        .addFields(
                                                TableFieldSchema.newBuilder()
                                                        .setName("city")
                                                        .setType(TableFieldSchema.Type.STRING)
                                                        .setMode(TableFieldSchema.Mode.NULLABLE))
                                        .addFields(
                                                TableFieldSchema.newBuilder()
                                                        .setName("zip")
                                                        .setType(TableFieldSchema.Type.STRING)
                                                        .setMode(TableFieldSchema.Mode.NULLABLE)))
                        .build();

        com.google.cloud.bigquery.Schema merged =
                BigQueryTableAdmin.mergeSchema(existing, proposed);

        com.google.cloud.bigquery.FieldList subFields = merged.getFields().get(0).getSubFields();
        assertThat(subFields).hasSize(2);
        assertThat(subFields.get(0).getDescription()).isEqualTo("the city");
        assertThat(subFields.get(1).getName()).isEqualTo("zip");
    }

    @Test
    void lostRacesAreRecognizedByHttpCode() {
        assertThat(BigQueryTableAdmin.isLostRace(new BigQueryException(409, "conflict"))).isTrue();
        assertThat(BigQueryTableAdmin.isLostRace(new BigQueryException(412, "precondition failed")))
                .isTrue();
        assertThat(BigQueryTableAdmin.isLostRace(new BigQueryException(403, "forbidden")))
                .isFalse();
        assertThat(BigQueryTableAdmin.isLostRace(new BigQueryException(400, "bad request")))
                .isFalse();
    }

    @Test
    void lostRacesAreRecognizedByErrorReason() {
        assertThat(
                        BigQueryTableAdmin.isLostRace(
                                new BigQueryException(
                                        400,
                                        "etag mismatch",
                                        new BigQueryError("conditionNotMet", null, "etag"))))
                .isTrue();
        assertThat(
                        BigQueryTableAdmin.isLostRace(
                                new BigQueryException(
                                        403,
                                        "quota",
                                        new BigQueryError("rateLimitExceeded", null, "quota"))))
                .isTrue();
        assertThat(
                        BigQueryTableAdmin.isLostRace(
                                new BigQueryException(
                                        403,
                                        "denied",
                                        new BigQueryError("accessDenied", null, "denied"))))
                .isFalse();
    }
}
