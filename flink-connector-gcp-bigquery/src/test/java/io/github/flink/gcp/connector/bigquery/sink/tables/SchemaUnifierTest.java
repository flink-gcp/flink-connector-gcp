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

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import io.github.flink.gcp.connector.bigquery.sink.SchemaUpdateOptions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SchemaUnifier}. */
class SchemaUnifierTest {

    private static final SchemaUpdateOptions ALL =
            SchemaUpdateOptions.builder().allowNewFields().allowFieldRelaxation().build();
    private static final SchemaUpdateOptions NEW_FIELDS_ONLY =
            SchemaUpdateOptions.builder().allowNewFields().build();
    private static final SchemaUpdateOptions RELAXATION_ONLY =
            SchemaUpdateOptions.builder().allowFieldRelaxation().build();

    private static TableFieldSchema field(
            String name, TableFieldSchema.Type type, TableFieldSchema.Mode mode) {
        return TableFieldSchema.newBuilder().setName(name).setType(type).setMode(mode).build();
    }

    private static TableFieldSchema struct(
            String name, TableFieldSchema.Mode mode, TableFieldSchema... subFields) {
        return TableFieldSchema.newBuilder()
                .setName(name)
                .setType(TableFieldSchema.Type.STRUCT)
                .setMode(mode)
                .addAllFields(Arrays.asList(subFields))
                .build();
    }

    private static TableSchema schema(TableFieldSchema... fields) {
        return TableSchema.newBuilder().addAllFields(Arrays.asList(fields)).build();
    }

    private static List<String> names(TableSchema schema) {
        return schema.getFieldsList().stream()
                .map(TableFieldSchema::getName)
                .collect(Collectors.toList());
    }

    private static final TableFieldSchema NAME =
            field("name", TableFieldSchema.Type.STRING, TableFieldSchema.Mode.NULLABLE);
    private static final TableFieldSchema EMAIL =
            field("email", TableFieldSchema.Type.STRING, TableFieldSchema.Mode.NULLABLE);

    @Test
    void identicalSchemasAreUnchanged() throws Exception {
        SchemaUnifier.UnionResult result =
                SchemaUnifier.union(schema(NAME, EMAIL), schema(NAME, EMAIL), ALL);

        assertThat(result.isChanged()).isFalse();
        assertThat(result.getSchema()).isEqualTo(schema(NAME, EMAIL));
    }

    @Test
    void newFieldsAreAppendedAtTheEnd() throws Exception {
        SchemaUnifier.UnionResult result =
                SchemaUnifier.union(schema(NAME), schema(EMAIL, NAME), NEW_FIELDS_ONLY);

        assertThat(result.isChanged()).isTrue();
        assertThat(names(result.getSchema())).containsExactly("name", "email");
    }

    @Test
    void newRequiredFieldsAreForcedNullable() throws Exception {
        TableFieldSchema required =
                field("email", TableFieldSchema.Type.STRING, TableFieldSchema.Mode.REQUIRED);

        SchemaUnifier.UnionResult result =
                SchemaUnifier.union(schema(NAME), schema(NAME, required), NEW_FIELDS_ONLY);

        assertThat(result.getSchema().getFields(1).getMode())
                .isEqualTo(TableFieldSchema.Mode.NULLABLE);
    }

    @Test
    void newRepeatedFieldsKeepTheirMode() throws Exception {
        TableFieldSchema repeated =
                field("tags", TableFieldSchema.Type.STRING, TableFieldSchema.Mode.REPEATED);

        SchemaUnifier.UnionResult result =
                SchemaUnifier.union(schema(NAME), schema(NAME, repeated), NEW_FIELDS_ONLY);

        assertThat(result.getSchema().getFields(1).getMode())
                .isEqualTo(TableFieldSchema.Mode.REPEATED);
    }

    @Test
    void newFieldsAreRejectedWhenNotAllowed() {
        assertThatThrownBy(
                        () ->
                                SchemaUnifier.union(
                                        schema(NAME), schema(NAME, EMAIL), RELAXATION_ONLY))
                .isInstanceOf(SchemaUnifier.SchemaUnionException.class)
                .hasMessageContaining("email")
                .hasMessageContaining("allowNewFields");
    }

    @Test
    void missingFieldsAreNeverDropped() throws Exception {
        SchemaUnifier.UnionResult result =
                SchemaUnifier.union(schema(NAME, EMAIL), schema(NAME), ALL);

        assertThat(result.isChanged()).isFalse();
        assertThat(names(result.getSchema())).containsExactly("name", "email");
    }

    @Test
    void requiredFieldsAreRelaxedOnlyWhenAllowed() throws Exception {
        TableFieldSchema required =
                field("name", TableFieldSchema.Type.STRING, TableFieldSchema.Mode.REQUIRED);

        SchemaUnifier.UnionResult relaxed =
                SchemaUnifier.union(schema(required), schema(NAME), RELAXATION_ONLY);
        assertThat(relaxed.isChanged()).isTrue();
        assertThat(relaxed.getSchema().getFields(0).getMode())
                .isEqualTo(TableFieldSchema.Mode.NULLABLE);

        SchemaUnifier.UnionResult kept =
                SchemaUnifier.union(schema(required), schema(NAME), NEW_FIELDS_ONLY);
        assertThat(kept.isChanged()).isFalse();
        assertThat(kept.getSchema().getFields(0).getMode())
                .isEqualTo(TableFieldSchema.Mode.REQUIRED);
    }

    @Test
    void unspecifiedDesiredModeCountsAsNullableForRelaxation() throws Exception {
        TableFieldSchema required =
                field("name", TableFieldSchema.Type.STRING, TableFieldSchema.Mode.REQUIRED);
        TableFieldSchema modeless =
                TableFieldSchema.newBuilder()
                        .setName("name")
                        .setType(TableFieldSchema.Type.STRING)
                        .build();

        SchemaUnifier.UnionResult result =
                SchemaUnifier.union(schema(required), schema(modeless), ALL);

        // Anything not explicitly REQUIRED counts as nullable, matching the converters'
        // mode defaulting.
        assertThat(result.isChanged()).isTrue();
        assertThat(result.getSchema().getFields(0).getMode())
                .isEqualTo(TableFieldSchema.Mode.NULLABLE);
    }

    @Test
    void modesAreNeverTightened() throws Exception {
        TableFieldSchema required =
                field("name", TableFieldSchema.Type.STRING, TableFieldSchema.Mode.REQUIRED);

        SchemaUnifier.UnionResult result = SchemaUnifier.union(schema(NAME), schema(required), ALL);

        assertThat(result.isChanged()).isFalse();
        assertThat(result.getSchema().getFields(0).getMode())
                .isEqualTo(TableFieldSchema.Mode.NULLABLE);
    }

    @Test
    void typeChangesAreRejected() {
        TableFieldSchema asInt =
                field("name", TableFieldSchema.Type.INT64, TableFieldSchema.Mode.NULLABLE);

        assertThatThrownBy(() -> SchemaUnifier.union(schema(NAME), schema(asInt), ALL))
                .isInstanceOf(SchemaUnifier.SchemaUnionException.class)
                .hasMessageContaining("name")
                .hasMessageContaining("STRING")
                .hasMessageContaining("INT64");
    }

    @Test
    void repeatedModeChangesAreRejected() {
        TableFieldSchema repeated =
                field("name", TableFieldSchema.Type.STRING, TableFieldSchema.Mode.REPEATED);

        assertThatThrownBy(() -> SchemaUnifier.union(schema(NAME), schema(repeated), ALL))
                .isInstanceOf(SchemaUnifier.SchemaUnionException.class)
                .hasMessageContaining("REPEATED");
        assertThatThrownBy(() -> SchemaUnifier.union(schema(repeated), schema(NAME), ALL))
                .isInstanceOf(SchemaUnifier.SchemaUnionException.class)
                .hasMessageContaining("REPEATED");
    }

    @Test
    void fieldNamesMatchCaseInsensitivelyKeepingTheExistingSpelling() throws Exception {
        TableFieldSchema upper =
                field("NAME", TableFieldSchema.Type.STRING, TableFieldSchema.Mode.NULLABLE);

        SchemaUnifier.UnionResult result = SchemaUnifier.union(schema(NAME), schema(upper), ALL);

        assertThat(result.isChanged()).isFalse();
        assertThat(names(result.getSchema())).containsExactly("name");
    }

    @Test
    void structsRecurse() throws Exception {
        TableFieldSchema existingStruct =
                struct(
                        "address",
                        TableFieldSchema.Mode.NULLABLE,
                        field(
                                "city",
                                TableFieldSchema.Type.STRING,
                                TableFieldSchema.Mode.REQUIRED));
        TableFieldSchema desiredStruct =
                struct(
                        "address",
                        TableFieldSchema.Mode.NULLABLE,
                        field("city", TableFieldSchema.Type.STRING, TableFieldSchema.Mode.NULLABLE),
                        field("zip", TableFieldSchema.Type.STRING, TableFieldSchema.Mode.REQUIRED));

        SchemaUnifier.UnionResult result =
                SchemaUnifier.union(schema(NAME, existingStruct), schema(NAME, desiredStruct), ALL);

        assertThat(result.isChanged()).isTrue();
        TableFieldSchema unifiedStruct = result.getSchema().getFields(1);
        assertThat(unifiedStruct.getFields(0).getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
        assertThat(unifiedStruct.getFields(1).getName()).isEqualTo("zip");
        assertThat(unifiedStruct.getFields(1).getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE);
    }

    @Test
    void structTypeChangesAreRejectedWithFieldPath() {
        TableFieldSchema existingStruct =
                struct(
                        "address",
                        TableFieldSchema.Mode.NULLABLE,
                        field("zip", TableFieldSchema.Type.STRING, TableFieldSchema.Mode.NULLABLE));
        TableFieldSchema desiredStruct =
                struct(
                        "address",
                        TableFieldSchema.Mode.NULLABLE,
                        field("zip", TableFieldSchema.Type.INT64, TableFieldSchema.Mode.NULLABLE));

        assertThatThrownBy(
                        () ->
                                SchemaUnifier.union(
                                        schema(existingStruct), schema(desiredStruct), ALL))
                .isInstanceOf(SchemaUnifier.SchemaUnionException.class)
                .hasMessageContaining("address.zip");
    }

    @Test
    void newStructFieldsAreGatedByAllowNewFields() {
        TableFieldSchema existingStruct =
                struct(
                        "address",
                        TableFieldSchema.Mode.NULLABLE,
                        field(
                                "city",
                                TableFieldSchema.Type.STRING,
                                TableFieldSchema.Mode.NULLABLE));
        TableFieldSchema desiredStruct =
                struct(
                        "address",
                        TableFieldSchema.Mode.NULLABLE,
                        field("city", TableFieldSchema.Type.STRING, TableFieldSchema.Mode.NULLABLE),
                        field("zip", TableFieldSchema.Type.STRING, TableFieldSchema.Mode.NULLABLE));

        assertThatThrownBy(
                        () ->
                                SchemaUnifier.union(
                                        schema(existingStruct),
                                        schema(desiredStruct),
                                        RELAXATION_ONLY))
                .isInstanceOf(SchemaUnifier.SchemaUnionException.class)
                .hasMessageContaining("address.zip");
    }

    @Test
    void unionOfAUnionIsIdempotent() throws Exception {
        TableFieldSchema required =
                field("name", TableFieldSchema.Type.STRING, TableFieldSchema.Mode.REQUIRED);
        TableSchema desired = schema(NAME, EMAIL);

        SchemaUnifier.UnionResult first = SchemaUnifier.union(schema(required), desired, ALL);
        assertThat(first.isChanged()).isTrue();

        SchemaUnifier.UnionResult second = SchemaUnifier.union(first.getSchema(), desired, ALL);
        assertThat(second.isChanged()).isFalse();
        assertThat(second.getSchema()).isEqualTo(first.getSchema());
    }
}
