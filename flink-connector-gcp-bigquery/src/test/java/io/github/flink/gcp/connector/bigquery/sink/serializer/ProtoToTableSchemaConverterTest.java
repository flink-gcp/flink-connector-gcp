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

package io.github.flink.gcp.connector.bigquery.sink.serializer;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ProtoToTableSchemaConverter}. */
class ProtoToTableSchemaConverterTest {

    @Test
    void mapsTheFullTypeMatrix() {
        TableSchema schema =
                ProtoToTableSchemaConverter.convert(
                        TestProtos.allTypes(), ProtoSchemaOptions.defaults());
        Map<String, TableFieldSchema> fields = byName(schema);

        assertThat(fields.get("f_int32").getType()).isEqualTo(TableFieldSchema.Type.INT64);
        assertThat(fields.get("f_int64").getType()).isEqualTo(TableFieldSchema.Type.INT64);
        assertThat(fields.get("f_uint32").getType()).isEqualTo(TableFieldSchema.Type.INT64);
        assertThat(fields.get("f_uint64").getType()).isEqualTo(TableFieldSchema.Type.INT64);
        assertThat(fields.get("f_float").getType()).isEqualTo(TableFieldSchema.Type.DOUBLE);
        assertThat(fields.get("f_double").getType()).isEqualTo(TableFieldSchema.Type.DOUBLE);
        assertThat(fields.get("f_bool").getType()).isEqualTo(TableFieldSchema.Type.BOOL);
        assertThat(fields.get("f_string").getType()).isEqualTo(TableFieldSchema.Type.STRING);
        assertThat(fields.get("f_bytes").getType()).isEqualTo(TableFieldSchema.Type.BYTES);
        assertThat(fields.get("f_enum").getType()).isEqualTo(TableFieldSchema.Type.STRING);
        assertThat(fields.get("f_ts").getType()).isEqualTo(TableFieldSchema.Type.TIMESTAMP);

        assertThat(fields.values())
                .filteredOn(f -> f.getMode() != TableFieldSchema.Mode.REPEATED)
                .allSatisfy(f -> assertThat(f.getMode()).isEqualTo(TableFieldSchema.Mode.NULLABLE));
        assertThat(fields.get("f_rep_ts").getMode()).isEqualTo(TableFieldSchema.Mode.REPEATED);
        assertThat(fields.get("f_rep_ts").getType()).isEqualTo(TableFieldSchema.Type.TIMESTAMP);
    }

    @Test
    void mapsNestedMessagesToStructs() {
        TableSchema schema =
                ProtoToTableSchemaConverter.convert(
                        TestProtos.allTypes(), ProtoSchemaOptions.defaults());
        TableFieldSchema nested = byName(schema).get("f_nested");

        assertThat(nested.getType()).isEqualTo(TableFieldSchema.Type.STRUCT);
        assertThat(nested.getFieldsList())
                .extracting(TableFieldSchema::getName, TableFieldSchema::getType)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("s", TableFieldSchema.Type.STRING),
                        org.assertj.core.groups.Tuple.tuple("n", TableFieldSchema.Type.INT64));
    }

    @Test
    void mapsRepeatedAndMapFields() {
        TableSchema schema =
                ProtoToTableSchemaConverter.convert(
                        TestProtos.allTypes(), ProtoSchemaOptions.defaults());
        Map<String, TableFieldSchema> fields = byName(schema);

        TableFieldSchema repeated = fields.get("f_rep_string");
        assertThat(repeated.getMode()).isEqualTo(TableFieldSchema.Mode.REPEATED);
        assertThat(repeated.getType()).isEqualTo(TableFieldSchema.Type.STRING);

        TableFieldSchema map = fields.get("f_map");
        assertThat(map.getMode()).isEqualTo(TableFieldSchema.Mode.REPEATED);
        assertThat(map.getType()).isEqualTo(TableFieldSchema.Type.STRUCT);
        assertThat(map.getFieldsList())
                .extracting(TableFieldSchema::getName, TableFieldSchema::getType)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("key", TableFieldSchema.Type.STRING),
                        org.assertj.core.groups.Tuple.tuple("value", TableFieldSchema.Type.INT64));
    }

    @Test
    void mapsConfiguredMessageFieldsToJson() {
        ProtoSchemaOptions options = ProtoSchemaOptions.builder().jsonFieldPath("f_json").build();
        TableSchema schema = ProtoToTableSchemaConverter.convert(TestProtos.allTypes(), options);

        TableFieldSchema json = byName(schema).get("f_json");
        assertThat(json.getType()).isEqualTo(TableFieldSchema.Type.JSON);
        assertThat(json.getFieldsList()).isEmpty();
    }

    @Test
    void rejectsJsonMappingOnNonMessageFields() {
        ProtoSchemaOptions options = ProtoSchemaOptions.builder().jsonFieldPath("f_string").build();

        assertThatThrownBy(
                        () -> ProtoToTableSchemaConverter.convert(TestProtos.allTypes(), options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("f_string");
    }

    @Test
    void rejectsCaseInsensitiveDuplicateFieldNames() {
        assertThatThrownBy(
                        () ->
                                ProtoToTableSchemaConverter.convert(
                                        TestProtos.caseCollision(), ProtoSchemaOptions.defaults()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("case");
    }

    @Test
    void rejectsJsonPathsMatchingNoField() {
        ProtoSchemaOptions options = ProtoSchemaOptions.builder().jsonFieldPath("f_jsonn").build();

        assertThatThrownBy(
                        () -> ProtoToTableSchemaConverter.convert(TestProtos.allTypes(), options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("f_jsonn");
    }

    @Test
    void rejectsRecursiveMessages() {
        assertThatThrownBy(
                        () ->
                                ProtoToTableSchemaConverter.convert(
                                        TestProtos.recursive(), ProtoSchemaOptions.defaults()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Recursive");
    }

    private static Map<String, TableFieldSchema> byName(TableSchema schema) {
        return schema.getFieldsList().stream()
                .collect(Collectors.toMap(TableFieldSchema::getName, Function.identity()));
    }
}
