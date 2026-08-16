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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import io.github.flink.gcp.connector.bigquery.sink.serializer.proto.ProtoMessageSerializer;
import io.github.flink.gcp.connector.bigquery.sink.serializer.proto.ProtoSchemaOptions;
import io.github.flink.gcp.connector.bigquery.testproto.WellKnownTypes;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/**
 * Integration test for the protobuf well-known-type <em>schema</em> derivation against the BigQuery
 * emulator (goccy/bigquery-emulator): the full {@code WellKnownTypes} matrix is a <b>creatable</b>
 * table — a scalar column where a {@code STRUCT} used to be, {@code JSON} for {@code Struct}/{@code
 * Value}/{@code ListValue} — which unit tests, seeing only the derived schema, cannot show. Kept on
 * the emulator for its per-PR feedback.
 *
 * <p>This class used to carry a write half over a singular-only fixture, because emulator 0.8.1
 * rejects <em>every</em> insert into a table carrying an {@code ARRAY<JSON>} column, populated or
 * empty alike. The #16 real-GCP suite retired that workaround: writes of the full fixture —
 * repeated JSON columns, wrapper NULL-versus-default, timestamp precision — run against the service
 * in {@link BigQuerySerializerFidelityITCase}, and the {@code SingularWellKnownTypes} fixture
 * message was deleted with it.
 */
class BigQueryProtoWellKnownTypesITCase extends AbstractBigQueryEmulatorITCase {

    private static final String TABLE = "proto_wellknown";

    /** The full matrix, including the repeated and map columns, is a creatable table. */
    @Test
    void derivesACreatableTableForEveryWellKnownType() {
        createTable(
                TABLE,
                ProtoMessageSerializer.of(WellKnownTypes.class, ProtoSchemaOptions.defaults())
                        .getTableSchema(null));

        Schema created = schemaOf(TABLE);
        assertThat(created.getFields())
                .extracting(Field::getName, Field::getType, Field::getMode)
                .contains(
                        tuple("w_int32", LegacySQLTypeName.INTEGER, Field.Mode.NULLABLE),
                        tuple("w_double", LegacySQLTypeName.FLOAT, Field.Mode.NULLABLE),
                        tuple("w_bool", LegacySQLTypeName.BOOLEAN, Field.Mode.NULLABLE),
                        tuple("w_string", LegacySQLTypeName.STRING, Field.Mode.NULLABLE),
                        tuple("w_bytes", LegacySQLTypeName.BYTES, Field.Mode.NULLABLE),
                        tuple("w_duration", LegacySQLTypeName.INTEGER, Field.Mode.NULLABLE),
                        tuple("w_mask", LegacySQLTypeName.STRING, Field.Mode.NULLABLE),
                        tuple("w_struct", LegacySQLTypeName.JSON, Field.Mode.NULLABLE),
                        tuple("w_value", LegacySQLTypeName.JSON, Field.Mode.NULLABLE),
                        tuple("w_list", LegacySQLTypeName.JSON, Field.Mode.NULLABLE),
                        tuple("w_rep_int64", LegacySQLTypeName.INTEGER, Field.Mode.REPEATED),
                        tuple("w_rep_struct", LegacySQLTypeName.JSON, Field.Mode.REPEATED),
                        // Any keeps its two columns: the payload cannot be expanded without the
                        // descriptor its type URL names.
                        tuple("w_any", LegacySQLTypeName.RECORD, Field.Mode.NULLABLE));
        assertThat(created.getFields().get("w_map_struct").getSubFields())
                .extracting(Field::getName, Field::getType)
                .contains(tuple("value", LegacySQLTypeName.JSON));
    }

    private static Schema schemaOf(String table) {
        Schema schema =
                restClient
                        .getTable(TableId.of(PROJECT, DATASET, table))
                        .getDefinition()
                        .getSchema();
        assertThat(schema).isNotNull();
        return schema;
    }
}
