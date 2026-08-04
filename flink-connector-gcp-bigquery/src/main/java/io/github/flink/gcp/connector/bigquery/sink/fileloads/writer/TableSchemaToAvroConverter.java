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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.writer;

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts a Storage API {@link TableSchema} (the schema form of the serializer SPI) into the Avro
 * schema staged files are written with.
 *
 * <p>The mapping is chosen so that a BigQuery load job with {@code useAvroLogicalTypes} enabled and
 * an explicit destination schema round-trips every value produced by {@link ProtoToAvroConverter}:
 *
 * <ul>
 *   <li>{@code TIMESTAMP} → {@code long} + {@code timestamp-micros}, {@code DATE} → {@code int} +
 *       {@code date}, {@code TIME} → {@code long} + {@code time-micros}, {@code DATETIME} → {@code
 *       long} + {@code local-timestamp-micros}
 *   <li>{@code NUMERIC}/{@code BIGNUMERIC} → {@code bytes} + {@code decimal} with the field's
 *       parameterized precision/scale when set, else the type maximum (38,9)/(77,38)
 *   <li>{@code JSON} and {@code GEOGRAPHY} → {@code string} (typed by the explicit load schema)
 *   <li>{@code STRUCT} → nested record, {@code REPEATED} → array of non-null items (BigQuery {@code
 *       REPEATED} fields cannot contain NULLs), {@code NULLABLE} → {@code union["null", T]}
 * </ul>
 *
 * <p>{@code INTERVAL} and {@code RANGE} fields are rejected: the Storage Write API serializer
 * surface has no canonical wire form for them yet, so the FILE_LOADS path does not accept them
 * either. BigQuery flexible column names (leading digits, dashes, non-ASCII) are rejected too —
 * Avro names cannot represent them, so they cannot travel through Avro staging files.
 */
@Internal
public final class TableSchemaToAvroConverter {

    /** Namespace prefix keeping nested record names unique per field path. */
    private static final String NAMESPACE = "io.github.flink.gcp.connector.bigquery.fileloads";

    /** The names Avro accepts; BigQuery flexible column names fall outside it. */
    private static final java.util.regex.Pattern AVRO_NAME =
            java.util.regex.Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private TableSchemaToAvroConverter() {}

    /**
     * Converts the given Storage API table schema to the Avro schema of the staged files.
     *
     * @param schema the Storage API schema
     * @return the Avro schema (a record named {@code Row})
     */
    public static Schema convert(TableSchema schema) {
        return toRecord(schema.getFieldsList(), "Row", NAMESPACE);
    }

    private static Schema toRecord(List<TableFieldSchema> fields, String name, String namespace) {
        List<Schema.Field> avroFields = new ArrayList<>(fields.size());
        for (TableFieldSchema field : fields) {
            avroFields.add(toField(field, namespace + "." + name));
        }
        return Schema.createRecord(name, null, namespace, false, avroFields);
    }

    private static Schema.Field toField(TableFieldSchema field, String namespace) {
        if (!AVRO_NAME.matcher(field.getName()).matches()) {
            throw new IllegalArgumentException(
                    "Field "
                            + field.getName()
                            + " is not representable as an Avro name; BigQuery flexible column"
                            + " names are not supported by WriteMethod.FILE_LOADS.");
        }
        Schema base = toBaseSchema(field, namespace);
        switch (field.getMode()) {
            case REPEATED:
                return new Schema.Field(field.getName(), Schema.createArray(base), null, null);
            case REQUIRED:
                return new Schema.Field(field.getName(), base, null, null);
            default:
                Schema nullable = Schema.createUnion(Schema.create(Schema.Type.NULL), base);
                return new Schema.Field(field.getName(), nullable, null, Schema.Field.NULL_VALUE);
        }
    }

    private static Schema toBaseSchema(TableFieldSchema field, String namespace) {
        switch (field.getType()) {
            case STRING:
            case JSON:
            case GEOGRAPHY:
                return Schema.create(Schema.Type.STRING);
            case BYTES:
                return Schema.create(Schema.Type.BYTES);
            case INT64:
                return Schema.create(Schema.Type.LONG);
            case DOUBLE:
                return Schema.create(Schema.Type.DOUBLE);
            case BOOL:
                return Schema.create(Schema.Type.BOOLEAN);
            case TIMESTAMP:
                return LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
            case DATE:
                return LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
            case TIME:
                return LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
            case DATETIME:
                return LogicalTypes.localTimestampMicros()
                        .addToSchema(Schema.create(Schema.Type.LONG));
            case NUMERIC:
                return decimal(field, 38, 9);
            case BIGNUMERIC:
                return decimal(field, 77, 38);
            case STRUCT:
                return toRecord(field.getFieldsList(), field.getName(), namespace);
            default:
                throw new IllegalArgumentException(
                        "Field "
                                + field.getName()
                                + " has type "
                                + field.getType()
                                + ", which WriteMethod.FILE_LOADS does not support.");
        }
    }

    private static Schema decimal(TableFieldSchema field, int maxPrecision, int defaultScale) {
        int precision = field.getPrecision() > 0 ? (int) field.getPrecision() : maxPrecision;
        int scale = field.getPrecision() > 0 ? (int) field.getScale() : defaultScale;
        return LogicalTypes.decimal(precision, scale).addToSchema(Schema.create(Schema.Type.BYTES));
    }
}
