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

package io.github.flink.gcp.connector.bigquery.source.serializer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * Hands decoded rows on unchanged.
 *
 * <p>The Avro {@link Schema} is serializable and is held directly. A JSON schema is parsed in the
 * constructor, so a malformed schema fails where the job is built rather than on a TaskManager once
 * rows flow.
 *
 * <p>{@link #getProducedType()} answers with {@link GenericRecordAvroTypeInfo}, which selects
 * Flink's Avro serializer. The alternative, {@code TypeInformation.of(GenericRecord.class)}, is a
 * generic type backed by Kryo, and Kryo cannot serialize a {@code GenericData.Record} at all: it
 * fails on the record's schema (measured 2026-08-09 against Flink 2.2 and Avro 1.12.1). That is why
 * this class exists rather than a documented one-liner for users to write.
 */
@Internal
final class GenericRecordDeserializer implements BigQueryRowDeserializer<GenericRecord> {

    private static final long serialVersionUID = 1L;

    private final Schema schema;

    GenericRecordDeserializer(Schema readerSchema) {
        this.schema = Preconditions.checkNotNull(readerSchema, "readerSchema must not be null");
    }

    GenericRecordDeserializer(String readerSchemaJson) {
        this(
                new Schema.Parser()
                        .parse(
                                Preconditions.checkNotNull(
                                        readerSchemaJson, "readerSchemaJson must not be null")));
    }

    @Override
    public Schema getReaderSchema() {
        return schema;
    }

    @Override
    public void deserialize(GenericRecord row, Collector<GenericRecord> out) {
        out.collect(row);
    }

    @Override
    public TypeInformation<GenericRecord> getProducedType() {
        return new GenericRecordAvroTypeInfo(schema);
    }
}
