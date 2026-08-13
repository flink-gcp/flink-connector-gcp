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

package io.github.flink.gcp.connector.bigquery.source.serializer;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;

import io.github.flink.gcp.connector.bigquery.source.TestRows;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BigQueryRowDeserializerTest {

    @Test
    void handsTheRowOnUnchanged() throws Exception {
        BigQueryRowDeserializer<GenericRecord> deserializer =
                BigQueryRowDeserializer.genericRecord(TestRows.SCHEMA_JSON);
        GenericRecord row = TestRows.rows(1).get(0);
        List<GenericRecord> records = new ArrayList<>();

        deserializer.deserialize(
                row,
                new Collector<GenericRecord>() {
                    @Override
                    public void collect(GenericRecord record) {
                        records.add(record);
                    }

                    @Override
                    public void close() {}
                });

        assertThat(records).singleElement().isSameAs(row);
    }

    @Test
    void answersWithTheSchemaItWasGiven() {
        BigQueryRowDeserializer<GenericRecord> deserializer =
                BigQueryRowDeserializer.genericRecord(TestRows.SCHEMA);

        assertThat(deserializer.getReaderSchema()).isEqualTo(TestRows.SCHEMA);
    }

    @Test
    void producesATypeThatCanActuallySerializeAGenericRecord() throws Exception {
        // The reason this implementation ships at all: TypeInformation.of(GenericRecord.class) is a
        // generic type backed by Kryo, and Kryo cannot serialize a GenericData.Record — it fails on
        // the record's own schema (measured 2026-08-09).
        BigQueryRowDeserializer<GenericRecord> deserializer =
                BigQueryRowDeserializer.genericRecord(TestRows.SCHEMA_JSON);
        TypeSerializer<GenericRecord> serializer =
                deserializer.getProducedType().createSerializer(new SerializerConfigImpl());
        GenericRecord row = new GenericData.Record(TestRows.SCHEMA);
        row.put("id", 7L);
        row.put("name", "seven");

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        serializer.serialize(row, new DataOutputViewStreamWrapper(bytes));
        GenericRecord restored =
                serializer.deserialize(
                        new DataInputViewStreamWrapper(
                                new ByteArrayInputStream(bytes.toByteArray())));

        assertThat(restored.get("id")).isEqualTo(7L);
        assertThat(String.valueOf(restored.get("name"))).isEqualTo("seven");
    }

    @Test
    void rejectsAMalformedSchemaWhereTheJobIsBuilt() {
        // A schema problem must never surface from deserialize(): by then the job is running and
        // the
        // failure is a per-record one on a TaskManager.
        assertThatThrownBy(() -> BigQueryRowDeserializer.genericRecord("{\"type\":"))
                .isInstanceOf(SchemaParseException.class);
    }

    @Test
    void survivesTheJobGraphAsSerializedState() throws Exception {
        // Avro's Schema serialization replacement must preserve both the reader schema and the
        // TypeInformation derived from it when Flink ships the job graph.
        BigQueryRowDeserializer<GenericRecord> deserializer =
                BigQueryRowDeserializer.genericRecord(TestRows.SCHEMA_JSON);

        BigQueryRowDeserializer<GenericRecord> restored =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(deserializer),
                        getClass().getClassLoader());

        Schema schema = restored.getReaderSchema();
        assertThat(schema).isEqualTo(TestRows.SCHEMA);
        assertThat(restored.getProducedType()).isEqualTo(deserializer.getProducedType());
    }
}
