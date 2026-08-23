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

package io.github.flink.gcp.connector.docs;

import org.apache.flink.api.connector.sink2.Sink;

import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.DestinationResolver;
import io.github.flink.gcp.connector.bigquery.sink.serializer.avro.AvroRecordSerializationSchema;
import io.github.flink.gcp.connector.bigquery.sink.serializer.avro.AvroSchemaOptions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

final class BigQueryConnectorAvroRecords {

    private BigQueryConnectorAvroRecords() {}

    static void buildSink(Schema schema, DestinationResolver<GenericRecord> myDestinationResolver) {
        // tag::bigquery-connector-avro-records-sink[]
        Sink<GenericRecord> sink =
                BigQuerySink.<GenericRecord>builder()
                        .destinationResolver(myDestinationResolver)
                        .serializer(AvroRecordSerializationSchema.of(schema))
                        .build();
        // end::bigquery-connector-avro-records-sink[]
    }

    static void buildRequiredColumns(Schema schema) {
        // tag::bigquery-connector-avro-records-required-columns[]
        AvroRecordSerializationSchema.of(
                schema, AvroSchemaOptions.builder().deriveRequiredColumns().build());
        // end::bigquery-connector-avro-records-required-columns[]
    }
}
