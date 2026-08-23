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

import org.apache.flink.api.connector.source.Source;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.source.BigQuerySource;
import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializationSchema;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

final class BigQueryExamplesReadingOneColumn {

    private BigQueryExamplesReadingOneColumn() {}

    static Source<GenericRecord, ?, ?> build() {
        // tag::bigquery-examples-reading-one-column[]
        Schema readerSchema =
                new Schema.Parser()
                        .parse(
                                "{\"type\":\"record\",\"name\":\"Event\",\"fields\":["
                                        + "{\"name\":\"user_id\",\"type\":\"long\"}]}");

        Source<GenericRecord, ?, ?> source =
                BigQuerySource.<GenericRecord>builder()
                        .table(TableDestination.of("my-project", "analytics", "events"))
                        .deserializer(BigQueryRowDeserializationSchema.genericRecord(readerSchema))
                        .selectedFields("user_id")
                        .rowRestriction("event_date = '2026-08-01' AND country = 'JP'")
                        .build();
        // end::bigquery-examples-reading-one-column[]
        return source;
    }
}
