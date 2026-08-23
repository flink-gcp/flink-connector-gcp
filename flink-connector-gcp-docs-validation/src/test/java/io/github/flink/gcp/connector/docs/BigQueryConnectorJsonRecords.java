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

import com.google.cloud.bigquery.Schema;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.json.JsonDocumentSerializer;
import io.github.flink.gcp.connector.bigquery.sink.serializer.json.JsonDocumentSerializerOptions;

final class BigQueryConnectorJsonRecords {

    private BigQueryConnectorJsonRecords() {}

    static void buildSink(Schema schema) {
        // tag::bigquery-connector-json-records-sink[]
        Sink<String> sink =
                BigQuerySink.<String>builder()
                        .table(TableDestination.of("my-project", "my_dataset", "events"))
                        .serializer(JsonDocumentSerializer.of(schema))
                        .build();
        // end::bigquery-connector-json-records-sink[]
    }

    static void buildIgnoringUnknownFields(Schema schema) {
        // tag::bigquery-connector-json-records-ignore-unknown-fields[]
        JsonDocumentSerializer.of(
                schema, JsonDocumentSerializerOptions.builder().ignoreUnknownFields().build());
        // end::bigquery-connector-json-records-ignore-unknown-fields[]
    }
}
