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

import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.source.BigQuerySource;
import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializer;
import io.github.flink.gcp.connector.docs.BigQueryDocumentationTypes.MyEvent;
import io.github.flink.gcp.connector.docs.BigQueryDocumentationTypes.MyEventProtoSerializer;
import org.apache.avro.generic.GenericRecord;

final class BigQueryConnectorCredentials {

    private BigQueryConnectorCredentials() {}

    static void buildSink() {
        // tag::bigquery-connector-credentials-sink[]
        BigQuerySink.<MyEvent>builder()
                .destination(TableDestination.of("my-project", "my_dataset", "events"))
                .serializer(new MyEventProtoSerializer())
                .serviceAccountKeyFile("/var/run/secrets/bigquery/key.json")
                .build();
        // end::bigquery-connector-credentials-sink[]
    }

    static void buildSource(String schemaJson) {
        // tag::bigquery-connector-credentials-source[]
        BigQuerySource.<GenericRecord>builder()
                .table(TableDestination.of("my-project", "my_dataset", "events"))
                .deserializer(BigQueryRowDeserializer.genericRecord(schemaJson))
                .serviceAccountKeyFile("/var/run/secrets/bigquery/key.json")
                .build();
        // end::bigquery-connector-credentials-source[]
    }
}
