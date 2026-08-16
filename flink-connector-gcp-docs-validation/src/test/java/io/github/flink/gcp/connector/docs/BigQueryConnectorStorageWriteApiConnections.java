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
import io.github.flink.gcp.connector.bigquery.sink.SchemaUpdateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.docs.BigQueryDocumentationTypes.MyEvent;
import io.github.flink.gcp.connector.docs.BigQueryDocumentationTypes.MyEventProtoSerializer;

final class BigQueryConnectorStorageWriteApiConnections {

    private BigQueryConnectorStorageWriteApiConnections() {}

    static void build() {
        // tag::bigquery-connector-storage-write-api-connections[]
        Sink<MyEvent> sink =
                BigQuerySink.<MyEvent>builder()
                        .destination(TableDestination.of("my-project", "my_dataset", "events"))
                        .serializer(new MyEventProtoSerializer())
                        .schemaUpdateOptions(
                                SchemaUpdateOptions.builder()
                                        .allowNewFields()
                                        .allowFieldRelaxation()
                                        .build())
                        .build();
        // end::bigquery-connector-storage-write-api-connections[]
    }
}
