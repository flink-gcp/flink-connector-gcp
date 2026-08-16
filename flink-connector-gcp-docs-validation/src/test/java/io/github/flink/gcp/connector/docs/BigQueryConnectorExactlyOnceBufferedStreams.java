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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.github.flink.gcp.connector.docs.BigQueryDocumentationTypes.MyEvent;
import io.github.flink.gcp.connector.docs.BigQueryDocumentationTypes.MyEventProtoSerializer;

final class BigQueryConnectorExactlyOnceBufferedStreams {

    private BigQueryConnectorExactlyOnceBufferedStreams() {}

    static void build(StreamExecutionEnvironment env) {
        // tag::bigquery-connector-exactly-once-buffered-streams[]
        Sink<MyEvent> sink =
                BigQuerySink.<MyEvent>builder()
                        .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                        .destination(TableDestination.of("my-project", "my_dataset", "events"))
                        .serializer(new MyEventProtoSerializer())
                        .bufferedStreamOptions(BufferedStreamOptions.builder().build())
                        .build();

        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(60_000); // EXACTLY_ONCE mode (the default)
        // end::bigquery-connector-exactly-once-buffered-streams[]
    }
}
