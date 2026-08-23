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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.source.BigQuerySource;
import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializationSchema;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

final class BigQueryExamplesPreferredStreamCount {

    private BigQueryExamplesPreferredStreamCount() {}

    static void build(StreamExecutionEnvironment env, Schema readerSchema) {
        // tag::bigquery-examples-preferred-stream-count[]
        BigQuerySource.<GenericRecord>builder()
                .table(TableDestination.of("my-project", "my_dataset", "events"))
                .deserializer(BigQueryRowDeserializationSchema.genericRecord(readerSchema))
                .preferredMinStreamCount(3 * env.getParallelism())
                .build();
        // end::bigquery-examples-preferred-stream-count[]
    }
}
