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
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions.TimePartitioningType;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializationSchema;
import io.github.flink.gcp.connector.docs.BigQueryDocumentationTypes.OrderEvent;
import io.github.flink.gcp.connector.docs.BigQueryExamplesTablePerDay.DailyTableResolver;

import java.time.Duration;
import java.util.List;

final class BigQueryExamplesTableAutoCreation {

    private BigQueryExamplesTableAutoCreation() {}

    static void build(BigQueryProtoSerializationSchema<OrderEvent> serializer) {
        // tag::bigquery-examples-table-auto-creation[]
        BigQuerySink.<OrderEvent>builder()
                .destinationResolver(new DailyTableResolver("my-project", "my_dataset", "orders"))
                .serializer(serializer)
                .tableCreateOptions(
                        TableCreateOptions.builder()
                                .timePartitioning(TimePartitioningType.DAY, "created_at")
                                .timePartitioningExpiration(Duration.ofDays(90))
                                .clusteredFields(List.of("customer_id"))
                                .build())
                .build();
        // end::bigquery-examples-table-auto-creation[]
    }
}
