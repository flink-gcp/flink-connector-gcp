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
import io.github.flink.gcp.connector.bigquery.sink.CdcTableOptions;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableReconciliationPolicy;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcChangeType;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcOptions;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;

import java.time.Duration;
import java.util.Collections;

final class BigQueryCdcTableCreation {

    private BigQueryCdcTableCreation() {}

    static Sink<MyMutation> build(BigQueryProtoSerializer<MyMutation> serializer) {
        // tag::bigquery-cdc-table-creation[]
        Sink<MyMutation> sink =
                BigQuerySink.<MyMutation>builder()
                        .writeMethod(WriteMethod.STORAGE_API_AT_LEAST_ONCE)
                        .destination(TableDestination.of("my-project", "my_dataset", "accounts"))
                        .serializer(serializer)
                        .cdcTableOptions(
                                CdcTableOptions.builder()
                                        .primaryKeyColumns(Collections.singletonList("id"))
                                        .maxStaleness(Duration.ofMinutes(10))
                                        .build())
                        .cdcTableReconciliationPolicy(CdcTableReconciliationPolicy.RECONCILE)
                        .cdcOptions(
                                CdcOptions.<MyMutation>builder(
                                                mutation ->
                                                        mutation.deleted()
                                                                ? CdcChangeType.DELETE
                                                                : CdcChangeType.UPSERT)
                                        .sequenceNumberProvider(MyMutation::sequenceNumber)
                                        .build())
                        .build();
        // end::bigquery-cdc-table-creation[]
        return sink;
    }

    interface MyMutation {

        boolean deleted();

        String sequenceNumber();
    }
}
