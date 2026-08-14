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

package io.github.flink.gcp.connector.docs;

import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSink;
import io.github.flink.gcp.connector.bigtable.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigtable.sink.GcRule;
import io.github.flink.gcp.connector.bigtable.sink.TableCreateOptions;
import io.github.flink.gcp.connector.docs.BigtableDocumentationTypes.OrderEvent;
import io.github.flink.gcp.connector.docs.BigtableDocumentationTypes.OrderEventMutations;

import java.time.Duration;

final class BigtableConnectorTableAutoCreation {

    private BigtableConnectorTableAutoCreation() {}

    static void build() {
        // tag::bigtable-connector-table-auto-creation[]
        BigtableSink.<OrderEvent>builder()
                .table(TableDestination.of("my-project", "my-instance", "orders"))
                .serializer(new OrderEventMutations())
                .createDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .tableCreateOptions(
                        TableCreateOptions.builder()
                                .columnFamily(
                                        "cf",
                                        GcRule.union(
                                                GcRule.maxVersions(1),
                                                GcRule.maxAge(Duration.ofDays(30))))
                                .build())
                .build();
        // end::bigtable-connector-table-auto-creation[]
    }
}
