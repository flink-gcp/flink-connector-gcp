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

import com.google.cloud.bigtable.data.v2.models.Filters;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.BigtableSource;
import io.github.flink.gcp.connector.docs.BigtableDocumentationTypes.Order;
import io.github.flink.gcp.connector.docs.BigtableDocumentationTypes.OrderRows;

final class BigtableExamplesServerFilter {

    private BigtableExamplesServerFilter() {}

    static void build() {
        // tag::bigtable-examples-server-filter[]
        BigtableSource.<Order>builder()
                .table(TableDestination.of("my-project", "my-instance", "orders"))
                .deserializer(new OrderRows())
                .filter(
                        Filters.FILTERS
                                .chain()
                                .filter(Filters.FILTERS.family().exactMatch("cf"))
                                .filter(Filters.FILTERS.qualifier().exactMatch("payload"))
                                // The latest version of each cell only.
                                .filter(Filters.FILTERS.limit().cellsPerColumn(1)))
                .build();
        // end::bigtable-examples-server-filter[]
    }
}
