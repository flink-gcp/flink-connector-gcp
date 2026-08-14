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

import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSink;
import io.github.flink.gcp.connector.docs.BigtableDocumentationTypes.OrderEvent;
import io.github.flink.gcp.connector.docs.BigtableDocumentationTypes.OrderEventMutations;

final class BigtableExamplesDroppingBadRows {

    private BigtableExamplesDroppingBadRows() {}

    static void build() {
        // tag::bigtable-examples-dropping-bad-rows[]
        BigtableSink.<OrderEvent>builder()
                .table(TableDestination.of("my-project", "my-instance", "orders"))
                .serializer(new OrderEventMutations())
                .failedMutationHandler(FailureHandler.logAndDrop())
                .build();
        // end::bigtable-examples-dropping-bad-rows[]
    }
}
