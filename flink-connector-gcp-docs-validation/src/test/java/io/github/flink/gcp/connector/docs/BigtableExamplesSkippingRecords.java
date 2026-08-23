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

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSink;

final class BigtableExamplesSkippingRecords {

    private BigtableExamplesSkippingRecords() {}

    static void build() {
        // tag::bigtable-examples-skipping-records[]
        BigtableSink.<Event>builder()
                .table(TableDestination.of("my-project", "my-instance", "readings"))
                .serializer(
                        (event, context) ->
                                event.isHeartbeat()
                                        ? null
                                        : RowMutationEntry.create("device#" + event.deviceId())
                                                .setCell(
                                                        "cf",
                                                        "reading",
                                                        event.timestampMicros(),
                                                        event.value()))
                .build();
        // end::bigtable-examples-skipping-records[]
    }

    static final class Event {

        boolean isHeartbeat() {
            return false;
        }

        String deviceId() {
            return "device-1";
        }

        long timestampMicros() {
            return 1L;
        }

        String value() {
            return "reading";
        }
    }
}
