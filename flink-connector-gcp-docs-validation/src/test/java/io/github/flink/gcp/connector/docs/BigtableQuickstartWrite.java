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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSink;

final class BigtableQuickstartWrite {

    private BigtableQuickstartWrite() {}

    static void run() throws Exception {
        // tag::bigtable-quickstart-write[]
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        // Not optional: the sink is at-least-once only with checkpointing, which is what makes
        // Flink wait
        // for every outstanding mutation before the barrier passes.
        env.enableCheckpointing(60_000);

        env.fromData("a-1", "a-2")
                .sinkTo(
                        BigtableSink.<String>builder()
                                .table(TableDestination.of("my-project", "my-instance", "orders"))
                                .serializer(
                                        (element, context) ->
                                                RowMutationEntry.create("order#" + element)
                                                        // An explicit cell timestamp, so a replayed
                                                        // record
                                                        // overwrites this cell instead of adding a
                                                        // version.
                                                        .setCell(
                                                                "cf",
                                                                "payload",
                                                                context.timestamp() == null
                                                                        ? 0L
                                                                        : context.timestamp()
                                                                                * 1_000,
                                                                element))
                                .build());

        env.execute("bigtable-quickstart");
        // end::bigtable-quickstart-write[]
    }
}
