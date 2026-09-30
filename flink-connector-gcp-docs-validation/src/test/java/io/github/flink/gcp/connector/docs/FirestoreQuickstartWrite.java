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

import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.sink.FirestoreSink;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWrite;

import java.util.Map;

final class FirestoreQuickstartWrite {

    private FirestoreQuickstartWrite() {}

    static void run() throws Exception {
        // tag::firestore-quickstart-write[]
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        // Not optional: the sink is at-least-once only with checkpointing, which is what makes
        // Flink wait for every write to be answered before the barrier passes.
        env.enableCheckpointing(60_000);

        env.fromData("a-1", "a-2")
                .sinkTo(
                        FirestoreSink.<String>builder()
                                .database(DatabaseDestination.of("my-project"))
                                .serializer(
                                        (element, context) ->
                                                // set, not create: the sink is at-least-once, so
                                                // a record can arrive twice. A set makes that a
                                                // no-op; a create makes it a routed failure.
                                                FirestoreWrite.set(
                                                        "orders/" + element,
                                                        Map.of("total", (long) element.length())))
                                .build());

        env.execute("firestore-quickstart");
        // end::firestore-quickstart-write[]
    }
}
