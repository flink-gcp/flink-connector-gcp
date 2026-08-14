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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.spanner.Mutation;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.sink.SpannerSink;

final class SpannerQuickstartWrite {

    private SpannerQuickstartWrite() {}

    static void run() throws Exception {
        // tag::spanner-quickstart-write[]
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        // Not optional: the sink is at-least-once only with checkpointing, which is what makes
        // Flink wait for the batch to be applied before the barrier passes.
        env.enableCheckpointing(60_000);

        env.fromData("a-1", "a-2")
                .sinkTo(
                        SpannerSink.<String>builder()
                                .database(
                                        SpannerDatabase.of(
                                                "my-project", "my-instance", "orders-db"))
                                .serializer(
                                        (element, context) ->
                                                // insertOrUpdate, not insert: the sink is
                                                // at-least-once and Spanner's batch write has no
                                                // replay protection, so a record can arrive twice.
                                                // An upsert makes that a no-op; an insert makes it
                                                // a routed failure.
                                                Mutation.newInsertOrUpdateBuilder("Orders")
                                                        .set("OrderId")
                                                        .to("order#" + element)
                                                        .set("Total")
                                                        .to(element.length())
                                                        .build())
                                .build());

        env.execute("spanner-quickstart");
        // end::spanner-quickstart-write[]
    }
}
