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

import com.google.cloud.spanner.Mutation;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;
import io.github.flink.gcp.connector.spanner.sink.ConstraintViolationPolicy;
import io.github.flink.gcp.connector.spanner.sink.FailedMutation;
import io.github.flink.gcp.connector.spanner.sink.SpannerSink;
import io.github.flink.gcp.connector.spanner.sink.SpannerWriterOptions;

import java.time.Duration;

final class SpannerExamplesSinkOptions {

    private SpannerExamplesSinkOptions() {}

    static void droppingRefusedMutations() {
        // tag::spanner-examples-dropping-refused-mutations[]
        SpannerSink.<String>builder()
                .database(DatabaseDestination.of("my-project", "my-instance", "orders-db"))
                .serializer(
                        (orderId, context) ->
                                Mutation.newInsertOrUpdateBuilder("Orders")
                                        .set("OrderId")
                                        .to(orderId)
                                        .set("Total")
                                        .to(0L)
                                        .build())
                .failedMutationHandler(FailureHandler.logAndDrop())
                .build();
        // end::spanner-examples-dropping-refused-mutations[]
    }

    static void routeConstraintViolations() {
        // tag::spanner-examples-constraint-violation-policy[]
        SpannerSink.<String>builder()
                .database(DatabaseDestination.of("my-project", "my-instance", "orders-db"))
                .serializer(
                        (orderId, context) ->
                                Mutation.newInsertOrUpdateBuilder("Orders")
                                        .set("OrderId")
                                        .to(orderId)
                                        .set("Total")
                                        .to(0L)
                                        .build())
                .constraintViolationPolicy(ConstraintViolationPolicy.ROUTE_TO_FAILURE_HANDLER)
                .failedMutationHandler(FailureHandler.logAndDrop())
                .build();
        // end::spanner-examples-constraint-violation-policy[]
    }

    static void customFailureHandler() {
        // tag::spanner-examples-custom-failure-handler[]
        SpannerSink.<String>builder()
                .database(DatabaseDestination.of("my-project", "my-instance", "orders-db"))
                .serializer(
                        (orderId, context) ->
                                Mutation.newInsertOrUpdateBuilder("Orders")
                                        .set("OrderId")
                                        .to(orderId)
                                        .set("Total")
                                        .to(0L)
                                        .build())
                .failedMutationHandler(
                        (FailureHandler<FailedMutation>)
                                failure ->
                                        System.getLogger("SpannerFailures")
                                                .log(
                                                        System.Logger.Level.WARNING,
                                                        "Dropping a mutation on {0}: {1}",
                                                        failure.getTable(),
                                                        failure.getErrorMessage()))
                .build();
        // end::spanner-examples-custom-failure-handler[]
    }

    static void batchOptions() {
        // tag::spanner-examples-batch-options[]
        SpannerSink.<String>builder()
                .database(DatabaseDestination.of("my-project", "my-instance", "orders-db"))
                .serializer(
                        (orderId, context) ->
                                Mutation.newInsertOrUpdateBuilder("Orders")
                                        .set("OrderId")
                                        .to(orderId)
                                        .set("Total")
                                        .to(0L)
                                        .build())
                .writerOptions(
                        SpannerWriterOptions.builder()
                                .maxBatchMutations(100)
                                // A commit delay trades latency for throughput by letting Spanner
                                // group this commit with others. Zero to 500 ms.
                                .maxCommitDelay(Duration.ofMillis(50))
                                // A backfill that must not disturb serving traffic on the same
                                // instance.
                                .rpcPriority(SpannerRpcPriority.LOW)
                                .build())
                .build();
        // end::spanner-examples-batch-options[]
    }

    static void emulatorSink() {
        // tag::spanner-examples-emulator-sink[]
        SpannerSink.<String>builder()
                .database(DatabaseDestination.of("my-project", "my-instance", "orders-db"))
                .serializer(
                        (orderId, context) ->
                                Mutation.newInsertOrUpdateBuilder("Orders")
                                        .set("OrderId")
                                        .to(orderId)
                                        .set("Total")
                                        .to(0L)
                                        .build())
                .emulatorEndpoint("localhost:9010")
                .build();
        // end::spanner-examples-emulator-sink[]
    }
}
