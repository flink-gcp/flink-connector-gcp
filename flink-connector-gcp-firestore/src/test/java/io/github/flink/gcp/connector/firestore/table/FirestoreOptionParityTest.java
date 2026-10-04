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

package io.github.flink.gcp.connector.firestore.table;

import org.apache.flink.configuration.ConfigOption;

import io.github.flink.gcp.connector.firestore.sink.FirestoreSinkBuilder;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWriterOptions;
import io.github.flink.gcp.connector.firestore.source.FirestoreSourceBuilder;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Keeps the DDL surface equal to the DataStream builders it maps onto (ADR-0133). */
class FirestoreOptionParityTest {

    @Test
    void everyWriterKnobHasATableOptionOrARecordedReason() {
        assertThat(publicSettersOf(FirestoreWriterOptions.Builder.class))
                .containsExactlyInAnyOrder(
                        "throttlingEnabled",
                        "initialOpsPerSecond",
                        "maxOpsPerSecond",
                        "writeMaxAttempts",
                        "retryTotalTimeout",
                        "retryInitialDelay",
                        "retryDelayMultiplier",
                        "retryMaxDelay",
                        "retryInitialRpcTimeout",
                        "retryRpcTimeoutMultiplier",
                        "retryMaxRpcTimeout",
                        "retryMaxAttempts",
                        "maxInFlightWrites",
                        "maxInFlightBytes",
                        "maxConsecutiveRejections");

        // maxConsecutiveRejections bounds rejections under a dropping failure handler, and the
        // table sink has none: no DDL can name a serializable handler, so the first routed
        // rejection already fails the job. The other knobs each have one option.
        assertThat(declaredKeys())
                .contains(
                        "sink.throttling.enabled",
                        "sink.throttling.initial-ops-per-second",
                        "sink.throttling.max-ops-per-second",
                        "sink.write.max-attempts",
                        "sink.retry.total-timeout",
                        "sink.retry.initial-delay",
                        "sink.retry.delay-multiplier",
                        "sink.retry.max-delay",
                        "sink.retry.initial-rpc-timeout",
                        "sink.retry.rpc-timeout-multiplier",
                        "sink.retry.max-rpc-timeout",
                        "sink.retry.max-attempts",
                        "sink.in-flight.max-writes",
                        "sink.in-flight.max-bytes")
                .doesNotContain("sink.max-consecutive-rejections");
    }

    @Test
    void everySinkBuilderSetterIsMappedOrSuppliedByTheTableLayer() {
        assertThat(publicSettersOf(FirestoreSinkBuilder.class))
                .containsExactlyInAnyOrder(
                        "database",
                        "serializer",
                        "writerOptions",
                        "failedWriteHandler",
                        "preconditionFailurePolicy",
                        "serviceAccountKeyFile",
                        "emulatorEndpoint");

        // database is assembled from project and database, serializer from the physical schema,
        // the markers and sink.write-mode, writerOptions from the options above. The failure
        // handler stays fail-job (no DDL names a serializable handler), and no table write
        // carries a precondition, so preconditionFailurePolicy has nothing to govern.
        assertThat(declaredKeys())
                .contains(
                        "project",
                        "database",
                        "collection",
                        "geo-point-field-paths",
                        "reference-field-paths",
                        "sink.write-mode",
                        "service-account-key-file",
                        "emulator-endpoint");
    }

    @Test
    void everyDeclaredOptionHasAHomeInTheTableConnector() {
        assertThat(declaredKeys())
                .containsExactlyInAnyOrder(
                        "project",
                        "database",
                        "collection",
                        "emulator-endpoint",
                        "service-account-key-file",
                        "geo-point-field-paths",
                        "reference-field-paths",
                        "sink.write-mode",
                        "sink.throttling.enabled",
                        "sink.throttling.initial-ops-per-second",
                        "sink.throttling.max-ops-per-second",
                        "sink.write.max-attempts",
                        "sink.retry.total-timeout",
                        "sink.retry.initial-delay",
                        "sink.retry.delay-multiplier",
                        "sink.retry.max-delay",
                        "sink.retry.initial-rpc-timeout",
                        "sink.retry.rpc-timeout-multiplier",
                        "sink.retry.max-rpc-timeout",
                        "sink.retry.max-attempts",
                        "sink.in-flight.max-writes",
                        "sink.in-flight.max-bytes",
                        "type-mismatch-policy",
                        "scan.collection-group",
                        "scan.partition.max-partitions",
                        "scan.read-time",
                        "scan.max-rows-per-fetch",
                        "lookup.async");
        FirestoreDynamicTableFactory factory = new FirestoreDynamicTableFactory();
        Set<String> accepted =
                factory.requiredOptions().stream()
                        .map(ConfigOption::key)
                        .collect(Collectors.toSet());
        factory.optionalOptions().stream().map(ConfigOption::key).forEach(accepted::add);
        assertThat(accepted).containsAll(declaredKeys());
    }

    @Test
    void everySourceBuilderSetterIsMappedOrSuppliedByTheTableLayer() {
        assertThat(publicSettersOf(FirestoreSourceBuilder.class))
                .containsExactlyInAnyOrder(
                        "database",
                        "deserializer",
                        "collectionGroup",
                        "query",
                        "select",
                        "partitionCount",
                        "readTime",
                        "pageSize",
                        "serviceAccountKeyFile",
                        "emulatorEndpoint");

        // database is assembled from project and database, deserializer from the physical schema,
        // the markers and type-mismatch-policy, collectionGroup and query from collection and
        // scan.collection-group, select from the planner's projection; the other three map one
        // option each.
        assertThat(declaredKeys())
                .contains(
                        "collection",
                        "type-mismatch-policy",
                        "scan.collection-group",
                        "scan.partition.max-partitions",
                        "scan.read-time",
                        "scan.max-rows-per-fetch");
    }

    private static Set<String> declaredKeys() {
        return DeclaredOptions.all().stream().map(ConfigOption::key).collect(Collectors.toSet());
    }

    private static Set<String> publicSettersOf(Class<?> builder) {
        return Arrays.stream(builder.getDeclaredMethods())
                .filter(m -> Modifier.isPublic(m.getModifiers()))
                .filter(m -> !Modifier.isStatic(m.getModifiers()))
                .filter(m -> m.getReturnType().equals(builder))
                .map(Method::getName)
                .collect(Collectors.toSet());
    }
}
