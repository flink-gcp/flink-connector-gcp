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

package io.github.flink.gcp.connector.datastore.table;

import org.apache.flink.configuration.ConfigOption;

import io.github.flink.gcp.connector.datastore.sink.DatastoreSinkBuilder;
import io.github.flink.gcp.connector.datastore.sink.DatastoreWriterOptions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Keeps the DDL surface equal to the DataStream builders it maps onto (ADR-0133). */
class DatastoreOptionParityTest {

    @Test
    void everyWriterKnobHasATableOptionOrARecordedReason() {
        assertThat(publicSettersOf(DatastoreWriterOptions.Builder.class))
                .containsExactlyInAnyOrder(
                        "maxBatchMutations",
                        "maxBatchBytes",
                        "requestTimeout",
                        "recoveryInitialBackoff",
                        "recoveryMaxBackoff",
                        "recoveryMaxAttempts",
                        "throttlingEnabled",
                        "throttlingParallelism",
                        "maxConsecutiveRejections",
                        "idAllocationBatchSize");

        // maxConsecutiveRejections bounds rejections under a dropping failure handler, and the
        // table sink has none: no DDL can name a serializable handler, so the first routed
        // rejection already fails the job. The other knobs each have one option.
        assertThat(declaredKeys())
                .contains(
                        "sink.buffer-flush.max-mutations",
                        "sink.buffer-flush.max-size",
                        "sink.request-timeout",
                        "sink.recovery.initial-backoff",
                        "sink.recovery.max-backoff",
                        "sink.recovery.max-attempts",
                        "sink.throttling.enabled",
                        "sink.throttling.parallelism",
                        "sink.id-allocation.batch-size")
                .doesNotContain("sink.max-consecutive-rejections");
    }

    @Test
    void everySinkBuilderSetterIsMappedOrSuppliedByTheTableLayer() {
        assertThat(publicSettersOf(DatastoreSinkBuilder.class))
                .containsExactlyInAnyOrder(
                        "database",
                        "serializer",
                        "writerOptions",
                        "failedMutationHandler",
                        "serviceAccountKeyFile",
                        "emulatorEndpoint");

        // database is assembled from project and database, serializer from the physical schema,
        // kind, namespace and sink.unindexed-columns, writerOptions from the options above. The
        // failure handler stays fail-job: no DDL names a serializable handler.
        assertThat(declaredKeys())
                .contains(
                        "project",
                        "database",
                        "kind",
                        "namespace",
                        "sink.unindexed-columns",
                        "service-account-key-file",
                        "emulator-endpoint");
    }

    @Test
    void everyDeclaredOptionHasAHomeInTheTableConnector() {
        assertThat(declaredKeys())
                .containsExactlyInAnyOrder(
                        "project",
                        "database",
                        "kind",
                        "namespace",
                        "emulator-endpoint",
                        "service-account-key-file",
                        "sink.unindexed-columns",
                        "sink.buffer-flush.max-mutations",
                        "sink.buffer-flush.max-size",
                        "sink.request-timeout",
                        "sink.recovery.initial-backoff",
                        "sink.recovery.max-backoff",
                        "sink.recovery.max-attempts",
                        "sink.throttling.enabled",
                        "sink.throttling.parallelism",
                        "sink.id-allocation.batch-size");
        DatastoreDynamicTableFactory factory = new DatastoreDynamicTableFactory();
        Set<String> accepted =
                factory.requiredOptions().stream()
                        .map(ConfigOption::key)
                        .collect(Collectors.toSet());
        factory.optionalOptions().stream().map(ConfigOption::key).forEach(accepted::add);
        assertThat(accepted).containsAll(declaredKeys());
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
