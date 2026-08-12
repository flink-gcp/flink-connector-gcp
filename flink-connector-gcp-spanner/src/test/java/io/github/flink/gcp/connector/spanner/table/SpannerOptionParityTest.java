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

package io.github.flink.gcp.connector.spanner.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;

import io.github.flink.gcp.connector.spanner.sink.SpannerSinkBuilder;
import io.github.flink.gcp.connector.spanner.sink.SpannerWriterOptions;
import io.github.flink.gcp.connector.spanner.source.SpannerSourceBuilder;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Keeps the DDL surface equal to the DataStream builders it maps onto. */
class SpannerOptionParityTest {

    @Test
    void documentedSpecialTypeSeparatorsParseIntoDistinctPaths() {
        Configuration options =
                Configuration.fromMap(
                        Map.of(
                                "schema.json-field-paths",
                                "metadata;nested.payload",
                                "schema.uuid-field-paths",
                                "id;nested.id",
                                "schema.proto-type-names",
                                "event:example.Event,nested.event:example.Nested"));

        assertThat(options.get(SpannerConnectorOptions.SCHEMA_JSON_FIELD_PATHS))
                .containsExactly("metadata", "nested.payload");
        assertThat(options.get(SpannerConnectorOptions.SCHEMA_UUID_FIELD_PATHS))
                .containsExactly("id", "nested.id");
        assertThat(options.get(SpannerConnectorOptions.SCHEMA_PROTO_TYPE_NAMES))
                .containsExactlyInAnyOrderEntriesOf(
                        Map.of("event", "example.Event", "nested.event", "example.Nested"));
    }

    @Test
    void everyWriterKnobHasExactlyOneTableOption() {
        assertThat(publicSettersOf(SpannerWriterOptions.Builder.class))
                .containsExactlyInAnyOrder(
                        "maxBatchCells",
                        "maxBatchMutations",
                        "maxBatchBytes",
                        "maxCommitDelay",
                        "rpcPriority",
                        "retryInitialBackoff",
                        "retryMaxBackoff",
                        "retryMaxAttempts");

        assertThat(declaredKeys())
                .contains(
                        "sink.buffer-flush.max-cells",
                        "sink.buffer-flush.max-mutations",
                        "sink.buffer-flush.max-size",
                        "sink.buffer-flush.max-commit-delay",
                        "sink.rpc-priority",
                        "sink.retry.initial-backoff",
                        "sink.retry.max-backoff",
                        "sink.retry.max-attempts");
    }

    @Test
    void everySinkBuilderSetterIsMappedOrDeliberatelySuppliedByTheTableLayer() {
        assertThat(publicSettersOf(SpannerSinkBuilder.class))
                .containsExactlyInAnyOrder(
                        "database",
                        "serializer",
                        "writerOptions",
                        "failedMutationHandler",
                        "constraintViolationPolicy",
                        "emulatorEndpoint");

        // database is assembled from project/instance/database, serializer from the physical DDL,
        // writerOptions from the eight options above. Failure policy stays fail-job because a DDL
        // has no serializable FailureHandler to pair with a dropping constraint policy.
        assertThat(declaredKeys()).contains("project", "instance", "database", "emulator-endpoint");
    }

    @Test
    void everySourceBuilderSetterIsMappedOrSuppliedByTheTableLayer() {
        assertThat(publicSettersOf(SpannerSourceBuilder.class))
                .containsExactlyInAnyOrder(
                        "database",
                        "readOperation",
                        "deserializer",
                        "timestampBound",
                        "maxPartitions",
                        "partitionSizeBytes",
                        "dataBoostEnabled",
                        "rpcPriority",
                        "emulatorEndpoint");

        // The database and read operation come from destination options and the projected DDL;
        // the planner supplies the RowData deserializer. Every remaining setter has one option.
        assertThat(declaredKeys())
                .contains(
                        "scan.timestamp-bound.read-timestamp",
                        "scan.timestamp-bound.exact-staleness",
                        "scan.partition.max-partitions",
                        "scan.partition.size",
                        "scan.data-boost-enabled",
                        "scan.rpc-priority",
                        "emulator-endpoint");
    }

    @Test
    void everyDeclaredOptionHasAHomeInTheTableConnector() {
        assertThat(declaredKeys())
                .containsExactlyInAnyOrder(
                        "project",
                        "instance",
                        "database",
                        "table",
                        "dialect",
                        "emulator-endpoint",
                        "schema.json-field-paths",
                        "schema.uuid-field-paths",
                        "schema.proto-type-names",
                        "schema.enum-type-names",
                        "scan.partition.max-partitions",
                        "scan.partition.size",
                        "scan.data-boost-enabled",
                        "scan.rpc-priority",
                        "scan.timestamp-bound.read-timestamp",
                        "scan.timestamp-bound.exact-staleness",
                        "lookup.async",
                        "sink.buffer-flush.max-cells",
                        "sink.buffer-flush.max-mutations",
                        "sink.buffer-flush.max-size",
                        "sink.buffer-flush.max-commit-delay",
                        "sink.rpc-priority",
                        "sink.retry.initial-backoff",
                        "sink.retry.max-backoff",
                        "sink.retry.max-attempts");
    }

    private static Set<String> publicSettersOf(Class<?> builder) {
        return Arrays.stream(builder.getDeclaredMethods())
                .filter(method -> Modifier.isPublic(method.getModifiers()))
                .filter(method -> method.getReturnType() == builder)
                .map(Method::getName)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static Set<String> declaredKeys() {
        return Arrays.stream(SpannerConnectorOptions.class.getDeclaredFields())
                .filter(field -> Modifier.isStatic(field.getModifiers()))
                .filter(field -> ConfigOption.class.isAssignableFrom(field.getType()))
                .map(SpannerOptionParityTest::option)
                .map(ConfigOption::key)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static ConfigOption<?> option(Field field) {
        try {
            return (ConfigOption<?>) field.get(null);
        } catch (IllegalAccessException e) {
            throw new AssertionError(e);
        }
    }
}
