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

package io.github.flink.gcp.connector.bigtable;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.base.lineage.PhysicalResourceFacet;
import io.github.flink.gcp.connector.bigtable.sink.BigtableMutateRowsSink;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSink;
import io.github.flink.gcp.connector.bigtable.sink.DestinationResolver;
import io.github.flink.gcp.connector.bigtable.sink.conditional.BigtableConditionalSink;
import io.github.flink.gcp.connector.bigtable.sink.conditional.ConditionalRequest;
import io.github.flink.gcp.connector.bigtable.sink.conditional.ConditionalSerializationSchema;
import io.github.flink.gcp.connector.bigtable.sink.readmodifywrite.BigtableReadModifyWriteSink;
import io.github.flink.gcp.connector.bigtable.sink.readmodifywrite.ReadModifyWriteRequest;
import io.github.flink.gcp.connector.bigtable.sink.readmodifywrite.ReadModifyWriteSerializationSchema;
import io.github.flink.gcp.connector.bigtable.sink.serializer.BigtableSerializationSchema;
import io.github.flink.gcp.connector.bigtable.source.BigtableChangeStreamSource;
import io.github.flink.gcp.connector.bigtable.source.BigtableSource;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutation;
import io.github.flink.gcp.connector.bigtable.source.readrows.BigtableScanSource;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableChangeStreamDeserializationSchema;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableRowDeserializationSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BigtableLineageTest {
    static final TableDestination TABLE =
            TableDestination.of("lineage-project", "lineage-instance", "events");
    static final String NAMESPACE = "bigtable://lineage-project/lineage-instance";
    static final AtomicInteger USER_CALLS = new AtomicInteger();
    static final AtomicInteger RESOLVER_CALLS = new AtomicInteger();
    @TempDir Path directory;

    @BeforeEach
    void resetCounters() {
        USER_CALLS.set(0);
        RESOLVER_CALLS.set(0);
    }

    @Test
    void publicSourcesReportTheTableAndActualBoundednessAfterSerialization() throws Exception {
        String key = directory.resolve("missing.json").toString();
        for (SourceKind kind : SourceKind.values()) {
            Source<String, ?, ?> original = source(kind, key);
            for (Source<String, ?, ?> candidate : List.of(original, roundTrip(original))) {
                SourceLineageVertex vertex = (SourceLineageVertex) lineage(candidate);
                assertPhysical(vertex);
                assertThat(vertex.boundedness()).isEqualTo(candidate.getBoundedness());
                assertThat(vertex.boundedness())
                        .isEqualTo(
                                kind == SourceKind.CHANGES
                                        ? Boundedness.CONTINUOUS_UNBOUNDED
                                        : Boundedness.BOUNDED);
            }
        }
        assertThat(USER_CALLS).hasValue(0);
        assertThatThrownBy(() -> BigtableCredentials.loadDataAndTableAdmin(key))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> new FakeSourceSchema().deserialize((Row) null, null))
                .isInstanceOf(AssertionError.class);
        assertThat(USER_CALLS).hasValue(1);
    }

    @ParameterizedTest
    @EnumSource(SinkKind.class)
    void publicSinksUseTheEffectiveResolverWithoutEvaluatingIt(SinkKind kind) throws Exception {
        String key = directory.resolve("missing.json").toString();
        for (DestinationOrder order : DestinationOrder.values()) {
            Sink<String> original = sink(kind, order, key);
            for (Sink<String> candidate : List.of(original, roundTrip(original))) {
                LineageVertex vertex = lineage(candidate);
                assertThat(vertex).isNotInstanceOf(SourceLineageVertex.class);
                if (order == DestinationOrder.DYNAMIC_LAST) {
                    assertThat(vertex.datasets()).isEmpty();
                } else {
                    assertPhysical(vertex);
                }
            }
        }
        assertThat(USER_CALLS).hasValue(0);
        assertThat(RESOLVER_CALLS).hasValue(0);
        assertThatThrownBy(() -> new FakeResolver().resolve("record", null))
                .isInstanceOf(AssertionError.class);
        assertThat(RESOLVER_CALLS).hasValue(1);
    }

    @Test
    void tableCopiesPreservePhysicalIdentityAndLeaveOriginalsUnchanged() throws Exception {
        String key = directory.resolve("missing.json").toString();
        for (SourceKind kind : SourceKind.values()) {
            Source<String, ?, ?> original = source(kind, key);
            Source<String, ?, ?> copy =
                    original instanceof BigtableScanSource
                            ? ((BigtableScanSource<String>) original)
                                    .withTableLineage("catalog.db.input")
                            : ((BigtableChangeStreamSource<String>) original)
                                    .withTableLineage("catalog.db.input");
            assertPhysical(lineage(original));
            for (Source<String, ?, ?> candidate : List.of(copy, roundTrip(copy))) {
                assertLogical(lineage(candidate), "catalog.db.input");
                assertThat(((SourceLineageVertex) lineage(candidate)).boundedness())
                        .isEqualTo(original.getBoundedness());
            }
        }
        for (SinkKind kind : SinkKind.values()) {
            Sink<String> original = sink(kind, DestinationOrder.FIXED, key);
            Sink<String> copy;
            switch (kind) {
                case MUTATE_ROWS:
                    copy =
                            ((BigtableMutateRowsSink<String>) original)
                                    .withTableLineage("catalog.db.output");
                    break;
                case CONDITIONAL:
                    copy =
                            ((BigtableConditionalSink<String>) original)
                                    .withTableLineage("catalog.db.output");
                    break;
                case READ_MODIFY_WRITE:
                    copy =
                            ((BigtableReadModifyWriteSink<String>) original)
                                    .withTableLineage("catalog.db.output");
                    break;
                default:
                    throw new AssertionError(kind);
            }
            assertPhysical(lineage(original));
            assertLogical(lineage(copy), "catalog.db.output");
            assertLogical(lineage(roundTrip(copy)), "catalog.db.output");
        }
        assertThat(USER_CALLS).hasValue(0);
        assertThat(RESOLVER_CALLS).hasValue(0);
    }

    @Test
    void extractionIsRepeatableAndSnapshotsAreImmutable() {
        var candidate =
                sink(
                        SinkKind.MUTATE_ROWS,
                        DestinationOrder.FIXED,
                        directory.resolve("missing.json").toString());
        LineageVertex first = lineage(candidate);
        assertPhysical(first);
        assertPhysical(lineage(candidate));
        LineageDataset dataset = first.datasets().get(0);
        assertThatThrownBy(() -> first.datasets().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> dataset.facets().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> facet(dataset).resources().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> facet(dataset).resources().get(0).identity().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    static Source<String, ?, ?> source(SourceKind kind, String key) {
        if (kind == SourceKind.SCAN) {
            return BigtableSource.<String>builder()
                    .table(TABLE)
                    .deserializer(new FakeSourceSchema())
                    .appProfileId("scan-profile")
                    .prefix("prefix")
                    .filter(Filters.FILTERS.family().exactMatch("cf"))
                    .serviceAccountKeyFile(key)
                    .build();
        }
        var builder =
                BigtableChangeStreamSource.<String>builder()
                        .table(TABLE)
                        .appProfileId("change-profile")
                        .deserializer(new FakeSourceSchema())
                        .serviceAccountKeyFile(key);
        if (kind == SourceKind.BOUNDED_CHANGES) {
            builder.boundedTimestamp(Instant.parse("2026-01-02T00:00:00Z"));
        }
        return builder.build();
    }

    static Sink<String> sink(SinkKind kind, DestinationOrder order, String key) {
        switch (kind) {
            case MUTATE_ROWS:
                var mutations =
                        BigtableSink.<String>builder()
                                .table(TABLE)
                                .serializer(new FakeMutations())
                                .serviceAccountKeyFile(key);
                if (order != DestinationOrder.FIXED) {
                    mutations.destinationResolver(new FakeResolver());
                }
                if (order == DestinationOrder.FIXED_LAST) {
                    mutations.table(TABLE);
                }
                return mutations.build();
            case CONDITIONAL:
                var conditional =
                        BigtableConditionalSink.<String>builder()
                                .table(TABLE)
                                .serializer(new FakeConditional())
                                .serviceAccountKeyFile(key);
                if (order != DestinationOrder.FIXED) {
                    conditional.destinationResolver(new FakeResolver());
                }
                if (order == DestinationOrder.FIXED_LAST) {
                    conditional.table(TABLE);
                }
                return conditional.build();
            case READ_MODIFY_WRITE:
                var requests =
                        BigtableReadModifyWriteSink.<String>builder()
                                .table(TABLE)
                                .serializer(new FakeReadModifyWrite())
                                .serviceAccountKeyFile(key);
                if (order != DestinationOrder.FIXED) {
                    requests.destinationResolver(new FakeResolver());
                }
                if (order == DestinationOrder.FIXED_LAST) {
                    requests.table(TABLE);
                }
                return requests.build();
            default:
                throw new AssertionError(kind);
        }
    }

    static LineageVertex lineage(Object candidate) {
        assertThat(candidate).isInstanceOf(LineageVertexProvider.class);
        return ((LineageVertexProvider) candidate).getLineageVertex();
    }

    static void assertPhysical(LineageVertex vertex) {
        assertThat(vertex.datasets())
                .singleElement()
                .satisfies(
                        dataset -> {
                            assertThat(dataset.name()).isEqualTo("events");
                            assertDataset(dataset);
                        });
    }

    static void assertLogical(LineageVertex vertex, String name) {
        assertThat(vertex.datasets())
                .singleElement()
                .satisfies(
                        dataset -> {
                            assertThat(dataset.name()).isEqualTo(name);
                            assertDataset(dataset);
                        });
    }

    private static void assertDataset(LineageDataset dataset) {
        assertThat(dataset.namespace()).isEqualTo(NAMESPACE);
        assertThat(dataset.facets()).containsOnlyKeys("gcp");
        assertThat(facet(dataset).name()).isEqualTo("gcp");
        assertThat(facet(dataset).resources())
                .singleElement()
                .satisfies(
                        resource -> {
                            assertThat(resource.kind()).isEqualTo("bigtable-table");
                            assertThat(resource.namespace()).isEqualTo(NAMESPACE);
                            assertThat(resource.name()).isEqualTo("events");
                            assertThat(resource.identity())
                                    .isEqualTo(
                                            Map.of(
                                                    "project",
                                                    "lineage-project",
                                                    "instance",
                                                    "lineage-instance",
                                                    "table",
                                                    "events"));
                        });
    }

    private static PhysicalResourceFacet facet(LineageDataset dataset) {
        return (PhysicalResourceFacet) dataset.facets().get("gcp");
    }

    static <T> T roundTrip(T value) throws Exception {
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        try (var output = new LambdaCheckingOutputStream(bytes)) {
            output.writeObject(value);
        }
        return InstantiationUtil.deserializeObject(
                bytes.toByteArray(), BigtableLineageTest.class.getClassLoader());
    }

    @Test
    void serializationGuardRejectsConnectorOwnedLambdas() {
        DestinationResolver<String> lambda = (element, context) -> TABLE;
        assertThatThrownBy(() -> roundTrip(lambda))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Connector-owned serializable lambda");
    }

    private static final class LambdaCheckingOutputStream extends java.io.ObjectOutputStream {
        private LambdaCheckingOutputStream(java.io.OutputStream output) throws IOException {
            super(output);
            enableReplaceObject(true);
        }

        @Override
        protected Object replaceObject(Object object) {
            if (object instanceof java.lang.invoke.SerializedLambda) {
                String owner = ((java.lang.invoke.SerializedLambda) object).getCapturingClass();
                // Flink's RowData field getters are serializable lambdas. The connector must not
                // mint its own.
                assertThat(owner)
                        .as("Connector-owned serializable lambda")
                        .doesNotStartWith("io/github/flink/gcp/connector/");
            }
            return object;
        }
    }

    enum SourceKind {
        SCAN,
        CHANGES,
        BOUNDED_CHANGES
    }

    enum SinkKind {
        MUTATE_ROWS,
        CONDITIONAL,
        READ_MODIFY_WRITE
    }

    enum DestinationOrder {
        FIXED,
        DYNAMIC_LAST,
        FIXED_LAST
    }

    private static void failUserCode() {
        USER_CALLS.incrementAndGet();
        throw new AssertionError("Lineage must not invoke user code");
    }

    static final class FakeSourceSchema
            implements BigtableRowDeserializationSchema<String>,
                    BigtableChangeStreamDeserializationSchema<String> {
        @Override
        public void open(DeserializationSchema.InitializationContext context) {
            failUserCode();
        }

        @Override
        public void deserialize(Row row, Collector<String> out) {
            failUserCode();
        }

        @Override
        public void deserialize(BigtableChangeStreamMutation mutation, Collector<String> out) {
            failUserCode();
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }

    static final class FakeMutations implements BigtableSerializationSchema<String> {
        @Override
        public void open(SerializationSchema.InitializationContext context) {
            failUserCode();
        }

        @Override
        public RowMutationEntry serialize(String value, SinkWriter.Context context) {
            failUserCode();
            return null;
        }
    }

    static final class FakeConditional implements ConditionalSerializationSchema<String> {
        @Override
        public void open(SerializationSchema.InitializationContext context) {
            failUserCode();
        }

        @Override
        public ConditionalRequest serialize(String value, SinkWriter.Context context) {
            failUserCode();
            return null;
        }
    }

    static final class FakeReadModifyWrite implements ReadModifyWriteSerializationSchema<String> {
        @Override
        public void open(SerializationSchema.InitializationContext context) {
            failUserCode();
        }

        @Override
        public ReadModifyWriteRequest serialize(String value, SinkWriter.Context context) {
            failUserCode();
            return null;
        }
    }

    static final class FakeResolver implements DestinationResolver<String> {
        @Override
        public TableDestination resolve(String value, SinkWriter.Context context) {
            RESOLVER_CALLS.incrementAndGet();
            throw new AssertionError("Lineage must not evaluate a dynamic resolver");
        }
    }
}
