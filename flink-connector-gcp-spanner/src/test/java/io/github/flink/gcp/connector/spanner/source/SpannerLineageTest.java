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

package io.github.flink.gcp.connector.spanner.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.BatchTransactionId;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import io.github.flink.gcp.connector.base.lineage.PhysicalResourceFacet;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.sink.SpannerSink;
import io.github.flink.gcp.connector.spanner.sink.serializer.SpannerMutationSerializationSchema;
import io.github.flink.gcp.connector.spanner.source.batch.BatchReadSplit;
import io.github.flink.gcp.connector.spanner.source.batch.SpannerBatchReadEnumeratorState;
import io.github.flink.gcp.connector.spanner.source.batch.enumerator.PartitionPlanner;
import io.github.flink.gcp.connector.spanner.source.batch.enumerator.PartitionPlannerFactory;
import io.github.flink.gcp.connector.spanner.source.batch.reader.StructStream;
import io.github.flink.gcp.connector.spanner.source.batch.reader.StructStreamOpener;
import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;
import io.github.flink.gcp.connector.spanner.source.changestream.enumerator.SpannerChangeStreamCoordinatorClient;
import io.github.flink.gcp.connector.spanner.source.changestream.enumerator.SpannerChangeStreamCoordinatorClientFactory;
import io.github.flink.gcp.connector.spanner.source.changestream.reader.SpannerChangeStreamQueryClient;
import io.github.flink.gcp.connector.spanner.source.changestream.reader.SpannerChangeStreamQueryClientFactory;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerChangeStreamDeserializationSchema;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerStructDeserializationSchema;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the actual builder results with runtime hooks that must never run during inspection.
 */
public class SpannerLineageTest {
    private static final DatabaseDestination DATABASE = DatabaseDestination.of("p", "i", "db");
    private static final String KEY = "/lineage-test/must-not-read-credentials.json";

    @Test
    void explicitAndIndexReadsIdentifyOnlyTheirBaseTableBeforeAndAfterSerialization()
            throws Exception {
        for (String table : List.of("People", "Analytics.People", "Mixed.Case.With.Dots")) {
            for (SpannerReadOperation operation :
                    List.of(
                            SpannerReadOperation.read(table, KeySet.all(), List.of("id")),
                            SpannerReadOperation.readUsingIndex(
                                    table, "by_id", KeySet.all(), List.of("id")))) {
                Source<?, ?, ?> source = batch(operation);
                for (Source<?, ?, ?> candidate : List.of(source, roundTrip(source))) {
                    SourceLineageVertex vertex = (SourceLineageVertex) vertex(candidate);
                    assertThat(vertex.boundedness())
                            .isEqualTo(candidate.getBoundedness())
                            .isEqualTo(Boundedness.BOUNDED);
                    assertResource(
                            vertex,
                            "spanner-table",
                            "db." + table,
                            Map.of(
                                    "project",
                                    "p",
                                    "instance",
                                    "i",
                                    "database",
                                    "db",
                                    "table",
                                    table));
                }
            }
        }
    }

    @Test
    void queriesAndUnresolvedOperationsHaveEmptyDatasetsWithoutEvaluatingAnything()
            throws Exception {
        for (SpannerReadOperation operation :
                List.of(
                        SpannerReadOperation.query(
                                Statement.of("SELECT secret FROM private_table")),
                        SpannerReadOperationResolution.deferred(new ThrowingResolver()))) {
            Source<?, ?, ?> source = batch(operation);
            assertThat(vertex(source).datasets()).isEmpty();
            assertThat(vertex(roundTrip(source)).datasets()).isEmpty();
        }
    }

    @Test
    void mutationSerializerDoesNotEstablishAnyTableSet() throws Exception {
        Sink<Long> sink = sink();
        assertThat(vertex(sink)).isNotInstanceOf(SourceLineageVertex.class);
        assertThat(vertex(sink).datasets()).isEmpty();
        assertThat(vertex(roundTrip(sink)).datasets()).isEmpty();
    }

    @Test
    void changeStreamsRetainTheirIdentityAndActualBoundWithoutInferringFilteredTables()
            throws Exception {
        for (boolean bounded : List.of(false, true)) {
            SpannerChangeStreamSource<Long> source = changes(bounded);
            for (SpannerChangeStreamSource<Long> candidate : List.of(source, roundTrip(source))) {
                SourceLineageVertex vertex = candidate.getLineageVertex();
                assertThat(vertex.boundedness())
                        .isEqualTo(candidate.getBoundedness())
                        .isEqualTo(
                                bounded ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED);
                assertResource(
                        vertex,
                        "spanner-change-stream",
                        "db/changeStreams/Changes",
                        Map.of(
                                "project",
                                "p",
                                "instance",
                                "i",
                                "database",
                                "db",
                                "stream",
                                "Changes"));
                assertThat(candidate.getConfig().getServiceAccountKeyFile()).isEqualTo(KEY);
                assertThat(candidate.getConfig().getRecordFilter().hasFilters()).isTrue();
                assertThat(candidate.getConfig().getEndTimestamp())
                        .isEqualTo(source.getConfig().getEndTimestamp());
            }
        }
    }

    /** Builds the real batch source without allowing its runtime seams to open. */
    public static Source<Long, BatchReadSplit, SpannerBatchReadEnumeratorState> batch(
            SpannerReadOperation operation) {
        return SpannerSource.<Long>builder()
                .database(DATABASE)
                .readOperation(operation)
                .deserializer(new ThrowingCodec())
                .serviceAccountKeyFile(KEY)
                .plannerFactory(new ThrowingPlannerFactory())
                .opener(new ThrowingOpener())
                .build();
    }

    /** Builds the real Change Streams source with both client factories guarded. */
    public static SpannerChangeStreamSource<Long> changes(boolean bounded) {
        SpannerChangeStreamSourceBuilder<Long> builder =
                SpannerChangeStreamSource.<Long>builder()
                        .database(DATABASE)
                        .changeStreamName("Changes")
                        .deserializer(new ThrowingCodec())
                        .serviceAccountKeyFile(KEY)
                        .tableIncludeList(List.of("People.*"))
                        .coordinatorClientFactory(new ThrowingCoordinatorFactory())
                        .queryClientFactory(new ThrowingQueryFactory());
        if (bounded) {
            builder.endTimestamp(Instant.parse("2026-09-01T00:00:00Z"));
        }
        return builder.build();
    }

    /** Builds the real sink with a serializer that refuses even an initialization call. */
    public static Sink<Long> sink() {
        return SpannerSink.<Long>builder()
                .database(DATABASE)
                .serializer(new ThrowingCodec())
                .serviceAccountKeyFile(KEY)
                .build();
    }

    /** Inspects the provider interface on the actual returned object. */
    public static LineageVertex vertex(Object value) {
        assertThat(value).isInstanceOf(LineageVertexProvider.class);
        LineageVertex vertex = ((LineageVertexProvider) value).getLineageVertex();
        assertThat(vertex).isNotNull();
        return vertex;
    }

    /** Crosses the same Java serialization boundary as a submitted job configuration. */
    public static <T extends Serializable> T roundTrip(T value) throws Exception {
        return InstantiationUtil.deserializeObject(
                InstantiationUtil.serializeObject(value),
                SpannerLineageTest.class.getClassLoader());
    }

    private static void assertResource(
            LineageVertex vertex, String kind, String name, Map<String, String> identity) {
        assertThat(vertex.datasets())
                .singleElement()
                .satisfies(
                        dataset -> {
                            assertThat(dataset.namespace()).isEqualTo("spanner://p:i");
                            assertThat(dataset.name()).isEqualTo(name);
                            assertThat(dataset.facets()).containsOnlyKeys("gcp");
                            PhysicalResourceFacet facet =
                                    (PhysicalResourceFacet) dataset.facets().get("gcp");
                            assertThat(facet.name()).isEqualTo("gcp");
                            assertThat(facet.resources())
                                    .singleElement()
                                    .satisfies(
                                            resource -> {
                                                assertThat(resource.kind()).isEqualTo(kind);
                                                assertThat(resource.namespace())
                                                        .isEqualTo(dataset.namespace());
                                                assertThat(resource.name()).isEqualTo(name);
                                                assertThat(resource.identity()).isEqualTo(identity);
                                            });
                        });
    }

    private static final class ThrowingCodec
            implements SpannerStructDeserializationSchema<Long>,
                    SpannerChangeStreamDeserializationSchema<Long>,
                    SpannerMutationSerializationSchema<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public void open(DeserializationSchema.InitializationContext context) {
            throw new AssertionError("deserializer opened");
        }

        @Override
        public void open(SerializationSchema.InitializationContext context) {
            throw new AssertionError("serializer opened");
        }

        @Override
        public void deserialize(Struct row, Collector<Long> out) {
            throw new AssertionError("row deserialized");
        }

        @Override
        public void deserialize(DataChangeRecord row, Collector<Long> out) {
            throw new AssertionError("change deserialized");
        }

        @Override
        public Mutation serialize(Long row, SinkWriter.Context context) {
            throw new AssertionError("record serialized");
        }

        @Override
        public TypeInformation<Long> getProducedType() {
            throw new AssertionError("user type queried");
        }
    }

    private static final class ThrowingResolver implements SpannerReadOperationResolver {
        private static final long serialVersionUID = 1L;

        @Override
        public SpannerReadOperation resolve(DatabaseClient client, Timestamp timestamp) {
            throw new AssertionError("resolver evaluated");
        }
    }

    private static final class ThrowingPlannerFactory implements PartitionPlannerFactory {
        private static final long serialVersionUID = 1L;

        @Override
        public PartitionPlanner create() {
            throw new AssertionError("planner created");
        }
    }

    private static final class ThrowingOpener implements StructStreamOpener {
        private static final long serialVersionUID = 1L;

        @Override
        public StructStream open(BatchTransactionId transaction, Partition partition) {
            throw new AssertionError("partition opened");
        }

        @Override
        public void useCredentials(GoogleCredentials credentials) {
            throw new AssertionError("credentials used");
        }

        @Override
        public void close() {
            throw new AssertionError("opener closed");
        }
    }

    private static final class ThrowingCoordinatorFactory
            implements SpannerChangeStreamCoordinatorClientFactory {
        private static final long serialVersionUID = 1L;

        @Override
        public SpannerChangeStreamCoordinatorClient create() {
            throw new AssertionError("coordinator client opened");
        }
    }

    private static final class ThrowingQueryFactory
            implements SpannerChangeStreamQueryClientFactory {
        private static final long serialVersionUID = 1L;

        @Override
        public SpannerChangeStreamQueryClient create() {
            throw new AssertionError("query client opened");
        }
    }
}
