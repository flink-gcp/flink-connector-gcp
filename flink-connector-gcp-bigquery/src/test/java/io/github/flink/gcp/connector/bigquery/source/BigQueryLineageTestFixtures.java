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

package io.github.flink.gcp.connector.bigquery.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.util.Collector;

import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import io.github.flink.gcp.connector.base.lineage.PhysicalResourceFacet;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkBuilder;
import io.github.flink.gcp.connector.bigquery.sink.DestinationResolver;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcChangeTypeProvider;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcOptions;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializationSchema;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.github.flink.gcp.connector.bigquery.source.enumerator.ReadSessionCreator;
import io.github.flink.gcp.connector.bigquery.source.enumerator.ReadSessionCreatorFactory;
import io.github.flink.gcp.connector.bigquery.source.query.QueryResult;
import io.github.flink.gcp.connector.bigquery.source.query.QueryRunner;
import io.github.flink.gcp.connector.bigquery.source.query.QuerySpec;
import io.github.flink.gcp.connector.bigquery.source.reader.RowStream;
import io.github.flink.gcp.connector.bigquery.source.reader.RowStreamOpener;
import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializationSchema;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Actual public-builder configurations shared by direct and Flink graph extraction tests. */
public final class BigQueryLineageTestFixtures {
    public static final TableDestination TABLE =
            TableDestination.of("resource-project", "Dataset", "Events");

    private BigQueryLineageTestFixtures() {}

    public static BigQuerySourceBuilder<String> sourceBuilder(String keyFile) {
        ForbiddenSourceAccess access = new ForbiddenSourceAccess();
        return BigQuerySource.<String>builder()
                .deserializer(new ForbiddenDeserializer())
                .serviceAccountKeyFile(keyFile)
                .sessionCreatorFactory(access)
                .rowStreamOpener(access)
                .queryRunner(access);
    }

    public static Source<String, ?, ?> source(String mode, String keyFile) {
        BigQuerySourceBuilder<String> builder = sourceBuilder(keyFile);
        if (mode.equals("query")) {
            builder.query("SELECT secret_column FROM undisclosed.table")
                    .parentProject("billing-project")
                    .queryLocation("US")
                    .queryResultDataset("results-project.scratch");
        } else {
            builder.table(TABLE).parentProject("billing-project");
            if (mode.equals("view")) {
                builder.materializeViews().queryLocation("US").queryResultDataset("scratch");
            } else if (mode.equals("filtered")) {
                builder.selectedFields(java.util.List.of("id"))
                        .rowRestriction("id > 1")
                        .snapshotTime(Instant.ofEpochSecond(1));
            }
        }
        return builder.build();
    }

    public static BigQuerySinkBuilder<String> sinkBuilder(WriteMethod method, String keyFile) {
        BigQuerySinkBuilder<String> builder =
                BigQuerySink.<String>builder()
                        .table(TABLE)
                        .serializer(new ForbiddenSerializer())
                        .serviceAccountKeyFile(keyFile)
                        .writeMethod(method);
        if (method == WriteMethod.STORAGE_API_EXACTLY_ONCE) {
            builder.bufferedStreamOptions(BufferedStreamOptions.builder().build());
        } else if (method == WriteMethod.FILE_LOADS) {
            builder.fileLoadsOptions(
                    FileLoadsOptions.builder()
                            .stagingPath("gs://lineage-never-opened/staging")
                            .build());
        }
        return builder;
    }

    public static Sink<String> sink(WriteMethod method, boolean cdc, String keyFile) {
        BigQuerySinkBuilder<String> builder = sinkBuilder(method, keyFile);
        if (cdc) {
            builder.cdcOptions(
                    CdcOptions.<String>builder(CdcChangeTypeProvider.upsertOnly()).build());
        }
        return builder.build();
    }

    public static LineageVertex vertex(Object configured) {
        assertThat(configured).isInstanceOf(LineageVertexProvider.class);
        LineageVertex result = ((LineageVertexProvider) configured).getLineageVertex();
        assertThat(result).isNotNull();
        return result;
    }

    public static void assertPhysical(
            LineageDataset dataset, String logicalName, TableDestination table) {
        assertThat(dataset.namespace()).isEqualTo("bigquery");
        assertThat(dataset.name()).isEqualTo(logicalName);
        assertThat(dataset.facets()).containsOnlyKeys("gcp");
        PhysicalResourceFacet facet = (PhysicalResourceFacet) dataset.facets().get("gcp");
        assertThat(facet.name()).isEqualTo("gcp");
        assertThat(facet.resources())
                .singleElement()
                .satisfies(
                        resource -> {
                            assertThat(resource.kind()).isEqualTo("bigquery-table");
                            assertThat(resource.namespace()).isEqualTo("bigquery");
                            assertThat(resource.name()).isEqualTo(name(table));
                            assertThat(resource.identity())
                                    .containsExactlyInAnyOrderEntriesOf(
                                            Map.of(
                                                    "project",
                                                    table.getProject(),
                                                    "dataset",
                                                    table.getDataset(),
                                                    "table",
                                                    table.getTable()));
                        });
    }

    public static String name(TableDestination table) {
        return table.getProject() + "." + table.getDataset() + "." + table.getTable();
    }

    public static final class ForbiddenResolver implements DestinationResolver<String> {
        @Override
        public TableDestination resolve(String element, SinkWriter.Context context) {
            throw new AssertionError("Lineage evaluated a user destination");
        }
    }

    public static final class ConstantUserResolver implements DestinationResolver<String> {
        public int calls;

        @Override
        public TableDestination resolve(String element, SinkWriter.Context context) {
            calls++;
            return TABLE;
        }
    }

    public static final class ForbiddenDeserializer
            implements BigQueryRowDeserializationSchema<String> {
        @Override
        public void open(DeserializationSchema.InitializationContext context) {
            throw new AssertionError("Lineage opened the deserializer");
        }

        @Override
        public Schema getReaderSchema() {
            throw new AssertionError("Lineage inspected a payload schema");
        }

        @Override
        public void deserialize(GenericRecord row, Collector<String> out) {
            throw new AssertionError("Lineage deserialized a record");
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }

    public static final class ForbiddenSerializer extends BigQueryProtoSerializationSchema<String> {
        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            throw new AssertionError("Lineage inspected a payload schema");
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            throw new AssertionError("Lineage inspected a descriptor");
        }

        @Override
        public ByteString serialize(String element) {
            throw new AssertionError("Lineage serialized a record");
        }
    }

    private static final class ForbiddenSourceAccess
            implements ReadSessionCreatorFactory, QueryRunner, RowStreamOpener {
        @Override
        public ReadSessionCreator create() {
            throw new AssertionError("Lineage created a read client");
        }

        @Override
        public QueryResult run(QuerySpec spec) {
            throw new AssertionError("Lineage ran SQL");
        }

        @Override
        public boolean isView(TableDestination table) {
            throw new AssertionError("Lineage looked up a view");
        }

        @Override
        public RowStream open(String streamName, long offset) {
            throw new AssertionError("Lineage opened a read stream");
        }

        @Override
        public void close() {
            throw new AssertionError("Lineage closed a read client");
        }
    }
}
