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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;
import org.apache.flink.util.InstantiationUtil;

import io.github.flink.gcp.connector.base.lineage.PhysicalResourceFacet;
import io.github.flink.gcp.connector.bigquery.sink.BigQueryLineageSink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.source.BigQueryLineageTestFixtures.ConstantUserResolver;
import io.github.flink.gcp.connector.bigquery.source.BigQueryLineageTestFixtures.ForbiddenDeserializer;
import io.github.flink.gcp.connector.bigquery.source.BigQueryLineageTestFixtures.ForbiddenResolver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.List;

import static io.github.flink.gcp.connector.bigquery.source.BigQueryLineageTestFixtures.TABLE;
import static io.github.flink.gcp.connector.bigquery.source.BigQueryLineageTestFixtures.assertPhysical;
import static io.github.flink.gcp.connector.bigquery.source.BigQueryLineageTestFixtures.name;
import static io.github.flink.gcp.connector.bigquery.source.BigQueryLineageTestFixtures.sink;
import static io.github.flink.gcp.connector.bigquery.source.BigQueryLineageTestFixtures.sinkBuilder;
import static io.github.flink.gcp.connector.bigquery.source.BigQueryLineageTestFixtures.source;
import static io.github.flink.gcp.connector.bigquery.source.BigQueryLineageTestFixtures.sourceBuilder;
import static io.github.flink.gcp.connector.bigquery.source.BigQueryLineageTestFixtures.vertex;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BigQueryLineageTest {
    @TempDir Path directory;

    private String keyFile() {
        return directory.resolve("absent-key.json").toString();
    }

    @ParameterizedTest
    @ValueSource(strings = {"table", "view", "filtered", "query"})
    void publicSourceReportsConfiguredInputsBeforeAndAfterSerialization(String mode)
            throws Exception {
        Source<String, ?, ?> source = source(mode, keyFile());
        for (Source<String, ?, ?> configured : List.of(source, InstantiationUtil.clone(source))) {
            assertThat(vertex(configured)).isInstanceOf(SourceLineageVertex.class);
            SourceLineageVertex metadata = (SourceLineageVertex) vertex(configured);
            assertThat(metadata.boundedness())
                    .isEqualTo(configured.getBoundedness())
                    .isEqualTo(Boundedness.BOUNDED);
            if (mode.equals("query")) {
                assertThat(metadata.datasets()).isEmpty();
            } else {
                assertThat(metadata.datasets())
                        .singleElement()
                        .satisfies(dataset -> assertPhysical(dataset, name(TABLE), TABLE));
            }
        }
    }

    @ParameterizedTest
    @EnumSource(WriteMethod.class)
    void everyPublicSinkReportsTheSameFixedOutputWithoutOpeningClients(WriteMethod method)
            throws Exception {
        Sink<String> sink = sink(method, false, keyFile());
        for (Sink<String> configured : List.of(sink, InstantiationUtil.clone(sink))) {
            assertThat(vertex(configured)).isNotInstanceOf(SourceLineageVertex.class);
            assertThat(vertex(configured).datasets())
                    .singleElement()
                    .satisfies(dataset -> assertPhysical(dataset, name(TABLE), TABLE));
        }
    }

    @Test
    void defaultStreamCdcKeepsTheFixedTable() throws Exception {
        Sink<String> sink = sink(WriteMethod.STORAGE_API_AT_LEAST_ONCE, true, keyFile());
        assertThat(vertex(InstantiationUtil.clone(sink)).datasets())
                .singleElement()
                .satisfies(dataset -> assertPhysical(dataset, name(TABLE), TABLE));
    }

    @ParameterizedTest
    @EnumSource(WriteMethod.class)
    void finalResolverWinsAndUserResolversAreNeverEvaluated(WriteMethod method) throws Exception {
        Sink<String> dynamic =
                sinkBuilder(method, keyFile()).destinationResolver(new ForbiddenResolver()).build();
        assertThat(vertex(dynamic).datasets()).isEmpty();
        assertThat(vertex(InstantiationUtil.clone(dynamic)).datasets()).isEmpty();
        ConstantUserResolver constant = new ConstantUserResolver();
        assertThat(
                        vertex(sinkBuilder(method, keyFile()).destinationResolver(constant).build())
                                .datasets())
                .isEmpty();
        assertThat(constant.calls).isZero();
        Sink<String> fixed =
                sinkBuilder(method, keyFile())
                        .destinationResolver(new ForbiddenResolver())
                        .table(TABLE)
                        .build();
        assertThat(vertex(fixed).datasets())
                .singleElement()
                .satisfies(dataset -> assertPhysical(dataset, name(TABLE), TABLE));
    }

    @ParameterizedTest
    @ValueSource(strings = {"Events$20260906", "Events@1788652800000"})
    void configuredComponentSyntaxSurvives(String tableName) {
        TableDestination table = TableDestination.of("example.com:project", "DataSet", tableName);
        assertThat(vertex(sourceBuilder(keyFile()).table(table).build()).datasets())
                .singleElement()
                .satisfies(dataset -> assertPhysical(dataset, name(table), table));
        for (WriteMethod method : WriteMethod.values()) {
            assertThat(vertex(sinkBuilder(method, keyFile()).table(table).build()).datasets())
                    .singleElement()
                    .satisfies(dataset -> assertPhysical(dataset, name(table), table));
        }
    }

    @Test
    void defaultSourceFactoriesAlsoRemainCredentialFreeDuringInspection() throws Exception {
        Source<String, ?, ?> source =
                BigQuerySource.<String>builder()
                        .table(TABLE)
                        .deserializer(new ForbiddenDeserializer())
                        .serviceAccountKeyFile(keyFile())
                        .materializeViews()
                        .build();
        assertThat(vertex(InstantiationUtil.clone(source)).datasets())
                .singleElement()
                .satisfies(dataset -> assertPhysical(dataset, name(TABLE), TABLE));
    }

    @ParameterizedTest
    @EnumSource(WriteMethod.class)
    void sqlSinkCopiesRetainConcreteAbilitiesAndLeaveOriginalMetadataAlone(WriteMethod method)
            throws Exception {
        Sink<String> original = sink(method, false, keyFile());
        Sink<String> adapted =
                ((BigQueryLineageSink<String>) original).withTableLineage("catalog.db.output");
        assertThat(adapted).isExactlyInstanceOf(original.getClass()).isNotSameAs(original);
        assertThat(vertex(InstantiationUtil.clone(adapted)).datasets())
                .singleElement()
                .satisfies(dataset -> assertPhysical(dataset, "catalog.db.output", TABLE));
        assertThat(vertex(original).datasets())
                .singleElement()
                .satisfies(dataset -> assertPhysical(dataset, name(TABLE), TABLE));
    }

    @Test
    void sqlQueryCopyKeepsOnlyTheLogicalTableAfterSerialization() throws Exception {
        BigQueryStorageReadSource<String> original =
                (BigQueryStorageReadSource<String>) source("query", keyFile());
        BigQueryStorageReadSource<String> adapted = original.withTableLineage("catalog.db.input");
        assertThat(vertex(original).datasets()).isEmpty();
        assertThat(vertex(InstantiationUtil.clone(adapted)).datasets())
                .singleElement()
                .satisfies(
                        dataset -> {
                            assertThat(dataset.name()).isEqualTo("catalog.db.input");
                            assertThat(dataset.namespace()).isEqualTo("bigquery");
                            assertThat(
                                            ((PhysicalResourceFacet) dataset.facets().get("gcp"))
                                                    .resources())
                                    .isEmpty();
                        });
    }

    @Test
    void callersCannotChangeSubsequentExtraction() {
        Source<String, ?, ?> configured = source("table", keyFile());
        LineageVertex first = vertex(configured);
        LineageDataset dataset = first.datasets().get(0);
        PhysicalResourceFacet facet = (PhysicalResourceFacet) dataset.facets().get("gcp");
        assertThatThrownBy(() -> first.datasets().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> dataset.facets().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> facet.resources().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> facet.resources().get(0).identity().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(vertex(configured).datasets())
                .singleElement()
                .satisfies(next -> assertPhysical(next, name(TABLE), TABLE));
    }
}
