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

package io.github.flink.gcp.connector.base.lineage.internal;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;
import org.apache.flink.util.InstantiationUtil;

import io.github.flink.gcp.connector.base.lineage.PhysicalResourceFacet;
import io.github.flink.gcp.connector.base.lineage.ResourceIdentifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class LineageMetadataTest {
    private static final String NAMESPACE = "cloudtasks://p/loc";
    private static final ResourceIdentifier A = LineageIdentifiers.cloudTasksQueue("p", "loc", "a");
    private static final ResourceIdentifier B = LineageIdentifiers.cloudTasksQueue("p", "loc", "b");

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = "catalog.db.tasks")
    void selectsPhysicalOrLogicalDatasetsBeforeAndAfterSerialization(String logicalName)
            throws Exception {
        LineageMetadata metadata = LineageMetadata.of(logicalName);
        for (LineageMetadata candidate : List.of(metadata, InstantiationUtil.clone(metadata))) {
            for (Boundedness bound : Boundedness.values()) {
                SourceLineageVertex source = candidate.source(bound, NAMESPACE, List.of(B, A, B));
                assertThat(source.boundedness()).isEqualTo(bound);
                LineageVertex sink = candidate.sink(NAMESPACE, List.of(B, A, B));
                assertThat(sink).isNotInstanceOf(SourceLineageVertex.class);
                for (LineageVertex vertex : List.of(source, sink)) {
                    if (logicalName == null) {
                        assertThat(vertex.datasets())
                                .extracting(LineageDataset::name)
                                .containsExactly("a", "b");
                        assertResources(vertex.datasets().get(0), List.of(A));
                        assertResources(vertex.datasets().get(1), List.of(B));
                    } else {
                        assertThat(vertex.datasets())
                                .singleElement()
                                .satisfies(
                                        dataset -> {
                                            assertThat(dataset.name()).isEqualTo(logicalName);
                                            assertResources(dataset, List.of(A, B));
                                        });
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = "catalog.db.tasks")
    void unknownResourcesRetainOnlyAKnownLogicalDatasetAfterSerialization(String logicalName)
            throws Exception {
        LineageMetadata metadata = InstantiationUtil.clone(LineageMetadata.of(logicalName));
        for (LineageVertex vertex :
                List.of(
                        metadata.source(Boundedness.BOUNDED, NAMESPACE, List.of()),
                        metadata.sink(NAMESPACE, List.of()))) {
            if (logicalName == null) {
                assertThat(vertex.datasets()).isEmpty();
            } else {
                assertThat(vertex.datasets())
                        .singleElement()
                        .satisfies(
                                dataset -> {
                                    assertThat(dataset.name()).isEqualTo(logicalName);
                                    assertResources(dataset, List.of());
                                });
            }
        }
    }

    @Test
    void resourcesAndNamespacesComeFromEachInspectionAndVerticesAreSnapshots() {
        LineageMetadata metadata = LineageMetadata.of("catalog.db.tasks");
        List<ResourceIdentifier> resources = new ArrayList<>(List.of(A));
        LineageDataset first = metadata.sink(NAMESPACE, resources).datasets().get(0);
        resources.clear();
        resources.add(B);
        assertResources(first, List.of(A));
        assertResources(metadata.sink(NAMESPACE, resources).datasets().get(0), List.of(B));
        assertThat(metadata.sink("other-namespace", List.of()).datasets().get(0).namespace())
                .isEqualTo("other-namespace");
        LineageDataset physical =
                LineageMetadata.of(null).sink("unused", List.of(A)).datasets().get(0);
        assertResources(physical, List.of(A));
    }

    private static void assertResources(
            LineageDataset dataset, List<ResourceIdentifier> resources) {
        assertThat(dataset.namespace()).isEqualTo(NAMESPACE);
        assertThat(dataset.facets()).containsOnlyKeys("gcp");
        assertThat(((PhysicalResourceFacet) dataset.facets().get("gcp")).resources())
                .containsExactlyElementsOf(resources);
    }
}
