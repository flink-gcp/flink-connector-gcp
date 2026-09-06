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

package io.github.flink.gcp.connector.base.lineage;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;

import io.github.flink.gcp.connector.base.lineage.internal.Lineage;
import io.github.flink.gcp.connector.base.lineage.internal.LineageIdentifiers;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

class LineageTest {
    @Test
    void namesEachKindWithoutDiscoveringResources() {
        List<ResourceIdentifier> resources =
                List.of(
                        LineageIdentifiers.bigQueryTable("p", "d", "View"),
                        LineageIdentifiers.pubSubTopic("p", "t"),
                        LineageIdentifiers.pubSubSubscription("p", "s"),
                        LineageIdentifiers.bigtableTable("p", "i", "T"),
                        LineageIdentifiers.spannerTable("p", "i", "d", null, "T", null, "T"),
                        LineageIdentifiers.spannerChangeStream("p", "i", "d", "Changes"),
                        LineageIdentifiers.cloudTasksQueue("p", "loc", "q"));
        assertThat(resources)
                .extracting(ResourceIdentifier::kind)
                .containsExactly(
                        "bigquery-table",
                        "pubsub-topic",
                        "pubsub-subscription",
                        "bigtable-table",
                        "spanner-table",
                        "spanner-change-stream",
                        "cloudtasks-queue");
        assertThat(resources)
                .extracting(ResourceIdentifier::namespace)
                .containsExactly(
                        "bigquery",
                        "pubsub",
                        "pubsub",
                        "bigtable://p/i",
                        "spanner://p:i",
                        "spanner://p:i",
                        "cloudtasks://p/loc");
        assertThat(resources)
                .extracting(ResourceIdentifier::name)
                .containsExactly(
                        "p.d.View",
                        "topic:p:t",
                        "subscription:p:s",
                        "T",
                        "d.T",
                        "d/changeStreams/Changes",
                        "q");
        assertThat(resources)
                .extracting(ResourceIdentifier::identity)
                .containsExactly(
                        Map.of("project", "p", "dataset", "d", "table", "View"),
                        Map.of("project", "p", "topic", "t"),
                        Map.of("project", "p", "subscription", "s"),
                        Map.of("project", "p", "instance", "i", "table", "T"),
                        Map.of("project", "p", "instance", "i", "database", "d", "table", "T"),
                        Map.of(
                                "project",
                                "p",
                                "instance",
                                "i",
                                "database",
                                "d",
                                "stream",
                                "Changes"),
                        Map.of("project", "p", "location", "loc", "queue", "q"));
    }

    @Test
    void preservesParsedAndConfiguredSpannerNamesSeparately() {
        ResourceIdentifier quoted =
                LineageIdentifiers.spannerTable(
                        "p", "i", "db", "Sales", "Order.Detail", "\"Sales\"", "\"Order.Detail\"");
        assertThat(quoted.name()).isEqualTo("db.Sales.Order.Detail");
        assertThat(quoted.identity())
                .contains(entry("schema", "\"Sales\""), entry("table", "\"Order.Detail\""));
        ResourceIdentifier unquoted =
                LineageIdentifiers.spannerTable(
                        "p", "i", "db", "Sales", "Order.Detail", "Sales", "Order.Detail");
        assertThat(quoted).isNotEqualTo(unquoted);
        assertThat(new PhysicalResourceFacet(List.of(quoted, unquoted)).resources()).hasSize(2);
        assertThat(LineageIdentifiers.spannerTable("p", "i", "db", "", "T", null, "T").name())
                .isEqualTo("db.T");
        assertThat(
                        LineageIdentifiers.spannerTable("p", "i", "db", "public", "t", null, "t")
                                .identity())
                .doesNotContainKey("schema");
    }

    @Test
    void snapshotsEveryCollectionBoundary() {
        Map<String, String> identity = new HashMap<>(Map.of("table", "t", "project", "p"));
        ResourceIdentifier resource = new ResourceIdentifier("kind", "namespace", "name", identity);
        List<ResourceIdentifier> input = new ArrayList<>(List.of(resource));
        LineageVertex vertex = Lineage.sink(input);
        PhysicalResourceFacet facet = facet(vertex.datasets().get(0));
        identity.clear();
        input.clear();
        assertThat(facet.resources()).containsExactly(resource);
        assertThat(resource.identity()).containsExactly(entry("project", "p"), entry("table", "t"));
        assertThatThrownBy(() -> vertex.datasets().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> vertex.datasets().get(0).facets().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> facet.resources().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> resource.identity().put("table", "other"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> resource.identity().entrySet().iterator().next().setValue("other"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void normalizesResourceOrderAndDuplicatesAcrossRepeatedExtraction() {
        ResourceIdentifier a = LineageIdentifiers.pubSubTopic("p", "a");
        ResourceIdentifier b = LineageIdentifiers.pubSubTopic("p", "b");
        List<ResourceIdentifier> input = List.of(b, a, LineageIdentifiers.pubSubTopic("p", "a"));
        for (int i = 0; i < 3; i++) {
            LineageVertex vertex = Lineage.sink(input);
            assertThat(vertex.datasets())
                    .extracting(LineageDataset::name)
                    .containsExactly("topic:p:a", "topic:p:b");
            assertThat(vertex.datasets())
                    .allSatisfy(dataset -> assertThat(facet(dataset).resources()).hasSize(1));
            assertThat(new PhysicalResourceFacet(input).resources()).containsExactly(a, b);
        }
        assertThat(a).isEqualTo(LineageIdentifiers.pubSubTopic("p", "a"));
        assertThat(a.hashCode()).isEqualTo(LineageIdentifiers.pubSubTopic("p", "a").hashCode());
    }

    @Test
    void identityOrderingDoesNotConflateDelimiterBearingComponents() {
        ResourceIdentifier a = new ResourceIdentifier("k", "n", "t", Map.of("a", "b, c=d"));
        ResourceIdentifier b = new ResourceIdentifier("k", "n", "t", Map.of("a", "b", "c", "d"));
        assertThat(a.identity().toString()).isEqualTo(b.identity().toString());
        assertThat(new PhysicalResourceFacet(List.of(a, b)).resources()).containsExactly(b, a);
        assertThat(new PhysicalResourceFacet(List.of(b, a)).resources()).containsExactly(b, a);
    }

    @Test
    void emptyVerticesRetainTheirDirectionAndSourceBoundedness() {
        for (Boundedness bound : Boundedness.values()) {
            SourceLineageVertex vertex = Lineage.source(bound, List.of());
            assertThat(vertex.boundedness()).isSameAs(bound);
            assertThat(vertex.datasets()).isEmpty();
        }
        LineageVertex sink = Lineage.sink(List.of());
        assertThat(sink).isNotInstanceOf(SourceLineageVertex.class);
        assertThat(sink.datasets()).isEmpty();
    }

    @Test
    void tableAdapterRetainsAllResourcesInOneKnownLogicalDataset() {
        ResourceIdentifier a = LineageIdentifiers.bigQueryTable("p", "d", "a");
        ResourceIdentifier b = LineageIdentifiers.bigQueryTable("p", "d", "b");
        SourceLineageVertex source =
                Lineage.tableSource(
                        "catalog.db.input", "bigquery", Boundedness.BOUNDED, List.of(b, a, b));
        assertThat(source.boundedness()).isEqualTo(Boundedness.BOUNDED);
        assertThat(source.datasets())
                .singleElement()
                .satisfies(
                        dataset -> {
                            assertThat(dataset.name()).isEqualTo("catalog.db.input");
                            assertThat(dataset.namespace()).isEqualTo("bigquery");
                            assertThat(facet(dataset).resources()).containsExactly(a, b);
                        });
        LineageVertex sink = Lineage.tableSink("catalog.db.output", "bigquery", List.of());
        assertThat(sink).isNotInstanceOf(SourceLineageVertex.class);
        assertThat(sink.datasets())
                .singleElement()
                .satisfies(
                        dataset -> {
                            assertThat(dataset.name()).isEqualTo("catalog.db.output");
                            assertThat(facet(dataset).resources()).isEmpty();
                        });
    }

    @Test
    void serializedMetadataStillBuildsImmutableVertices() throws Exception {
        PhysicalResourceFacet original =
                new PhysicalResourceFacet(
                        List.of(
                                LineageIdentifiers.spannerTable(
                                        "p", "i", "d", "S", "T", "\"S\"", "\"T\"")));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(original);
        }
        PhysicalResourceFacet restored;
        try (ObjectInputStream in =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (PhysicalResourceFacet) in.readObject();
        }
        assertThat(restored.resources()).containsExactlyElementsOf(original.resources());
        assertThatThrownBy(() -> restored.resources().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> restored.resources().get(0).identity().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(facet(Lineage.sink(restored.resources()).datasets().get(0)).resources())
                .containsExactlyElementsOf(original.resources());
    }

    private static PhysicalResourceFacet facet(LineageDataset dataset) {
        assertThat(dataset.facets()).containsOnlyKeys("gcp");
        PhysicalResourceFacet facet = (PhysicalResourceFacet) dataset.facets().get("gcp");
        assertThat(facet.name()).isEqualTo("gcp");
        return facet;
    }
}
