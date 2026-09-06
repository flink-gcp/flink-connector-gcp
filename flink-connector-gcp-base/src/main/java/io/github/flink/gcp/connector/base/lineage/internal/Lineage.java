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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;

import io.github.flink.gcp.connector.base.lineage.PhysicalResourceFacet;
import io.github.flink.gcp.connector.base.lineage.ResourceIdentifier;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Pure metadata assembly shared by connector runtimes; no client or callback is accepted. */
@Internal
public final class Lineage {

    private Lineage() {}

    /** Creates a DataStream source vertex from configured physical resources. */
    public static SourceLineageVertex source(
            Boundedness boundedness, List<ResourceIdentifier> resources) {
        return new SourceVertex(boundedness, datasets(resources));
    }

    /** Creates a DataStream sink vertex; an unknown destination supplies an empty list. */
    public static LineageVertex sink(List<ResourceIdentifier> resources) {
        return new Vertex(datasets(resources));
    }

    /**
     * Carries all physical resources in one logical Table source dataset, for Flink's single
     * dataset selection. The caller supplies the catalog identifier and connector namespace.
     */
    public static SourceLineageVertex tableSource(
            String logicalName,
            String namespace,
            Boundedness boundedness,
            List<ResourceIdentifier> resources) {
        return new SourceVertex(
                boundedness,
                List.of(new Dataset(logicalName, namespace, new PhysicalResourceFacet(resources))));
    }

    /** Carries all physical resources in one logical Table sink dataset. */
    public static LineageVertex tableSink(
            String logicalName, String namespace, List<ResourceIdentifier> resources) {
        return new Vertex(
                List.of(new Dataset(logicalName, namespace, new PhysicalResourceFacet(resources))));
    }

    private static List<LineageDataset> datasets(List<ResourceIdentifier> resources) {
        return new PhysicalResourceFacet(resources)
                .resources().stream()
                        .map(
                                resource ->
                                        new Dataset(
                                                resource.name(),
                                                resource.namespace(),
                                                new PhysicalResourceFacet(List.of(resource))))
                        .collect(Collectors.toUnmodifiableList());
    }

    private static final class Dataset implements LineageDataset {
        private final String name;
        private final String namespace;
        private final Map<String, LineageDatasetFacet> facets;

        private Dataset(String name, String namespace, PhysicalResourceFacet facet) {
            this.name = Objects.requireNonNull(name, "name");
            this.namespace = Objects.requireNonNull(namespace, "namespace");
            this.facets = Map.of(facet.name(), facet);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String namespace() {
            return namespace;
        }

        @Override
        public Map<String, LineageDatasetFacet> facets() {
            return facets;
        }
    }

    private static class Vertex implements LineageVertex {
        private final List<LineageDataset> datasets;

        private Vertex(List<LineageDataset> datasets) {
            this.datasets = List.copyOf(datasets);
        }

        @Override
        public List<LineageDataset> datasets() {
            return datasets;
        }
    }

    private static final class SourceVertex extends Vertex implements SourceLineageVertex {
        private final Boundedness boundedness;

        private SourceVertex(Boundedness boundedness, List<LineageDataset> datasets) {
            super(datasets);
            this.boundedness = Objects.requireNonNull(boundedness, "boundedness");
        }

        @Override
        public Boundedness boundedness() {
            return boundedness;
        }
    }
}
