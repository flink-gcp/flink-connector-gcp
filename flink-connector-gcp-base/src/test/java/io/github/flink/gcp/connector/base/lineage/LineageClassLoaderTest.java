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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.util.FlinkUserCodeClassLoaders;
import org.apache.flink.util.MutableURLClassLoader;

import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LineageClassLoaderTest {
    @Test
    void documentedParentFirstPatternsShareTheApiWithTheListener() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(
                CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL,
                List.of(
                        "io.github.flink.gcp.connector.base.lineage.PhysicalResourceFacet",
                        "io.github.flink.gcp.connector.base.lineage.ResourceIdentifier"));
        try (MutableURLClassLoader loader = loader(configuration)) {
            PhysicalResourceFacet facet = (PhysicalResourceFacet) extract(loader);
            ResourceIdentifier resource = facet.resources().get(0);
            assertThat(facet.getClass().getClassLoader()).isSameAs(getClass().getClassLoader());
            assertThat(resource.getClass().getClassLoader()).isSameAs(getClass().getClassLoader());
            assertThat(resource.name()).isEqualTo("topic:p:t");
        }
    }

    @Test
    void matchingClassNamesAloneDoNotMakeChildLoadedValuesCastable() throws Exception {
        try (MutableURLClassLoader loader = loader(new Configuration())) {
            Object facet = extract(loader);
            assertThat(facet.getClass().getName()).isEqualTo(PhysicalResourceFacet.class.getName());
            assertThat(facet.getClass().getClassLoader()).isNotSameAs(getClass().getClassLoader());
            assertThatThrownBy(() -> PhysicalResourceFacet.class.cast(facet))
                    .isInstanceOf(ClassCastException.class);
        }
    }

    private static MutableURLClassLoader loader(Configuration configuration) {
        return FlinkUserCodeClassLoaders.create(
                new URL[] {
                    ResourceIdentifier.class.getProtectionDomain().getCodeSource().getLocation()
                },
                ResourceIdentifier.class.getClassLoader(),
                configuration);
    }

    private static Object extract(ClassLoader loader) throws Exception {
        Class<?> factory =
                loader.loadClass(
                        "io.github.flink.gcp.connector.base.lineage.internal.LineageIdentifiers");
        assertThat(factory.getClassLoader()).isNotSameAs(ResourceIdentifier.class.getClassLoader());
        Object resource =
                factory.getMethod("pubSubTopic", String.class, String.class).invoke(null, "p", "t");
        Class<?> vertices =
                loader.loadClass("io.github.flink.gcp.connector.base.lineage.internal.Lineage");
        LineageVertex vertex =
                (LineageVertex)
                        vertices.getMethod("sink", List.class).invoke(null, List.of(resource));
        return vertex.datasets().get(0).facets().get("gcp");
    }
}
