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

package io.github.flink.gcp.connector.testutils.sql;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;
import org.apache.flink.streaming.api.lineage.LineageVertex;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies the shared API using isolated SQL helper loaders and a common listener loader. */
@Internal
final class SqlLineageClassLoading {
    private static final String API = "io.github.flink.gcp.connector.base.lineage.";

    private SqlLineageClassLoading() {}

    static void assertSharedApi(ShadedJar current) throws Exception {
        ClassLoader listenerLoader = SqlLineageClassLoading.class.getClassLoader();
        Class<?> facetType = listenerLoader.loadClass(API + "PhysicalResourceFacet");
        Class<?> identifierType = listenerLoader.loadClass(API + "ResourceIdentifier");
        List<URLClassLoader> loaders = new ArrayList<>();
        List<Path> jars = sqlJars(current.path().toRealPath());
        assertThat(jars).as("SQL lineage artifacts").contains(current.path().toRealPath());
        try {
            for (Path jar : jars) {
                String module = jar.getParent().getParent().getFileName().toString();
                String product = module.substring("flink-sql-connector-gcp-".length());
                String shaded = "io.github.flink.gcp.connector." + product + ".shaded.";
                URLClassLoader loader =
                        new URLClassLoader(new URL[] {jar.toUri().toURL()}, listenerLoader) {
                            @Override
                            protected synchronized Class<?> loadClass(String name, boolean resolve)
                                    throws ClassNotFoundException {
                                if (!name.startsWith(shaded)) {
                                    return super.loadClass(name, resolve);
                                }
                                Class<?> loaded = findLoadedClass(name);
                                if (loaded == null) {
                                    loaded = findClass(name);
                                }
                                if (resolve) {
                                    resolveClass(loaded);
                                }
                                return loaded;
                            }
                        };
                loaders.add(loader);
                assertThat(loader.loadClass(facetType.getName())).isSameAs(facetType);
                assertThat(loader.loadClass(identifierType.getName())).isSameAs(identifierType);
                Class<?> identifiers =
                        loader.loadClass(shaded + API + "internal.LineageIdentifiers");
                assertThat(identifiers.getClassLoader()).isSameAs(loader);
                Object resource =
                        identifiers
                                .getMethod("pubSubTopic", String.class, String.class)
                                .invoke(null, "project", "topic");
                assertThat(identifierType.cast(resource).getClass()).isSameAs(identifierType);
                Class<?> helper = loader.loadClass(shaded + API + "internal.Lineage");
                LineageVertex vertex =
                        (LineageVertex)
                                helper.getMethod("sink", List.class)
                                        .invoke(null, List.of(resource));
                LineageDatasetFacet facet = vertex.datasets().get(0).facets().get("gcp");
                assertThat(facetType.cast(facet).getClass()).isSameAs(facetType);
                List<?> resources = (List<?>) facetType.getMethod("resources").invoke(facet);
                assertThat(resources).hasSize(1);
                Object retained = identifierType.cast(resources.get(0));
                assertThat(identifierType.getMethod("namespace").invoke(retained))
                        .isEqualTo("pubsub");
                assertThat(identifierType.getMethod("name").invoke(retained))
                        .isEqualTo("topic:project:topic");
                assertThat(identifierType.getMethod("identity").invoke(retained))
                        .isEqualTo(Map.of("project", "project", "topic", "topic"));
            }
        } finally {
            for (URLClassLoader loader : loaders) {
                loader.close();
            }
        }
    }

    private static List<Path> sqlJars(Path current) throws IOException {
        String manifest = System.getProperty("gcp.lineage.sql-jar-manifest");
        if (manifest == null) {
            return List.of(current);
        }
        // Only the invoking full-build measurement knows which artifacts it just built.
        // Never discover sibling targets: they may be outside a scoped reactor or stale.
        List<Path> jars = new ArrayList<>();
        for (String path : Files.readAllLines(Path.of(manifest))) {
            jars.add(Path.of(path).toRealPath());
        }
        assertThat(jars).hasSize(5).doesNotHaveDuplicates();
        assertThat(jars.stream().map(path -> path.getParent().getParent().getFileName().toString()))
                .containsExactlyInAnyOrder(
                        "flink-sql-connector-gcp-bigquery",
                        "flink-sql-connector-gcp-bigtable",
                        "flink-sql-connector-gcp-cloudtasks",
                        "flink-sql-connector-gcp-pubsub",
                        "flink-sql-connector-gcp-spanner");
        return jars;
    }
}
