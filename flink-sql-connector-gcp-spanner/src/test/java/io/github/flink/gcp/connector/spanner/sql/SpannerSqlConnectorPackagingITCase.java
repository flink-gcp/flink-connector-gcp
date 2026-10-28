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

package io.github.flink.gcp.connector.spanner.sql;

import org.apache.flink.metrics.Counter;

import io.github.flink.gcp.connector.testutils.sql.AbstractSqlConnectorPackagingITCase;
import io.github.flink.gcp.connector.testutils.sql.ShadedJar;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.jar.JarFile;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Asserts the shape of this module's uber-jar. The checks are the shared ones; what is Spanner's
 * own is the jar, the factory, and the two package roots left unrelocated below.
 *
 * <p>The base class checks classes, deliberately. In this tree around 160 non-class resources keep
 * their original paths: the {@code google/**}{@code /*.proto} and {@code grpc/**}{@code /*.proto}
 * descriptors (inert for a Java runtime — protobuf reads descriptors compiled into the generated
 * classes, not these files), the {@code .java} sources {@code jsr305} ships beside its classes,
 * {@code conscrypt.properties} under the deliberately unrelocated {@code org/conscrypt/}, and three
 * that are load-bearing where they are. The sibling jars carry the same pair — {@code
 * mozilla/public-suffix-list.txt}, which httpclient reads by literal path, and gax's root-level
 * {@code dependencies.properties}, whose lookup carries no package for shade to rewrite. Another
 * jar in {@code lib/} shipping either root-level file would shadow one copy, cosmetically for the
 * gax version header and materially for the public-suffix data.
 *
 * <p>{@link SpannerSqlConnectorSmokeITCase} is what proves the relocated classes actually work.
 */
class SpannerSqlConnectorPackagingITCase extends AbstractSqlConnectorPackagingITCase {

    @Test
    void changeStreamMetadataImplementationIsInTheShadedArtifact() throws Exception {
        try (JarFile jar = new JarFile(UberJar.SHADED.path().toFile())) {
            assertThat(
                            jar.getJarEntry(
                                    "io/github/flink/gcp/connector/spanner/table/source/ReadableMetadata.class"))
                    .isNotNull();
            assertThat(
                            jar.getJarEntry(
                                    "io/github/flink/gcp/connector/spanner/table/source/SpannerChangeStreamDynamicSource.class"))
                    .isNotNull();
        }
    }

    /** The catalog is found through the same SPI file as the table factory, by its own type. */
    @Test
    void sqlCanDiscoverTheCatalogFactory() throws Exception {
        try (JarFile jar = new JarFile(UberJar.SHADED.path().toFile())) {
            String services =
                    new String(
                            jar.getInputStream(
                                            jar.getJarEntry(
                                                    "META-INF/services/org.apache.flink.table.factories.Factory"))
                                    .readAllBytes(),
                            StandardCharsets.UTF_8);
            assertThat(services)
                    .contains(
                            "io.github.flink.gcp.connector.spanner.table.catalog.SpannerCatalogFactory");
        }
    }

    /**
     * The client metrics work inside the relocated artifact: base and OpenTelemetry are relocated
     * together, so the relocated {@code ClientMetrics} hands the relocated client's metrics
     * provider an {@code OpenTelemetry} of the type it takes, and a recording through that instance
     * reaches the Flink counter. Nothing here touches an unrelocated or global OpenTelemetry.
     */
    @Test
    void theRelocatedClientMetricsFeedTheRelocatedClientsMetricsProvider() throws Exception {
        String shaded = "io.github.flink.gcp.connector.spanner.shaded.";
        try (URLClassLoader loader = shadedFirst(shaded)) {
            Class<?> clientMetrics =
                    loader.loadClass(
                            shaded + "io.github.flink.gcp.connector.base.metrics.ClientMetrics");
            Object metrics = clientMetrics.getMethod("create", String.class).invoke(null, "method");
            Object openTelemetry = clientMetrics.getMethod("openTelemetry").invoke(metrics);
            Class<?> openTelemetryType =
                    loader.loadClass(shaded + "io.opentelemetry.api.OpenTelemetry");
            assertThat(openTelemetryType.getClassLoader()).isSameAs(loader);

            Class<?> provider =
                    loader.loadClass(
                            shaded + "com.google.cloud.spanner.CustomOpenTelemetryMetricsProvider");
            Object created =
                    provider.getMethod("create", openTelemetryType).invoke(null, openTelemetry);
            assertThat(provider.getMethod("getOpenTelemetry").invoke(created))
                    .isSameAs(openTelemetry);

            Counter counter =
                    (Counter)
                            clientMetrics
                                    .getMethod("counterSum", String.class, String.class)
                                    .invoke(metrics, "spanner/client/operation_count", "Spanner.X");
            Class<?> attributes =
                    loader.loadClass(shaded + "io.opentelemetry.api.common.Attributes");
            Class<?> attributeKey =
                    loader.loadClass(shaded + "io.opentelemetry.api.common.AttributeKey");
            Object key = attributeKey.getMethod("stringKey", String.class).invoke(null, "method");
            Object labels =
                    attributes
                            .getMethod("of", attributeKey, Object.class)
                            .invoke(null, key, "Spanner.X");
            Object meterProvider =
                    openTelemetryType.getMethod("getMeterProvider").invoke(openTelemetry);
            Object meter =
                    loader.loadClass(shaded + "io.opentelemetry.api.metrics.MeterProvider")
                            .getMethod("get", String.class)
                            .invoke(meterProvider, "gax-java");
            Object counterBuilder =
                    loader.loadClass(shaded + "io.opentelemetry.api.metrics.Meter")
                            .getMethod("counterBuilder", String.class)
                            .invoke(meter, "spanner/client/operation_count");
            Object longCounter =
                    loader.loadClass(shaded + "io.opentelemetry.api.metrics.LongCounterBuilder")
                            .getMethod("build")
                            .invoke(counterBuilder);
            loader.loadClass(shaded + "io.opentelemetry.api.metrics.LongCounter")
                    .getMethod("add", long.class, attributes)
                    .invoke(longCounter, 3L, labels);

            assertThat(counter.getCount()).isEqualTo(3);
        }
    }

    /** Loads the relocated packages from the artifact alone, and everything else as Flink would. */
    private static URLClassLoader shadedFirst(String shaded) throws Exception {
        return new URLClassLoader(
                new URL[] {UberJar.SHADED.path().toUri().toURL()},
                SpannerSqlConnectorPackagingITCase.class.getClassLoader()) {
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
    }

    @Override
    protected ShadedJar shadedJar() {
        return UberJar.SHADED;
    }

    @Override
    protected String factoryClass() {
        return UberJar.FACTORY_CLASS;
    }

    @Override
    protected List<String> additionalUnrelocatedPackages() {
        return List.of("io/github/flink/gcp/connector/spanner/");
    }

    @Override
    protected int minimumBundledArtifacts() {
        return UberJar.MINIMUM_BUNDLED_ARTIFACTS;
    }
}
