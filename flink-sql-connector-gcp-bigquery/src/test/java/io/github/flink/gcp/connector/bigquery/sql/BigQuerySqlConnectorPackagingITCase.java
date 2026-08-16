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

package io.github.flink.gcp.connector.bigquery.sql;

import io.github.flink.gcp.connector.testutils.sql.AbstractSqlConnectorPackagingITCase;
import io.github.flink.gcp.connector.testutils.sql.ShadedJar;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Asserts the shape of this module's uber-jar. The checks are the shared ones; what is BigQuery's
 * own is the jar, the factory, and the one package root left unrelocated below.
 *
 * <p>The base class checks classes, deliberately. In this tree 172 non-class resources outside
 * {@code META-INF/} keep their original paths (measured 2026-08-06, against the Pub/Sub bundle's
 * 128 on the same measure): the {@code google/**}{@code /*.proto} descriptors from the proto
 * artifacts, the {@code .java} sources {@code jsr305} ships beside its classes, and two that are
 * load-bearing where they are — {@code mozilla/public-suffix-list.txt}, which httpclient reads by
 * literal path, and gax's root-level {@code dependencies.properties}, whose lookup carries no
 * package for shade to rewrite. The proto descriptors are inert for a Java runtime (protobuf reads
 * descriptors compiled into the generated classes, not these files), which is why every surveyed
 * uber-jar leaves them alone. {@code dependencies.properties} is the one with a real, if cosmetic,
 * collision surface: another jar in {@code lib/} shipping one would feed a foreign version string
 * into the relocated gax's {@code x-goog-api-client} header — and with the Pub/Sub uber-jar this is
 * no longer hypothetical, since both bundle gax and whichever loader wins decides.
 *
 * <p>{@link BigQuerySqlConnectorSmokeITCase} is what proves the relocated classes actually work.
 */
class BigQuerySqlConnectorPackagingITCase extends AbstractSqlConnectorPackagingITCase {

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
        // org/checkerframework/ is annotation-only and exempt for the reason the base class gives
        // for its own five, but it belongs here rather than there: only this tree carries it
        // (google-cloud-bigquery brings checker-compat-qual), and a shared entry the Pub/Sub jar
        // has no classes under would exempt a package that arrives there later.
        return List.of("io/github/flink/gcp/connector/bigquery/", "org/checkerframework/");
    }

    @Override
    protected int minimumBundledArtifacts() {
        return UberJar.MINIMUM_BUNDLED_ARTIFACTS;
    }

    /**
     * zstd-jni's native libraries must stay at the jar root, unrelocated, while its classes move
     * under the shaded prefix.
     *
     * <p>The base class checks classes, so nothing above would notice a shading change that moved
     * or dropped these — and the symptom would be an {@code UnsatisfiedLinkError} on a TaskManager
     * the first time a FILE_LOADS staging file is opened, not a build failure.
     *
     * <p>The expected path is <em>computed the way {@code Native.resourceName()} computes it</em> —
     * {@code "/" + osName() + "/" + os.arch + "/lib" + "zstd-jni-" + version + ext} — rather than
     * hardcoded, so this fails on whichever platform it runs on rather than passing everywhere
     * because one hardcoded entry happens to survive.
     */
    @Test
    void zstdNativeLibrariesStayAtTheJarRootWhileItsClassesAreRelocated() throws Exception {
        try (JarFile jar = new JarFile(shadedJar().path().toFile())) {
            List<String> names = names(jar);

            List<String> natives =
                    names.stream()
                            .filter(n -> n.matches(".*/libzstd-jni-[^/]+\\.(so|dylib|dll)"))
                            .collect(Collectors.toList());
            assertThat(natives)
                    .as("zstd-jni's per-platform native libraries")
                    .isNotEmpty()
                    .allSatisfy(
                            n ->
                                    assertThat(n)
                                            .as(
                                                    "a native library moved under the shaded"
                                                            + " prefix, where Native.resourceName()"
                                                            + " will not look for it")
                                            .doesNotStartWith(SHADED_PREFIX));

            assertThat(names)
                    .as("the native library for the platform this test runs on")
                    .contains(expectedNativeEntry(natives));

            assertThat(names)
                    .as("zstd-jni's classes, which must be relocated like every other bundle")
                    .anyMatch(n -> n.startsWith(SHADED_PREFIX + "com/github/luben/zstd/"))
                    .noneMatch(n -> n.startsWith("com/github/luben/zstd/"));
        }
    }

    private static final String SHADED_PREFIX = "io/github/flink/gcp/connector/bigquery/shaded/";

    /** The entry {@code com.github.luben.zstd.util.Native} will ask this JVM's classloader for. */
    private static String expectedNativeEntry(List<String> natives) {
        // Native.osName(), Native.libExtension() and Native.resourceName(), reproduced.
        String os = System.getProperty("os.name").toLowerCase().replace(' ', '_');
        String extension;
        if (os.startsWith("win")) {
            os = "win";
            extension = ".dll";
        } else if (os.startsWith("mac")) {
            os = "darwin";
            extension = ".dylib";
        } else {
            extension = ".so";
        }
        String arch = System.getProperty("os.arch");
        if (os.equals("darwin") && arch.equals("amd64")) {
            arch = "x86_64";
        }
        // Only the version is read off the bundle, so a dependency bump needs no edit here while a
        // bump that lost the natives still fails.
        String any = natives.get(0);
        String version =
                any.substring(
                        any.lastIndexOf("libzstd-jni-") + "libzstd-jni-".length(),
                        any.lastIndexOf('.'));
        return os + "/" + arch + "/libzstd-jni-" + version + extension;
    }

    private static List<String> names(JarFile jar) {
        List<String> names = new ArrayList<>();
        for (Enumeration<JarEntry> entries = jar.entries(); entries.hasMoreElements(); ) {
            names.add(entries.nextElement().getName());
        }
        return names;
    }
}
