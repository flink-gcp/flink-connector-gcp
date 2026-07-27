/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.pubsub.sql;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Asserts the shape of the shaded jar: that SQL can discover the connector in it, that every
 * runtime dependency actually made it in, and that no <em>class</em> escaped relocation except
 * those that deliberately did.
 *
 * <p>Classes, deliberately. Around 128 non-class resources keep their original paths, and are not
 * checked here: the {@code google/**}{@code /*.proto} descriptors from {@code
 * proto-google-common-protos}, the {@code META-INF/native-image/} configuration, the {@code .java}
 * sources {@code jsr305} ships beside its classes, and two that are load-bearing where they are —
 * {@code mozilla/public-suffix-list.txt}, which httpclient reads by literal path, and gax's
 * root-level {@code dependencies.properties}, whose lookup carries no package for shade to rewrite.
 * The proto descriptors are inert for a Java runtime (protobuf reads descriptors compiled into the
 * generated classes, not these files), which is why every surveyed uber-jar leaves them alone.
 * {@code dependencies.properties} is the one with a real, if cosmetic, collision surface: another
 * jar in {@code lib/} shipping one would feed a foreign version string into the relocated gax's
 * {@code x-goog-api-client} header.
 *
 * <p>This is a jar-content test, not a runtime one — {@link PubSubSqlConnectorSmokeITCase} is what
 * proves the relocated classes actually work. Named {@code ITCase} because it must run after {@code
 * package}, which the connector parent's surefire configuration arranges by running everything that
 * does not match {@code **}{@code /*Test.*} in the integration-test phase.
 */
class PubSubSqlConnectorPackagingITCase {

    /**
     * Package roots allowed to appear outside {@link ShadedJar#SHADED_PREFIX}.
     *
     * <p>{@code org/conscrypt/} ships native libraries whose names conscrypt derives from its own
     * package at load time, and maven-shade does not rename native resources. It is a leaf that
     * gRPC only picks up reflectively as an optional TLS provider, so it is left alone rather than
     * given the resource-renaming treatment {@code grpc-netty-shaded} gets in the pom.
     *
     * <p>The four annotation packages are here because relocation exists to stop two versions of a
     * behaving class fighting over one name, and a duplicate annotation class is inert. The first
     * entry is this module's own connector code, which is not third-party and has nothing to
     * collide with.
     *
     * <p>Note what is <em>not</em> here: {@code io/grpc/netty/shaded/}. That package is relocated
     * like everything else, which is what lets this jar share a classpath with another that bundles
     * gRPC.
     */
    private static final List<String> UNRELOCATED_ALLOW_LIST =
            List.of(
                    "io/github/flink/gcp/connector/pubsub/",
                    "org/conscrypt/",
                    "javax/annotation/",
                    "org/jspecify/",
                    "org/codehaus/mojo/animal_sniffer/",
                    "android/annotation/");

    private static final String FACTORY_SERVICE =
            "META-INF/services/org.apache.flink.table.factories.Factory";

    private static final String FACTORY_CLASS =
            "io.github.flink.gcp.connector.pubsub.table.PubSubDynamicTableFactory";

    /** The relocated name the {@code ServicesResourceTransformer} must have rewritten it to. */
    private static final String GRPC_CHANNEL_PROVIDER_SERVICE =
            "META-INF/services/io.github.flink.gcp.connector.pubsub.shaded.io.grpc"
                    + ".ManagedChannelProvider";

    @Test
    void sqlCanDiscoverTheConnectorFactory() throws Exception {
        try (JarFile jar = open()) {
            assertThat(read(jar, FACTORY_SERVICE))
                    .as("the factory SPI file the SQL planner looks up")
                    .isNotNull()
                    .contains(FACTORY_CLASS);
        }
    }

    /**
     * Every artifact on the runtime classpath actually contributed its classes to the jar.
     *
     * <p>With {@code artifactSet} at {@code *:*} the bundle tracks the runtime classpath by
     * construction, so this is a guard against regression rather than against routine drift: a
     * reintroduced include list, or a shade filter broad enough to swallow an artifact whole, fails
     * here with the artifact's name. When it was an enumerated list, dropping {@code
     * io.grpc:grpc-xds} removed 4143 classes while every other assertion stayed green — they all
     * check that nothing extra is present, not that anything in particular is — which is why this
     * test exists and why the list does not.
     *
     * <p>Checked by sampling one class per artifact and looking for it at its relocated name, or at
     * its original name when its package is on the allow-list above.
     */
    @Test
    void everyRuntimeDependencyContributedItsClassesToTheJar() throws Exception {
        Path classpathFile = ShadedJar.targetDir().resolve("runtime-classpath.txt");
        assertThat(classpathFile)
                .as("written by maven-dependency-plugin during the build")
                .exists();

        List<Path> artifacts =
                Arrays.stream(
                                Files.readString(classpathFile, StandardCharsets.UTF_8)
                                        .trim()
                                        .split(Pattern.quote(File.pathSeparator)))
                        .filter(entry -> !entry.isBlank())
                        .map(Paths::get)
                        .filter(path -> path.getFileName().toString().endsWith(".jar"))
                        .collect(Collectors.toList());

        assertThat(artifacts).as("parsed from %s", classpathFile).hasSizeGreaterThan(40);

        List<String> missing = new ArrayList<>();
        try (JarFile uberJar = open()) {
            Set<String> present = new HashSet<>(entryNames(uberJar));
            for (Path artifact : artifacts) {
                String sample = sampleClassEntry(artifact);
                if (sample == null) {
                    // Legitimately class-free: com.google.guava:listenablefuture ships an empty
                    // jar purely to win version mediation against guava.
                    continue;
                }
                boolean unrelocated = UNRELOCATED_ALLOW_LIST.stream().anyMatch(sample::startsWith);
                String expected = unrelocated ? sample : ShadedJar.SHADED_PREFIX + sample;
                if (!present.contains(expected)) {
                    missing.add(artifact.getFileName() + " (looked for " + expected + ")");
                }
            }
        }

        assertThat(missing)
                .as(
                        "artifacts on the runtime classpath whose classes are not in the jar —"
                                + " almost always a dependency the tree gained and"
                                + " artifactSet/includes did not, which fails at runtime with"
                                + " NoClassDefFoundError rather than at build time")
                .isEmpty();
    }

    @Test
    void everyBundledPackageIsRelocatedExceptTheDocumentedExemptions() throws Exception {
        try (JarFile jar = open()) {
            assertThat(classEntries(jar))
                    .as("an empty or truncated jar would satisfy the emptiness check below")
                    .isNotEmpty();

            List<String> escaped =
                    classEntries(jar).stream()
                            .filter(name -> !name.startsWith(ShadedJar.SHADED_PREFIX))
                            .filter(
                                    name ->
                                            UNRELOCATED_ALLOW_LIST.stream()
                                                    .noneMatch(name::startsWith))
                            .collect(Collectors.toList());

            assertThat(escaped)
                    .as(
                            "classes outside the shaded prefix that are not on the exemption"
                                    + " list — a user's own copy of one of these would collide with"
                                    + " the bundle, which is the whole reason this jar relocates")
                    .isEmpty();
        }
    }

    @Test
    void nettyNativeLibrariesAreRenamedToMatchTheirRelocatedPackage() throws Exception {
        // Netty computes the library name at runtime from the package prefix of its own
        // NativeLibraryLoader, dots replaced by underscores. Derived here rather than written out,
        // so the pom's rawString relocations and this assertion cannot drift apart: change the
        // shaded prefix without changing them and this fails.
        String nettyPrefix =
                ShadedJar.SHADED_PREFIX.replace('/', '_') + "io_grpc_netty_shaded_netty";

        List<String> nativeEntries;
        try (JarFile jar = open()) {
            nativeEntries =
                    entryNames(jar).stream()
                            .filter(name -> name.startsWith("META-INF/native/"))
                            .filter(name -> name.contains("netty"))
                            .collect(Collectors.toList());
        }

        assertThat(nativeEntries)
                .as("the relocated netty transport's native libraries")
                .isNotEmpty()
                .allSatisfy(
                        name ->
                                assertThat(name)
                                        .as(
                                                "a native library still under gRPC's own shaded"
                                                        + " name would never be found by the relocated"
                                                        + " loader — netty falls back to NIO silently"
                                                        + " rather than failing, so nothing else"
                                                        + " catches this")
                                        // Windows DLLs carry no `lib` prefix, which is why both
                                        // rawString relocations in the pom are needed rather than
                                        // just the `lib` one.
                                        .matches(
                                                "META-INF/native/(lib)?"
                                                        + Pattern.quote(nettyPrefix)
                                                        + ".*"));
    }

    @Test
    void theRelocatedGrpcServiceFileNamesTheRelocatedNettyProvider() throws Exception {
        try (JarFile jar = open()) {
            // gRPC finds its transport through this SPI file. Both the file name and the class it
            // names have to land on the same side of the relocation; a mismatch is the
            // "NettyChannelProvider not a subtype" failure, which only shows up when a channel is
            // actually opened.
            assertThat(read(jar, GRPC_CHANNEL_PROVIDER_SERVICE))
                    .isNotNull()
                    .contains(
                            ShadedJar.SHADED_PREFIX.replace('/', '.')
                                    + "io.grpc.netty.shaded.io.grpc.netty.NettyChannelProvider");
        }
    }

    @Test
    void theNoticeDoesNotClaimApacheProvenance() throws Exception {
        try (JarFile jar = open()) {
            String notice = read(jar, "META-INF/NOTICE");

            assertThat(notice).isNotNull();

            // Scoped to the header block the transformer generates, NOT the whole file: it
            // aggregates every bundled artifact's own NOTICE, several of which legitimately carry
            // ASF copyright lines, and one bundling a component with a 2006 inception year would
            // fail a whole-file check for a reason unrelated to what this guards.
            String header = notice.substring(0, Math.min(notice.length(), 600));
            assertThat(header)
                    .as("the transformer's own header block")
                    .contains("GCP Connectors for Apache Flink\nCopyright 2026 laughingman7743")
                    // The transformer defaults name the ASF and an inception year of 2006, and
                    // setting only <projectName> leaves both in place.
                    .doesNotContain("Copyright 2006")
                    .doesNotContain("Copyright 2026 The Apache Software Foundation");

            assertThat(entryNames(jar))
                    .as("the licence texts the NOTICE points at")
                    .contains(
                            "META-INF/licenses/LICENSE.protobuf",
                            "META-INF/licenses/LICENSE.javax-annotation-api");
        }
    }

    private static JarFile open() throws IOException {
        return new JarFile(ShadedJar.path().toFile());
    }

    /**
     * One class entry from {@code artifact} that shading would carry across unchanged apart from
     * its package, or null if it has none.
     *
     * <p>{@code module-info} is excluded because the shade filters drop it, and {@code META-INF}
     * because multi-release copies live there under a versioned path that relocation does not
     * mirror.
     */
    private static String sampleClassEntry(Path artifact) throws IOException {
        try (JarFile jar = new JarFile(artifact.toFile())) {
            return entryNames(jar).stream()
                    .filter(name -> name.endsWith(".class"))
                    .filter(name -> !name.startsWith("META-INF/"))
                    .filter(name -> !name.endsWith("module-info.class"))
                    .findFirst()
                    .orElse(null);
        }
    }

    private static List<String> entryNames(JarFile jar) {
        List<String> names = new java.util.ArrayList<>();
        Enumeration<JarEntry> entries = jar.entries();
        while (entries.hasMoreElements()) {
            names.add(entries.nextElement().getName());
        }
        return names;
    }

    private static List<String> classEntries(JarFile jar) {
        return entryNames(jar).stream()
                .filter(name -> name.endsWith(".class"))
                .collect(Collectors.toList());
    }

    private static String read(JarFile jar, String entryName) throws IOException {
        JarEntry entry = jar.getJarEntry(entryName);
        if (entry == null) {
            return null;
        }
        try (InputStream in = jar.getInputStream(entry)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
