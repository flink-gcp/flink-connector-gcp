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

package io.github.flink.gcp.connector.testutils.sql;

import org.apache.flink.annotation.Internal;

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
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Asserts the shape of a {@code flink-sql-connector-gcp-*} uber-jar: that SQL can discover the
 * connector in it, that every runtime dependency actually made it in, and that no <em>class</em>
 * escaped relocation except those that deliberately did.
 *
 * <p>Classes, deliberately. Each tree also carries non-class resources that keep their original
 * paths — proto descriptors and a handful of files read by literal path — and those are surveyed in
 * the concrete subclass's javadoc, because the survey is a property of the tree rather than of the
 * check.
 *
 * <p>These are jar-content tests, not runtime ones — each module's smoke ITCase is what proves the
 * relocated classes actually work. Named {@code ITCase} in the subclass because they must run after
 * {@code package}, which the connector parent's surefire configuration arranges by running
 * everything that does not match {@code **}{@code /*Test.*} in the integration-test phase.
 */
@Internal
public abstract class AbstractSqlConnectorPackagingITCase {

    /**
     * Package roots every GCP uber-jar here leaves unrelocated, whichever tree it bundles.
     *
     * <p>{@code org/conscrypt/} ships native libraries whose names conscrypt derives from its own
     * package at load time, and maven-shade does not rename native resources. It is a leaf that
     * gRPC only picks up reflectively as an optional TLS provider, so it is left alone rather than
     * given the resource-renaming treatment {@code grpc-netty-shaded} gets in the poms.
     *
     * <p>The rest are annotation-only artifacts. Relocation exists to stop two versions of a
     * <em>behaving</em> class fighting over one name, and a duplicate annotation class is inert —
     * resolved by whichever loader wins and never invoked. For {@code javax/annotation/} it would
     * also mean rewriting a standard JSR package, which is a surprise nobody asked for.
     *
     * <p>This is the <em>intersection</em>, not the union: a package only one tree carries goes in
     * that module's {@link #additionalUnrelocatedPackages()}. An allow-list entry only ever
     * permits, so a vacuous one is not an error — it is a hole, silently exempting a package that
     * arrives later and should have been relocated. {@link
     * #everyExemptionOnTheAllowListIsInTheJar()} is what keeps this list honest in both directions.
     *
     * <p>Note what is <em>not</em> here: {@code io/grpc/netty/shaded/}. That package is relocated
     * like everything else, which is what lets these jars share a classpath with each other.
     */
    private static final List<String> COMMON_UNRELOCATED_PACKAGES =
            List.of(
                    "org/conscrypt/",
                    "javax/annotation/",
                    "org/jspecify/",
                    "org/codehaus/mojo/animal_sniffer/",
                    "android/annotation/");

    private static final String SERVICES = "META-INF/services/";

    private static final String FACTORY_SERVICE =
            SERVICES + "org.apache.flink.table.factories.Factory";

    /**
     * Service files allowed to register a relocated implementation under an interface that is not
     * relocated with it.
     *
     * <p>Netty's BlockHound integration is the one, and it is inert: {@code BlockHoundIntegration}
     * exists only when the BlockHound agent is on the classpath, which is a test-time choice, and
     * an agent that is there wants the integration for the netty this jar actually runs — the
     * relocated one.
     */
    private static final List<String> FOREIGN_INTERFACE_SERVICE_FILES =
            List.of(SERVICES + "reactor.blockhound.integration.BlockHoundIntegration");

    /** The jar under test, and the two constants that describe it. */
    protected abstract ShadedJar shadedJar();

    /** The {@code DynamicTableFactory} the SQL planner must find through the SPI file. */
    protected abstract String factoryClass();

    /**
     * The connector's own package root, plus any package only this module leaves unrelocated.
     *
     * <p>The connector's own code is not third-party and has nothing to collide with; it is this
     * jar's public surface. Anything else here is a package the module's pom deliberately does not
     * relocate and the sibling's tree does not contain.
     */
    protected abstract List<String> additionalUnrelocatedPackages();

    /** See {@link AbstractBundledDependenciesNoticeTest#minimumBundledArtifacts()}. */
    protected abstract int minimumBundledArtifacts();

    @Test
    void sqlCanDiscoverTheConnectorFactory() throws Exception {
        try (JarFile jar = open()) {
            assertThat(read(jar, FACTORY_SERVICE))
                    .as("the factory SPI file the SQL planner looks up")
                    .isNotNull()
                    .contains(factoryClass());
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
     * its original name when its package is on the allow-list.
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

        assertThat(artifacts)
                .as("parsed from %s", classpathFile)
                .hasSizeGreaterThan(minimumBundledArtifacts());

        List<String> allowList = unrelocatedAllowList();
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
                boolean unrelocated = allowList.stream().anyMatch(sample::startsWith);
                String expected = unrelocated ? sample : shadedJar().shadedPrefix() + sample;
                if (!present.contains(expected)) {
                    missing.add(artifact.getFileName() + " (looked for " + expected + ")");
                }
            }
        }

        assertThat(missing)
                .as(
                        "artifacts on the runtime classpath whose classes are not in the jar —"
                                + " almost always a dependency the tree gained and the relocation"
                                + " list did not, which fails at runtime with NoClassDefFoundError"
                                + " rather than at build time")
                .isEmpty();
    }

    @Test
    void everyBundledPackageIsRelocatedExceptTheDocumentedExemptions() throws Exception {
        List<String> allowList = unrelocatedAllowList();
        try (JarFile jar = open()) {
            assertThat(classEntries(jar))
                    .as("an empty or truncated jar would satisfy the emptiness check below")
                    .isNotEmpty();

            List<String> escaped =
                    classEntries(jar).stream()
                            .filter(name -> !name.startsWith(shadedJar().shadedPrefix()))
                            .filter(name -> allowList.stream().noneMatch(name::startsWith))
                            .collect(Collectors.toList());

            assertThat(escaped)
                    .as(
                            "classes outside the shaded prefix that are not on the exemption"
                                    + " list — a user's own copy of one of these would collide with"
                                    + " the bundle, which is the whole reason this jar relocates")
                    .isEmpty();
        }
    }

    /**
     * No exemption is dead, in either module.
     *
     * <p>The check above only ever <em>permits</em>, so an entry matching nothing is invisible
     * there while it silently exempts whatever arrives under that root later. That is not
     * hypothetical: the shared list was written as the union over both trees, which took {@code
     * org/checkerframework/} — a BigQuery-only artifact — out of the Pub/Sub module's check, where
     * a {@code libraries-bom} bump could then have shipped it unrelocated.
     */
    @Test
    void everyExemptionOnTheAllowListIsInTheJar() throws Exception {
        List<String> classes;
        try (JarFile jar = open()) {
            classes = classEntries(jar);
        }

        assertThat(unrelocatedAllowList())
                .as(
                        "package roots this module exempts from relocation but does not bundle."
                                + " A shared entry only one tree carries belongs in that module's"
                                + " additionalUnrelocatedPackages() instead")
                .allSatisfy(
                        exemption ->
                                assertThat(classes)
                                        .as("classes under %s", exemption)
                                        .anyMatch(name -> name.startsWith(exemption)));

        // The service-file exemption is the same kind of statement and needs the same guard: an
        // entry naming a file no longer in the jar is a standing permission for that exact name.
        List<String> entries;
        try (JarFile jar = open()) {
            entries = entryNames(jar);
        }
        assertThat(FOREIGN_INTERFACE_SERVICE_FILES)
                .as("service files this module exempts but does not ship")
                .allSatisfy(exemption -> assertThat(entries).contains(exemption));
    }

    /**
     * No service file hands a relocated implementation to an interface that stayed put.
     *
     * <p>An SPI whose interface relocates with it moves as a pair and is invisible outside this
     * jar. One whose interface does not — a JDK type, or anything else on the deployment's
     * classpath — registers this jar's private copy <em>globally</em>: the file name is the name
     * everything else looks up. These jars are built for Flink's {@code lib/}, so "everything else"
     * is Flink and every job on the TaskManager.
     *
     * <p>Measured, not hypothetical: google-cloud-storage brings jackson-dataformat-xml and
     * Woodstox, whose three {@code javax.xml.stream.*Factory} files made the BigQuery jar the JVM's
     * StAX provider, and threeten-extra's {@code java.time.chrono.Chronology} added nine
     * chronologies to every caller of {@code getAvailableChronologies()}. Both are filtered out in
     * that module's pom. Nothing else in the packaging suite looks at resources, which is why this
     * one is here rather than being left to the escaped-classes check.
     */
    @Test
    void noServiceFileRegistersARelocatedImplementationUnderAnUnrelocatedInterface()
            throws Exception {
        String packagePrefix = shadedJar().shadedPackagePrefix();
        List<String> leaking = new ArrayList<>();
        try (JarFile jar = open()) {
            for (String name : entryNames(jar)) {
                if (!name.startsWith(SERVICES)
                        || name.equals(SERVICES)
                        || name.startsWith(SERVICES + packagePrefix)
                        || FOREIGN_INTERFACE_SERVICE_FILES.contains(name)) {
                    continue;
                }
                String body = read(jar, name);
                if (body != null && body.contains(packagePrefix)) {
                    leaking.add(name);
                }
            }
        }

        assertThat(leaking)
                .as(
                        "service files naming a relocated implementation under an interface this"
                                + " jar does not own — every JVM sharing a classpath with this jar"
                                + " would get the bundled copy as its provider. Filter the file out"
                                + " in the module's shade configuration, or add it above with the"
                                + " reason it is inert")
                .isEmpty();
    }

    @Test
    void nettyNativeLibrariesAreRenamedToMatchTheirRelocatedPackage() throws Exception {
        // Netty computes the library name at runtime from the package prefix of its own
        // NativeLibraryLoader, dots replaced by underscores. Derived here rather than written out,
        // so the pom's META-INF/native relocations and this assertion cannot drift apart: change
        // the shaded prefix without changing them and this fails.
        String nettyPrefix =
                shadedJar().shadedPrefix().replace('/', '_') + "io_grpc_netty_shaded_netty";

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
                                        // path relocations in the pom are needed rather than just
                                        // the `lib` one.
                                        .matches(
                                                "META-INF/native/(lib)?"
                                                        + Pattern.quote(nettyPrefix)
                                                        + ".*"));
    }

    @Test
    void theRelocatedGrpcServiceFileNamesTheRelocatedNettyProvider() throws Exception {
        // gRPC finds its transport through this SPI file. Both the file name and the class it
        // names have to land on the same side of the relocation; a mismatch is the
        // "NettyChannelProvider not a subtype" failure, which only shows up when a channel is
        // actually opened. Both are derived from the shaded prefix for the netty-native reason.
        String serviceFile =
                "META-INF/services/"
                        + shadedJar().shadedPackagePrefix()
                        + "io.grpc.ManagedChannelProvider";

        try (JarFile jar = open()) {
            assertThat(read(jar, serviceFile))
                    .isNotNull()
                    .contains(
                            shadedJar().shadedPackagePrefix()
                                    + "io.grpc.netty.shaded.io.grpc.netty.NettyChannelProvider");
        }
    }

    @Test
    void theNoticeDoesNotClaimApacheProvenance() throws Exception {
        try (JarFile jar = open()) {
            String notice = read(jar, "META-INF/NOTICE");

            assertThat(notice).isNotNull();

            // The transformer's own header block, located rather than assumed at an offset: it
            // opens with a banner naming the project, then the project/copyright pair, then the
            // organization. Everything after it is the bundled artifacts' own NOTICEs, several of
            // which legitimately carry ASF copyright lines, so a substring of the whole file is
            // either weak or false for a reason unrelated to what this guards.
            List<String> head = notice.lines().limit(12).collect(Collectors.toList());
            int project = head.indexOf("GCP Connectors for Apache Flink");
            assertThat(project)
                    .as("the transformer's projectName line, in %s", head)
                    .isGreaterThanOrEqualTo(0);
            // The year is a *range* from the inception year once the two differ, which they do
            // from the January after it: pinning the single year would fail both SQL modules on a
            // date, and repairing that by loosening the line would take the holder with it — the
            // holder being the whole point, since the parent's defaults name the ASF.
            assertThat(head.get(project + 1))
                    .as("Copyright <inception year, or a range from it> <organization>")
                    .matches("Copyright 2026(-\\d{4})? laughingman7743");
            assertThat(head)
                    .as("the header block must not name the ASF as this artifact's origin")
                    .noneMatch(line -> line.contains("The Apache Software Foundation"));
        }
    }

    /**
     * This project's own licence reached the jar.
     *
     * <p>Shade takes the project jar first, so the {@code META-INF/LICENSE} that survives is the
     * one maven-remote-resources generates from the repository root — not a bundled artifact's.
     * That is worth an assertion rather than a comment because the path looks exactly like the
     * dependency clutter the filters above remove, and #290 removed it on that reading: for one
     * commit the two jars a user downloads directly were the only artifacts this build produces
     * carrying no licence, while two lines of the packaged NOTICE went on pointing at an
     * "accompanying LICENSE file". Apache-2.0 section 4(a) asks a redistributor for a copy of the
     * licence, and this is that copy.
     */
    @Test
    void theProjectsOwnLicenceIsInTheJar() throws Exception {
        // The repository root, one level above any module. Compared rather than merely required to
        // exist: a dependency's copy at the same path would satisfy presence and say the wrong
        // thing about who licences this artifact.
        String own =
                Files.readString(
                        ShadedJar.basedir().getParent().resolve("LICENSE"), StandardCharsets.UTF_8);

        try (JarFile jar = open()) {
            assertThat(read(jar, "META-INF/LICENSE"))
                    .as("the licence this artifact is redistributed under")
                    .isEqualTo(own);
        }
    }

    /**
     * The checked-in NOTICE reached the jar.
     *
     * <p>What ships is not that file: {@code ApacheNoticeResourceTransformer} wraps it in a header
     * and appends every bundled artifact's own NOTICE — 333 lines against the checked-in 146 for
     * the BigQuery bundle. It does embed it verbatim (measured 2026-08-06), which is what makes
     * this assertion a containment rather than a comparison.
     *
     * <p>Everything else that checks a NOTICE reads the source tree: {@code
     * AbstractBundledDependenciesNoticeTest} and {@code scripts/check-notice.py} both hold {@code
     * src/main/resources/META-INF/NOTICE} to the resolved bundle, and neither opens the artifact.
     * So without this, a shade filter excluding {@code META-INF/NOTICE} — a plausible edit beside
     * the {@code META-INF/LICENSE.txt} and {@code META-INF/DEPENDENCIES} ones already there — would
     * ship a jar carrying no dependency list at all, with every check green.
     */
    @Test
    void theCheckedInNoticeIsInTheJar() throws Exception {
        String checkedIn =
                Files.readString(
                                ShadedJar.basedir()
                                        .resolve(
                                                Path.of(
                                                        "src",
                                                        "main",
                                                        "resources",
                                                        "META-INF",
                                                        "NOTICE")),
                                StandardCharsets.UTF_8)
                        .strip();
        assertThat(checkedIn).as("the module's checked-in NOTICE").isNotEmpty();

        try (JarFile jar = open()) {
            assertThat(read(jar, "META-INF/NOTICE"))
                    .as(
                            "the packaged NOTICE must carry the checked-in one — it is the only"
                                    + " document naming what the jar bundles, and every other check"
                                    + " of it reads the source tree")
                    .isNotNull()
                    .contains(checkedIn);
        }
    }

    /**
     * Every licence text the module checked in reached the jar.
     *
     * <p>Read from the source directory rather than listed per module, so it is the whole set and
     * stays current: {@code scripts/check-notice.py} holds *that directory* to the bundle in both
     * directions, against the pinned sources — but it reads the source tree and never opens the
     * built artifact, so it is this assertion and nothing else that connects the two. A shade
     * filter broad enough to swallow the directory is the way that breaks, and this PR widened one.
     */
    @Test
    void everyCheckedInLicenceTextIsPackaged() throws Exception {
        Path licences =
                ShadedJar.basedir()
                        .resolve(Path.of("src", "main", "resources", "META-INF", "licenses"));
        List<String> expected;
        try (Stream<Path> files = Files.list(licences)) {
            expected =
                    files.filter(Files::isRegularFile)
                            .map(file -> "META-INF/licenses/" + file.getFileName())
                            .sorted()
                            .collect(Collectors.toList());
        }
        assertThat(expected).as("licence texts checked in under %s", licences).isNotEmpty();

        try (JarFile jar = open()) {
            assertThat(entryNames(jar))
                    .as("the licence texts the NOTICE points at")
                    .containsAll(expected);
        }
    }

    private List<String> unrelocatedAllowList() {
        return Stream.concat(
                        additionalUnrelocatedPackages().stream(),
                        COMMON_UNRELOCATED_PACKAGES.stream())
                .collect(Collectors.toList());
    }

    private JarFile open() throws IOException {
        return new JarFile(shadedJar().path().toFile());
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
        List<String> names = new ArrayList<>();
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
