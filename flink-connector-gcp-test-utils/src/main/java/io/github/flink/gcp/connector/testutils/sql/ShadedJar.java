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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Locates the uber-jar a {@code flink-sql-connector-gcp-*} module builds, for the tests that
 * inspect or run against it.
 *
 * <p>The two things that differ between those modules — the artifact id and the prefix every
 * bundled package is relocated under — are this type's whole state; everything else about finding
 * and describing the jar is the same for all of them.
 */
@Internal
public final class ShadedJar {

    private final String artifactId;

    private final String shadedPrefix;

    private ShadedJar(String artifactId, String shadedPrefix) {
        this.artifactId = artifactId;
        this.shadedPrefix = shadedPrefix;
    }

    /**
     * @param artifactId the module's artifact id, e.g. {@code flink-sql-connector-gcp-pubsub}
     * @param shadedPrefix the relocation prefix in slash form with a trailing slash, e.g. {@code
     *     io/github/flink/gcp/connector/pubsub/shaded/}
     */
    public static ShadedJar of(String artifactId, String shadedPrefix) {
        if (!shadedPrefix.endsWith("/")) {
            throw new IllegalArgumentException(
                    "The shaded prefix is a jar-entry path and must end with '/': " + shadedPrefix);
        }
        // Netty derives its native library name from the package prefix of its own
        // NativeLibraryLoader, mapping `_` to `_1` and then `.` to `_`. A prefix carrying an
        // underscore would therefore have to be spelled `_1` in the pom's META-INF/native
        // relocations and in every assertion derived from it here — a rule stated in each SQL
        // module's pom and, until this check, enforced by nothing. Rejected rather than encoded:
        // no prefix in this repository needs one.
        if (shadedPrefix.indexOf('_') >= 0) {
            throw new IllegalArgumentException(
                    "The shaded prefix must not contain an underscore, or netty's mangled"
                            + " native-library name would have to spell it `_1`: "
                            + shadedPrefix);
        }
        return new ShadedJar(artifactId, shadedPrefix);
    }

    /** The prefix this module's relocations move every bundled package under, in slash form. */
    public String shadedPrefix() {
        return shadedPrefix;
    }

    /** The same prefix in dot form, as it appears in class names and SPI file names. */
    public String shadedPackagePrefix() {
        return shadedPrefix.replace('/', '.');
    }

    /**
     * Returns the shaded jar in {@code target/}.
     *
     * <p>Found by listing rather than by interpolating {@code ${project.version}}, so a version
     * bump does not have to be made here too. The integration-test phase runs after {@code
     * package}, so the jar is present by the time any caller runs.
     *
     * <p>The shade plugin leaves the pre-shade jar beside the shaded one as {@code
     * original-<name>.jar}. It is excluded by the {@code startsWith} below rather than by a rule of
     * its own — worth knowing before reordering that predicate, because nothing else keeps the
     * unshaded jar out and every assertion built on this would then be about the wrong file.
     *
     * <p>Two matches means a stale jar from an earlier version is still in {@code target/}; run
     * {@code mvn clean}.
     */
    public Path path() throws IOException {
        try (Stream<Path> entries = Files.list(targetDir())) {
            List<Path> candidates =
                    entries.filter(
                                    p -> {
                                        String name = p.getFileName().toString();
                                        return name.startsWith(artifactId + "-")
                                                && name.endsWith(".jar")
                                                && !name.endsWith("-sources.jar")
                                                && !name.endsWith("-tests.jar")
                                                && !name.endsWith("-javadoc.jar");
                                    })
                            .collect(Collectors.toList());
            if (candidates.size() != 1) {
                throw new IllegalStateException(
                        "Expected exactly one shaded jar in target/, found "
                                + candidates
                                + " — more than one usually means a stale jar from an earlier"
                                + " version; run `mvn clean`");
            }
            return candidates.get(0).normalize();
        }
    }

    /**
     * The calling module's base directory.
     *
     * <p>Resolved from the {@code project.basedir} system property the connector parent's surefire
     * configuration sets, rather than from the working directory: the integration-test execution
     * forks, and a relative path does not resolve there. Normalized, because {@link
     * Path#toAbsolutePath()} does not remove a leading {@code .} and {@link Path#equals} compares
     * name elements — so an unnormalized fallback path could never equal a code-source location it
     * in fact points at.
     */
    public static Path basedir() {
        return Paths.get(System.getProperty("project.basedir", ".")).toAbsolutePath().normalize();
    }

    /** The calling module's {@code target/} directory. */
    public static Path targetDir() {
        return basedir().resolve("target");
    }
}
