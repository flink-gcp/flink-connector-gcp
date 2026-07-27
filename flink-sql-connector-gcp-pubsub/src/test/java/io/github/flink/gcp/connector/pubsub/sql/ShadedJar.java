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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Locates the uber-jar this module builds, for the tests that inspect or run against it. */
final class ShadedJar {

    /** The prefix this module's relocations move every bundled package under. */
    static final String SHADED_PREFIX = "io/github/flink/gcp/connector/pubsub/shaded/";

    private static final String ARTIFACT_ID = "flink-sql-connector-gcp-pubsub";

    private ShadedJar() {}

    /**
     * Returns the shaded jar in {@code target/}.
     *
     * <p>Found by listing rather than by interpolating {@code ${project.version}}, so a version
     * bump does not have to be made here too. The integration-test phase runs after {@code
     * package}, so the jar is present by the time any caller runs.
     *
     * <p>Resolved against the {@code project.basedir} system property the connector parent's
     * surefire configuration sets, rather than against the working directory: the integration-test
     * execution forks, and a relative {@code target/} does not resolve there.
     *
     * <p>The {@code original-} prefixed sibling is the pre-shade jar the shade plugin leaves
     * behind; excluding it is what makes this the *shaded* artifact rather than whichever of the
     * two the filesystem happened to return first.
     */
    static Path path() throws IOException {
        Path target = Paths.get(System.getProperty("project.basedir", ".")).resolve("target");
        try (Stream<Path> entries = Files.list(target)) {
            List<Path> candidates =
                    entries.filter(
                                    p -> {
                                        String name = p.getFileName().toString();
                                        return name.startsWith(ARTIFACT_ID + "-")
                                                && name.endsWith(".jar")
                                                && !name.endsWith("-sources.jar")
                                                && !name.endsWith("-tests.jar")
                                                && !name.endsWith("-javadoc.jar");
                                    })
                            .collect(Collectors.toList());
            if (candidates.size() != 1) {
                throw new IllegalStateException(
                        "Expected exactly one shaded jar in target/, found " + candidates);
            }
            return candidates.get(0);
        }
    }
}
