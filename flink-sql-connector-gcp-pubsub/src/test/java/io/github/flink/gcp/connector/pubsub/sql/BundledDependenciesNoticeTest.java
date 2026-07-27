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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Keeps {@code META-INF/NOTICE} honest: it must list every artifact the uber-jar bundles, and
 * nothing it does not.
 *
 * <p>This is the check that makes a {@code libraries-bom} bump fail the build. The shade
 * configuration's enumerated {@code artifactSet/includes} does not, despite being the obvious place
 * to look: an artifact that appears in the dependency tree but not in that list is silently
 * <em>dropped</em> from the jar, and surfaces much later as a {@code NoClassDefFoundError} in
 * somebody's job. Both failure directions — an undeclared bundled dependency and a stale NOTICE
 * entry for something no longer bundled — are caught here instead.
 *
 * <p>Reads the dependency list {@code maven-dependency-plugin} records at {@code
 * generate-test-resources}, so it describes this build rather than a remembered one.
 */
class BundledDependenciesNoticeTest {

    private static final Path RUNTIME_DEPENDENCIES =
            Paths.get("target", "runtime-dependencies.txt");

    private static final Path NOTICE = Paths.get("src", "main", "resources", "META-INF", "NOTICE");

    /** This project's own artifacts are bundled but are not a third-party licence obligation. */
    private static final String OWN_GROUP_ID = "io.github.flink-gcp";

    /** {@code groupId:artifactId:type[:classifier]:version}, as {@code dependency:list} prints. */
    private static final Pattern COORDINATES =
            Pattern.compile(
                    "([A-Za-z0-9_.\\-]+):([A-Za-z0-9_.\\-]+):[A-Za-z0-9_.\\-]+"
                            + ":([A-Za-z0-9_.+\\-]+)");

    /** {@code dependency:list} colourises its output when the build is attached to a terminal. */
    private static final Pattern ANSI = Pattern.compile("\\[[0-9;]*m");

    @Test
    void theNoticeListsExactlyTheArtifactsTheJarBundles() throws IOException {
        // Both sides are parsed with a pattern, and two empty sets would satisfy the comparison
        // below while proving nothing. Neither list is anywhere near this small in practice, so
        // the floor only has to be low enough never to be the thing that fails.
        assertThat(bundledArtifacts())
                .as("parsed from %s", RUNTIME_DEPENDENCIES)
                .hasSizeGreaterThan(40);
        assertThat(noticedArtifacts()).as("parsed from %s", NOTICE).hasSizeGreaterThan(40);

        assertThat(noticedArtifacts())
                .as(
                        "META-INF/NOTICE must match the bundle exactly. Regenerate the lists from"
                                + " %s, and for anything new confirm the licence against the"
                                + " artifact's own pom before grouping it — the generated"
                                + " META-INF/DEPENDENCIES lists licences pre-mediation, so its"
                                + " versions can differ from what actually resolves.",
                        RUNTIME_DEPENDENCIES)
                .containsExactlyInAnyOrderElementsOf(bundledArtifacts());
    }

    /** {@code groupId:artifactId:version} for every third-party artifact the shade plugin takes. */
    private static Set<String> bundledArtifacts() throws IOException {
        assertThat(RUNTIME_DEPENDENCIES)
                .as("written by maven-dependency-plugin during the build")
                .exists();

        return Files.readAllLines(RUNTIME_DEPENDENCIES, StandardCharsets.UTF_8).stream()
                .map(line -> ANSI.matcher(line).replaceAll(""))
                .map(COORDINATES::matcher)
                .filter(Matcher::find)
                .map(m -> m.group(1) + ":" + m.group(2) + ":" + m.group(3))
                .filter(gav -> !gav.startsWith(OWN_GROUP_ID + ":"))
                .collect(Collectors.toSet());
    }

    /** Every artifact the NOTICE claims to bundle, from its {@code - g:a:v} bullet lines. */
    private static Set<String> noticedArtifacts() throws IOException {
        List<String> lines = Files.readAllLines(NOTICE, StandardCharsets.UTF_8);
        return lines.stream()
                .filter(line -> line.startsWith("- "))
                // Entries in the non-Apache groups carry a trailing licence-file pointer.
                .map(line -> line.substring(2).split(" ", 2)[0])
                .collect(Collectors.toSet());
    }
}
