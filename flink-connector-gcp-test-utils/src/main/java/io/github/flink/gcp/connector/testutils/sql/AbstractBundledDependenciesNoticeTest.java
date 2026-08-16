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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Keeps a {@code flink-sql-connector-gcp-*} module's {@code META-INF/NOTICE} honest: it must list
 * every artifact the uber-jar bundles, and nothing it does not.
 *
 * <p>This is the check that makes a {@code libraries-bom} bump fail the build: {@code artifactSet}
 * is {@code *:*}, so a new transitive is bundled automatically and the only thing that can go
 * unrecorded is the paperwork. Both failure directions — an undeclared bundled dependency and a
 * stale NOTICE entry for something no longer bundled — are caught here.
 *
 * <p>Reads the dependency list {@code maven-dependency-plugin} records at {@code
 * generate-test-resources}, so it describes this build rather than a remembered one. That list is
 * {@code includeScope=runtime}, so an artifact the module deliberately keeps out of the bundle by
 * declaring it {@code provided} is absent from both sides and needs no exemption here.
 *
 * <p>{@code scripts/check-notice.py} checks this same property and more — it also compares the
 * licence each artifact is filed under against the one its POM declares, and verifies the {@code
 * META-INF/licenses/} files. The overlap is deliberate: that comparison is a Python script rather
 * than a Maven plugin, so it runs as its own CI step, and this test is what makes the same drift
 * fail inside {@code just verify}, where a developer sees it immediately. (The half that *resolves*
 * the licences is bound into the build, so both are current for the same commit.)
 */
@Internal
public abstract class AbstractBundledDependenciesNoticeTest {

    /** This project's own artifacts are bundled but are not a third-party licence obligation. */
    private static final String OWN_GROUP_ID = "io.github.flink-gcp";

    /**
     * {@code groupId:artifactId:type:version}, as {@code dependency:list} prints it.
     *
     * <p>A classified artifact ({@code g:a:jar:linux-x86_64:2.0}) would put the classifier in the
     * version group. Nothing in these trees carries one; if that changes, the mismatched coordinate
     * fails the comparison below rather than slipping through, so the wrong shape is loud.
     */
    private static final Pattern COORDINATES =
            Pattern.compile(
                    "([A-Za-z0-9_.\\-]+):([A-Za-z0-9_.\\-]+):[A-Za-z0-9_.\\-]+"
                            + ":([A-Za-z0-9_.+\\-]+)");

    /** {@code dependency:list} colourises its output when the build is attached to a terminal. */
    private static final Pattern ANSI = Pattern.compile("\\[[0-9;]*m");

    /**
     * A floor below which both parsed lists are assumed to be a parsing failure rather than a real
     * bundle.
     *
     * <p>Per module rather than shared, because a shared floor is only as strong as the smallest
     * tree it has to admit: at 40 it would be vacuous for a tree three times that size, which is
     * the size of the second one. Set it a little below the current count — it exists to catch "the
     * regex matched nothing", not to pin the dependency count, which the comparison itself already
     * does exactly.
     */
    protected abstract int minimumBundledArtifacts();

    @Test
    void theNoticeListsExactlyTheArtifactsTheJarBundles() throws IOException {
        // Both sides are parsed with a pattern, and two empty sets would satisfy the comparison
        // below while proving nothing.
        assertThat(bundledArtifacts())
                .as("parsed from %s", runtimeDependencies())
                .hasSizeGreaterThan(minimumBundledArtifacts());
        assertThat(noticedArtifacts())
                .as("parsed from %s", notice())
                .hasSizeGreaterThan(minimumBundledArtifacts());

        assertThat(noticedArtifacts())
                .as(
                        "META-INF/NOTICE must match the bundle exactly. Regenerate it with"
                                + " `just update-notice <module>`, and for anything new confirm the"
                                + " licence against the artifact's own pom before grouping it — the"
                                + " generated META-INF/DEPENDENCIES lists licences pre-mediation, so"
                                + " its versions can differ from what actually resolves. An artifact"
                                + " on the bundled side that the NOTICE omits may instead be one the"
                                + " pom deliberately excludes and a change has re-admitted: check the"
                                + " <exclusions> on the connector dependency before regenerating.")
                .containsExactlyInAnyOrderElementsOf(bundledArtifacts());
    }

    /** The runtime tree {@code maven-dependency-plugin} records during the build. */
    private static Path runtimeDependencies() {
        return ShadedJar.targetDir().resolve("runtime-dependencies.txt");
    }

    /** The checked-in NOTICE the shade plugin packages into the jar. */
    private static Path notice() {
        return ShadedJar.basedir()
                .resolve(Path.of("src", "main", "resources", "META-INF", "NOTICE"));
    }

    /** {@code groupId:artifactId:version} for every third-party artifact the shade plugin takes. */
    private static Set<String> bundledArtifacts() throws IOException {
        assertThat(runtimeDependencies())
                .as("written by maven-dependency-plugin during the build")
                .exists();

        return Files.readAllLines(runtimeDependencies(), StandardCharsets.UTF_8).stream()
                .map(line -> ANSI.matcher(line).replaceAll(""))
                .map(COORDINATES::matcher)
                .filter(Matcher::find)
                .map(m -> m.group(1) + ":" + m.group(2) + ":" + m.group(3))
                .filter(gav -> !gav.startsWith(OWN_GROUP_ID + ":"))
                .collect(Collectors.toSet());
    }

    /** Every artifact the NOTICE claims to bundle, from its {@code - g:a:v} bullet lines. */
    private static Set<String> noticedArtifacts() throws IOException {
        List<String> lines = Files.readAllLines(notice(), StandardCharsets.UTF_8);
        return lines.stream()
                .filter(line -> line.startsWith("- "))
                // Entries in the non-Apache groups carry a trailing licence-file pointer. The
                // trim also strips the CR a Windows checkout leaves on every line: without it the
                // Apache-group entries, which have nothing after the coordinate, would each keep a
                // trailing \r and mismatch. The repository has no .gitattributes, so that checkout
                // is the default one on Windows.
                .map(line -> line.substring(2).split(" ", 2)[0].trim())
                .collect(Collectors.toSet());
    }
}
