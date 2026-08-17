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

package io.github.flink.gcp.connector.bigquery.table;

import org.apache.flink.table.api.DataTypes;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Which Flink the table tests are running against, where its behaviour differs across the supported
 * range.
 *
 * <p>The policy only, and shared, because a version-keyed expectation is exercised on one version
 * per build: whichever one is on the classpath. A copy of such a predicate is only ever tested
 * against the version in front of the author, so it cannot be caught being wrong until a different
 * version arrives. This one is a pure function of the version string as well, so {@code
 * FlinkVersionsTest} covers the whole supported range in every build.
 *
 * <p>That is not hypothetical. Both call sites here used to carry {@code
 * version.startsWith("2.3.")}, which is true of the ceiling and of nothing after it. It reached
 * main on 2026-08-13, and the first weekly run to evaluate it — 2026-08-16 — failed on the {@code
 * next} row's {@code 2.4-SNAPSHOT} (issue #933).
 */
final class FlinkVersions {

    /**
     * The leading {@code major.minor} of a Flink version.
     *
     * <p>Anchored and unanchored at the end so that {@code 2.2.1}, {@code 2.4-SNAPSHOT} and {@code
     * 1.20.4} all parse: what follows the minor carries no information this class needs.
     */
    private static final Pattern MAJOR_MINOR = Pattern.compile("^(\\d+)\\.(\\d+)");

    private FlinkVersions() {}

    /**
     * Whether the Flink on the classpath keeps the precision a SQL {@code TIME(p)} declares.
     *
     * @return true when a DDL {@code TIME(3)} reaches the connector as {@code TIME(3)}, false when
     *     the planner has resolved it to {@code TIME(0)} first
     */
    static boolean retainsSqlTimePrecision() {
        return retainsSqlTimePrecision(DataTypes.class.getPackage().getImplementationVersion());
    }

    /**
     * Whether {@code version} keeps the precision a SQL {@code TIME(p)} declares.
     *
     * <p>True from 2.3 onwards. The floor (2.2) and the 1.x LTS resolve a declared {@code TIME(p)}
     * to {@code TIME(0)} before the connector sees the schema, and 2.3 was the release that
     * stopped. Flink promises none of this, so it is measured instead: the weekly matrix covers
     * 1.20.4, 2.2.1 and 2.3.0, and its {@code next} row covers {@code 2.4-SNAPSHOT}. That the
     * versions after those keep 2.3's behaviour is what this predicate assumes rather than
     * something anyone has measured — and the {@code next} row is what would catch it if a release
     * changed course, which is the whole reason the boundary lives in one place.
     *
     * <p>Major and minor are compared as numbers, and the major first. Two shorter spellings get
     * that wrong, in different places, and both are covered in {@code FlinkVersionsTest}. Comparing
     * the minor alone puts {@code 1.20.4} above the boundary, because 20 is greater than 3.
     * Comparing the version strings lexicographically puts {@code 2.10.0} below it, because {@code
     * "2.10.0"} sorts before {@code "2.3.0"}.
     *
     * @param version a Flink version such as {@code 2.2.1}, {@code 2.4-SNAPSHOT} or {@code 1.20.4}
     * @return true from Flink 2.3 onwards, false below it
     * @throws AssertionError if {@code version} is null or does not start with {@code major.minor}
     *     — louder than defaulting, which would silently assert the floor's behaviour on a version
     *     whose behaviour is unknown
     */
    static boolean retainsSqlTimePrecision(String version) {
        Matcher matcher = version == null ? null : MAJOR_MINOR.matcher(version);
        if (matcher == null || !matcher.find()) {
            throw new AssertionError(
                    "Cannot read a Flink major.minor from the version '"
                            + version
                            + "', so a version-keyed test expectation cannot be evaluated. The"
                            + " no-argument overload takes this from the Implementation-Version"
                            + " manifest entry of the jar declaring "
                            + DataTypes.class.getName()
                            + ".");
        }
        int major = Integer.parseInt(matcher.group(1));
        int minor = Integer.parseInt(matcher.group(2));
        return major > 2 || (major == 2 && minor >= 3);
    }
}
