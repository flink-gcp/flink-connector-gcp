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

package io.github.flink.gcp.connector.spanner.table.catalog;

import org.apache.flink.annotation.Internal;

import java.util.Collection;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Renders the {@code schema.*} marker options' values from column names, quoted so that Flink
 * parses them back to the same names.
 *
 * <p>A Spanner column name may contain the characters Flink's list and map options split on: a
 * quoted GoogleSQL or PostgreSQL column can be named {@code col;semi} or {@code a.b}. Flink's
 * options parse YAML first on 2.x and fall back to its legacy splitter, which honours single quotes
 * with doubled quotes inside, the escaping Flink's own {@code StructuredOptionsSplitter} writes.
 * Every name that is not a plain identifier is quoted, which also keeps a name such as {@code [a]}
 * from being read as a YAML list.
 */
@Internal
final class SpannerMarkerValues {

    private static final Pattern PLAIN = Pattern.compile("[A-Za-z_][A-Za-z0-9_.]*");

    private SpannerMarkerValues() {}

    /** A list option's value: the names, separated by {@code ;}. */
    static String list(Collection<String> names) {
        return names.stream().map(SpannerMarkerValues::quote).collect(Collectors.joining(";"));
    }

    /**
     * A map option's value: {@code key:value} entries separated by {@code ,}, each entry quoted
     * again when it carries a quote or a comma, as Flink nests the two levels.
     */
    static String map(Map<String, String> entries) {
        return entries.entrySet().stream()
                .map(entry -> quoteEntry(quote(entry.getKey()) + ":" + quote(entry.getValue())))
                .collect(Collectors.joining(","));
    }

    private static String quote(String value) {
        return PLAIN.matcher(value).matches() ? value : singleQuoted(value);
    }

    private static String quoteEntry(String entry) {
        return entry.indexOf(',') >= 0 || entry.indexOf('\'') >= 0 || entry.indexOf('"') >= 0
                ? singleQuoted(entry)
                : entry;
    }

    private static String singleQuoted(String value) {
        return "'" + value.replace("'", "''") + "'";
    }
}
