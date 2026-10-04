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

import org.apache.flink.configuration.Configuration;

import io.github.flink.gcp.connector.spanner.table.SpannerConnectorOptions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link SpannerMarkerValues}, read back through Flink's {@link Configuration} as the planner reads
 * a catalog table's options, so the round trip holds on whichever Flink line runs it: 2.x parses
 * list and map options as YAML first and falls back to its legacy splitter, 1.20 uses the splitter
 * alone.
 */
class SpannerMarkerValuesTest {

    /** Every character either parser treats specially, beside a plain letter and a dot. */
    private static final char[] ALPHABET = {
        'a', ';', ',', ':', '\'', '"', '[', '{', '-', ' ', '.', '#'
    };

    @Test
    void everyListOfNamesReadsBackUnchanged() {
        List<String> names = names(3);
        for (String name : names) {
            assertListRoundTrip(Collections.singletonList(name));
        }
        for (String first : names(2)) {
            for (String second : names(2)) {
                assertListRoundTrip(Arrays.asList(first, second));
            }
        }
    }

    @Test
    void everyMapOfNamesToTypesReadsBackUnchanged() {
        for (String first : names(2)) {
            for (String second : names(2)) {
                if (first.equals(second)) {
                    continue;
                }
                Map<String, String> entries = new LinkedHashMap<>();
                entries.put(first, "example.events.Event");
                entries.put(second, "example.events.Status");
                String value = SpannerMarkerValues.map(entries);

                Map<String, String> read =
                        Configuration.fromMap(
                                        Collections.singletonMap(
                                                SpannerConnectorOptions.PROTO_TYPE_NAMES.key(),
                                                value))
                                .get(SpannerConnectorOptions.PROTO_TYPE_NAMES);
                assertThat(read).as(value).isEqualTo(entries);
            }
        }
    }

    @Test
    void plainNamesStayUnquoted() {
        assertThat(SpannerMarkerValues.list(Arrays.asList("payload", "related_ids")))
                .isEqualTo("payload;related_ids");
        assertThat(
                        SpannerMarkerValues.map(
                                Collections.singletonMap("event", "example.events.Event")))
                .isEqualTo("event:example.events.Event");
    }

    private static void assertListRoundTrip(List<String> names) {
        String value = SpannerMarkerValues.list(names);
        List<String> read =
                Configuration.fromMap(
                                Collections.singletonMap(
                                        SpannerConnectorOptions.JSON_FIELD_PATHS.key(), value))
                        .get(SpannerConnectorOptions.JSON_FIELD_PATHS);
        assertThat(read).as(value).isEqualTo(names);
    }

    /** Every non-empty string over {@link #ALPHABET} up to the given length. */
    private static List<String> names(int maxLength) {
        List<String> names = new ArrayList<>();
        List<String> previous = Collections.singletonList("");
        for (int length = 1; length <= maxLength; length++) {
            List<String> next = new ArrayList<>();
            for (String prefix : previous) {
                for (char c : ALPHABET) {
                    next.add(prefix + c);
                }
            }
            names.addAll(next);
            previous = next;
        }
        return names;
    }
}
