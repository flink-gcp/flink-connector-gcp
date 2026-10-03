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

package io.github.flink.gcp.connector.spanner;

import com.google.cloud.spanner.Dialect;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link SpannerObjectName}: every name the catalog lists must resolve to the table it came from,
 * and every name it emits as an option must decode to the native name.
 */
class SpannerObjectNameTest {

    /**
     * Characters whose interactions break a naive split or quote: both quote characters, the
     * escape, the separator, whitespace, case, a digit and a non-ASCII letter.
     */
    private static final char[] ALPHABET = {'a', 'A', '_', '0', '.', '`', '"', '\\', ' ', 'é'};

    @ParameterizedTest
    @EnumSource(
            value = Dialect.class,
            names = {"GOOGLE_STANDARD_SQL", "POSTGRESQL"})
    void everyFormattedNameParsesBackToItsSchemaAndTable(Dialect dialect) {
        List<String> names = names(3);
        List<String> schemas = new ArrayList<>(names(2));
        schemas.add(dialect.getDefaultSchema());

        for (String schema : schemas) {
            for (String table : names) {
                String formatted = SpannerObjectName.format(schema, table, dialect);
                Optional<SpannerObjectName> parsed = SpannerObjectName.parse(formatted, dialect);

                assertThat(parsed)
                        .as("%s formatted as %s", schema + "/" + table, formatted)
                        .isPresent();
                assertThat(parsed.get().schema()).as(formatted).isEqualTo(schema);
                assertThat(parsed.get().table()).as(formatted).isEqualTo(table);
            }
        }
    }

    /**
     * The catalog emits a named schema's {@code schema} and {@code table} options as single parts;
     * the connector reads them through {@link SpannerTableName#of}, which must land on the native
     * names, not a folded or re-split spelling.
     */
    @ParameterizedTest
    @EnumSource(
            value = Dialect.class,
            names = {"GOOGLE_STANDARD_SQL", "POSTGRESQL"})
    void everyEmittedPartDecodesThroughTheConnectorToTheNativeName(Dialect dialect) {
        for (String name : names(3)) {
            SpannerTableName table =
                    SpannerTableName.of(
                            SpannerObjectName.encodePart(name, dialect),
                            SpannerObjectName.encodePart(name, dialect),
                            dialect);

            assertThat(table.schema()).as(name).isEqualTo(name);
            assertThat(table.table()).as(name).isEqualTo(name);
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = Dialect.class,
            names = {"GOOGLE_STANDARD_SQL", "POSTGRESQL"})
    void aNameThatIsNotOneOrTwoCanonicalPartsParsesAsNothing(Dialect dialect) {
        for (String name : new String[] {"", "a.b.c", "a.", ".a", "a..b", "`a", "\"a"}) {
            assertThat(SpannerObjectName.parse(name, dialect)).as(name).isEmpty();
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = Dialect.class,
            names = {"GOOGLE_STANDARD_SQL", "POSTGRESQL"})
    void theDefaultSchemaIsUnqualifiedAndANamedOneIsNot(Dialect dialect) {
        String defaultSchema = dialect.getDefaultSchema();

        assertThat(SpannerObjectName.format(defaultSchema, "orders", dialect)).isEqualTo("orders");
        assertThat(SpannerObjectName.format("sales", "orders", dialect)).isEqualTo("sales.orders");
    }

    @org.junit.jupiter.api.Test
    void aPostgresqlNameWithACapitalIsQuotedSoItDoesNotFold() {
        assertThat(SpannerObjectName.format("public", "Orders", Dialect.POSTGRESQL))
                .isEqualTo("\"Orders\"");
        assertThat(SpannerObjectName.parse("Orders", Dialect.POSTGRESQL).get().table())
                .isEqualTo("orders");
        assertThat(SpannerObjectName.parse("public.orders", Dialect.POSTGRESQL).get())
                .isEqualTo(SpannerObjectName.parse("orders", Dialect.POSTGRESQL).get());
    }

    @org.junit.jupiter.api.Test
    void aGoogleSqlNameKeepsItsCaseAndQuotesOnlyWhatIsNotAPlainIdentifier() {
        assertThat(SpannerObjectName.format("", "Orders", Dialect.GOOGLE_STANDARD_SQL))
                .isEqualTo("Orders");
        assertThat(SpannerObjectName.format("Sales", "Order Items", Dialect.GOOGLE_STANDARD_SQL))
                .isEqualTo("Sales.`Order Items`");
        assertThat(SpannerObjectName.format("", "a`b\\c", Dialect.GOOGLE_STANDARD_SQL))
                .isEqualTo("`a\\`b\\\\c`");
    }

    /** Every non-empty string over {@link #ALPHABET} up to the given length. */
    private static List<String> names(int maxLength) {
        List<String> names = new ArrayList<>();
        List<String> previous = new ArrayList<>();
        previous.add("");
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
