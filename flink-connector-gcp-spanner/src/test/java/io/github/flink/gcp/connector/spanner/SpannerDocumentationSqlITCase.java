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
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Emulator validation for the GoogleSQL regions rendered by the Spanner examples. */
@Tag("documentation-sql")
class SpannerDocumentationSqlITCase extends AbstractSpannerEmulatorITCase {

    private static final String RESOURCE_DIRECTORY = "sql-snippets";
    private static final String SPANNER_EXAMPLES = "spanner/SpannerGoogleSqlExamples.sql";
    private static final Pattern START_MARKER = Pattern.compile("^-- tag::([^\\[]+)\\[\\]$");
    private static final Pattern COMMENTS_ONLY =
            Pattern.compile("(?is)^(?:\\s|--[^\\r\\n]*(?:\\R|$)|/\\*.*?\\*/)*$");

    @Test
    void everyGoogleSqlRegionIsExecuted() {
        Set<Snippet> executed = new LinkedHashSet<>();
        scenarios()
                .map(Scenario::snippet)
                .forEach(
                        snippet ->
                                assertThat(executed.add(snippet))
                                        .as("one execution scenario for %s", snippet)
                                        .isTrue());
        assertThat(executed).containsExactlyInAnyOrderElementsOf(sourceRegions());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("scenarios")
    void documentedGoogleSqlExecutes(Scenario scenario) throws Exception {
        scenario.assertion().execute(splitStatements(region(scenario.snippet())));
    }

    private static Stream<Scenario> scenarios() {
        return Stream.of(
                new Scenario(
                        "accounts table and seed row",
                        new Snippet(SPANNER_EXAMPLES, "accounts-table-and-row"),
                        SpannerDocumentationSqlITCase::assertAccountsTableAndSeedRow),
                new Scenario(
                        "account status table",
                        new Snippet(SPANNER_EXAMPLES, "account-status-table"),
                        SpannerDocumentationSqlITCase::assertAccountStatusTable),
                new Scenario(
                        "inventory table and seed row",
                        new Snippet(SPANNER_EXAMPLES, "inventory-table-and-row"),
                        SpannerDocumentationSqlITCase::assertInventoryTableAndSeedRow),
                new Scenario(
                        "orders change stream and replica",
                        new Snippet(SPANNER_EXAMPLES, "orders-change-stream-and-replica"),
                        SpannerDocumentationSqlITCase::assertOrdersChangeStreamAndReplica),
                new Scenario(
                        "order change after source starts",
                        new Snippet(SPANNER_EXAMPLES, "order-change-after-source-starts"),
                        SpannerDocumentationSqlITCase::assertOrderChangeAfterSourceStarts));
    }

    @Test
    void statementSplitterIgnoresSqlComments() {
        assertThat(
                        splitStatements(
                                "-- the account's key; this is still one comment\n"
                                        + "SELECT 1; /* quoted ' text and ; stay in a comment */ SELECT 2;"
                                        + " -- trailing comment's ; is not a statement"))
                .containsExactly(
                        "-- the account's key; this is still one comment\nSELECT 1",
                        "/* quoted ' text and ; stay in a comment */ SELECT 2");
    }

    private static void assertAccountsTableAndSeedRow(List<String> statements) throws Exception {
        assertThat(statements).hasSize(2);
        DatabaseDestination database =
                createDatabase(Dialect.GOOGLE_STANDARD_SQL, statements.get(0));

        long updated =
                client(database)
                        .readWriteTransaction()
                        .run(
                                transaction ->
                                        transaction.executeUpdate(Statement.of(statements.get(1))));

        assertThat(updated).isEqualTo(1);
        assertThat(query(database, "SELECT region, account, name FROM accounts"))
                .singleElement()
                .satisfies(SpannerDocumentationSqlITCase::assertAccount);
    }

    private static void assertAccountStatusTable(List<String> statements) throws Exception {
        assertThat(statements).hasSize(1);

        DatabaseDestination database =
                createDatabase(Dialect.GOOGLE_STANDARD_SQL, statements.get(0));

        assertThat(query(database, "SELECT region, account, status FROM account_status LIMIT 1"))
                .isEmpty();
    }

    private static void assertInventoryTableAndSeedRow(List<String> statements) throws Exception {
        assertThat(statements).hasSize(2);
        DatabaseDestination database =
                createDatabase(Dialect.GOOGLE_STANDARD_SQL, statements.get(0));

        long updated =
                client(database)
                        .readWriteTransaction()
                        .run(
                                transaction ->
                                        transaction.executeUpdate(Statement.of(statements.get(1))));

        assertThat(updated).isEqualTo(1);
        assertThat(query(database, "SELECT sku, quantity FROM inventory"))
                .singleElement()
                .satisfies(
                        row -> {
                            assertThat(row.getString("sku")).isEqualTo("widget-1");
                            assertThat(row.getLong("quantity")).isEqualTo(12L);
                        });
    }

    private static void assertOrdersChangeStreamAndReplica(List<String> statements)
            throws Exception {
        assertThat(statements).hasSize(3);
        DatabaseDestination database =
                createDatabase(
                        Dialect.GOOGLE_STANDARD_SQL,
                        statements.get(0),
                        statements.get(1),
                        statements.get(2));

        assertThat(query(database, "SELECT order_id FROM source_orders")).isEmpty();
        assertThat(
                        query(
                                database,
                                "SELECT OPTION_VALUE"
                                        + " FROM INFORMATION_SCHEMA.CHANGE_STREAM_OPTIONS"
                                        + " WHERE CHANGE_STREAM_NAME = 'source_order_changes'"
                                        + " AND OPTION_NAME = 'value_capture_type'"))
                .singleElement()
                .satisfies(
                        row ->
                                assertThat(row.getString("OPTION_VALUE"))
                                        .isEqualTo("NEW_ROW_AND_OLD_VALUES"));
        assertThat(query(database, "SELECT order_id FROM order_replica")).isEmpty();
    }

    private static void assertOrderChangeAfterSourceStarts(List<String> statements)
            throws Exception {
        assertThat(statements).hasSize(1);
        List<String> setupStatements =
                splitStatements(
                        region(new Snippet(SPANNER_EXAMPLES, "orders-change-stream-and-replica")));
        assertThat(setupStatements).hasSize(3);
        DatabaseDestination database =
                createDatabase(
                        Dialect.GOOGLE_STANDARD_SQL,
                        setupStatements.get(0),
                        setupStatements.get(1),
                        setupStatements.get(2));

        long updated =
                client(database)
                        .readWriteTransaction()
                        .run(
                                transaction ->
                                        transaction.executeUpdate(Statement.of(statements.get(0))));

        assertThat(updated).isEqualTo(1);
        assertThat(query(database, "SELECT order_id, customer, status FROM source_orders"))
                .singleElement()
                .satisfies(
                        row -> {
                            assertThat(row.getLong("order_id")).isEqualTo(1L);
                            assertThat(row.getString("customer")).isEqualTo("Ada");
                            assertThat(row.getString("status")).isEqualTo("PENDING");
                        });
    }

    private static void assertAccount(Struct row) {
        assertThat(row.getString("region")).isEqualTo("region-1");
        assertThat(row.getLong("account")).isEqualTo(1);
        assertThat(row.getString("name")).isEqualTo("Ada");
    }

    private static Set<Snippet> sourceRegions() {
        URL resource =
                SpannerDocumentationSqlITCase.class
                        .getClassLoader()
                        .getResource(RESOURCE_DIRECTORY);
        assertThat(resource).as(RESOURCE_DIRECTORY).isNotNull();
        Set<Snippet> regions = new LinkedHashSet<>();
        final Path directory;
        try {
            directory = Path.of(resource.toURI());
        } catch (URISyntaxException exception) {
            throw new AssertionError("Could not locate " + RESOURCE_DIRECTORY, exception);
        }
        try (Stream<Path> files = Files.walk(directory)) {
            for (Path file :
                    files.filter(Files::isRegularFile)
                            .filter(path -> path.toString().endsWith(".sql"))
                            .sorted()
                            .collect(Collectors.toList())) {
                String resourceName =
                        "spanner/" + directory.relativize(file).toString().replace('\\', '/');
                for (String line : Files.readAllLines(file)) {
                    Matcher marker = START_MARKER.matcher(line);
                    if (marker.matches()) {
                        Snippet snippet = new Snippet(resourceName, marker.group(1));
                        assertThat(regions.add(snippet))
                                .as("unique SQL region %s", snippet)
                                .isTrue();
                    }
                }
            }
        } catch (IOException exception) {
            throw new AssertionError("Could not inventory " + RESOURCE_DIRECTORY, exception);
        }
        return regions;
    }

    private static String region(Snippet snippet) {
        String source = source(snippet.file());
        String start = "-- tag::" + snippet.tag() + "[]";
        String end = "-- end::" + snippet.tag() + "[]";
        List<String> lines = List.of(source.split("\\R", -1));
        assertThat(lines).filteredOn(start::equals).hasSize(1);
        assertThat(lines).filteredOn(end::equals).hasSize(1);
        int startLine = lines.indexOf(start);
        int endLine = lines.indexOf(end);
        assertThat(endLine).isGreaterThan(startLine + 1);
        return String.join("\n", lines.subList(startLine + 1, endLine));
    }

    private static List<String> splitStatements(String sql) {
        List<String> statements = new ArrayList<>();
        StringBuilder statement = new StringBuilder();
        char quote = 0;
        boolean lineComment = false;
        boolean blockComment = false;
        for (int index = 0; index < sql.length(); index++) {
            char current = sql.charAt(index);
            statement.append(current);
            if (lineComment) {
                if (current == '\n' || current == '\r') {
                    lineComment = false;
                }
            } else if (blockComment) {
                if (current == '*' && index + 1 < sql.length() && sql.charAt(index + 1) == '/') {
                    statement.append(sql.charAt(++index));
                    blockComment = false;
                }
            } else if (quote != 0) {
                if (current == quote) {
                    if (index + 1 < sql.length() && sql.charAt(index + 1) == quote) {
                        statement.append(sql.charAt(++index));
                    } else {
                        quote = 0;
                    }
                }
            } else if (current == '-' && index + 1 < sql.length() && sql.charAt(index + 1) == '-') {
                statement.append(sql.charAt(++index));
                lineComment = true;
            } else if (current == '/' && index + 1 < sql.length() && sql.charAt(index + 1) == '*') {
                statement.append(sql.charAt(++index));
                blockComment = true;
            } else if (current == '\'' || current == '"' || current == '`') {
                quote = current;
            } else if (current == ';') {
                statements.add(statement.substring(0, statement.length() - 1).trim());
                statement.setLength(0);
            }
        }
        assertThat(quote).as("unterminated quoted value").isEqualTo((char) 0);
        assertThat(blockComment).as("unterminated block comment").isFalse();
        assertThat(COMMENTS_ONLY.matcher(statement).matches())
                .as("text after the final SQL statement")
                .isTrue();
        return statements;
    }

    private static String source(String file) {
        assertThat(file).startsWith("spanner/");
        String resource = RESOURCE_DIRECTORY + "/" + file.substring("spanner/".length());
        try (InputStream input =
                SpannerDocumentationSqlITCase.class
                        .getClassLoader()
                        .getResourceAsStream(resource)) {
            assertThat(input).as(resource).isNotNull();
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException exception) {
            throw new AssertionError("Could not read " + resource, exception);
        }
    }

    @FunctionalInterface
    private interface SqlAssertion {
        void execute(List<String> statements) throws Exception;
    }

    private static final class Scenario {
        private final String name;
        private final Snippet snippet;
        private final SqlAssertion assertion;

        private Scenario(String name, Snippet snippet, SqlAssertion assertion) {
            this.name = name;
            this.snippet = snippet;
            this.assertion = assertion;
        }

        private Snippet snippet() {
            return snippet;
        }

        private SqlAssertion assertion() {
            return assertion;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    private static final class Snippet {
        private final String file;
        private final String tag;

        private Snippet(String file, String tag) {
            this.file = file;
            this.tag = tag;
        }

        private String file() {
            return file;
        }

        private String tag() {
            return tag;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Snippet)) {
                return false;
            }
            Snippet that = (Snippet) other;
            return file.equals(that.file) && tag.equals(that.tag);
        }

        @Override
        public int hashCode() {
            return 31 * file.hashCode() + tag.hashCode();
        }

        @Override
        public String toString() {
            return file + "#" + tag;
        }
    }
}
