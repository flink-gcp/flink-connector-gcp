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

package io.github.flink.gcp.connector.docs;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.command.SetOperation;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.swing.text.MutableAttributeSet;
import javax.swing.text.html.HTML;
import javax.swing.text.html.HTMLEditorKit;
import javax.swing.text.html.parser.ParserDelegator;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Planner validation for the Flink SQL regions rendered by the examples and quickstarts. */
class DocumentationSqlPlanTest {

    private static final String DOCS_PUBLIC_PROPERTY = "flink.gcp.docs.public";
    private static final Pattern START_MARKER = Pattern.compile("^-- tag::([^\\[]+)\\[\\]$");
    private static final Pattern SET_STATEMENT =
            Pattern.compile("(?is)^(?:\\s|--[^\\r\\n]*(?:\\R|$)|/\\*.*?\\*/)*SET\\b");
    private static final Pattern COMMENTS_ONLY =
            Pattern.compile("(?is)^(?:\\s|--[^\\r\\n]*(?:\\R|$)|/\\*.*?\\*/)*$");
    private static final ScenarioSetup SOURCE_CHANGES_VIEW = upsertView("source_changes");
    private static final ScenarioSetup MYSQL_SOURCE_CHANGES_VIEW =
            upsertView("mysql_source_changes");

    @ParameterizedTest(name = "{0}")
    @MethodSource("scenarios")
    void documentedFlinkSqlCanBePlanned(Scenario scenario) {
        List<String> regions =
                scenario.snippets().stream()
                        .map(DocumentationSqlPlanTest::readRegion)
                        .collect(Collectors.toList());
        boolean batch = usesBatchRuntimeMode(regions);
        StreamExecutionEnvironment execution = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTable =
                StreamTableEnvironment.create(
                        execution,
                        batch
                                ? EnvironmentSettings.inBatchMode()
                                : EnvironmentSettings.inStreamingMode());
        TableEnvironmentInternal table = (TableEnvironmentInternal) streamTable;
        scenario.setups().forEach(setup -> setup.apply(execution, streamTable));

        for (int index = 0; index < scenario.snippets().size(); index++) {
            Snippet snippet = scenario.snippets().get(index);
            try {
                plan(table, regions.get(index));
            } catch (RuntimeException | AssertionError failure) {
                throw new AssertionError("Failed to plan " + snippet, failure);
            }
        }
    }

    @Test
    void batchRuntimeModeDetectionDoesNotDependOnSpacing() {
        assertThat(usesBatchRuntimeMode(List.of("SET 'execution.runtime-mode'='batch';"))).isTrue();
        assertThat(usesBatchRuntimeMode(List.of("set  'execution.runtime-mode' = 'batch' ;")))
                .isTrue();
        assertThat(
                        usesBatchRuntimeMode(
                                List.of(
                                        "-- select the topic's mode; this semicolon is commentary\n"
                                                + "SET 'execution.runtime-mode'='batch';")))
                .isTrue();
        assertThat(
                        usesBatchRuntimeMode(
                                List.of(
                                        "/* the topic's mode; remains commentary */ "
                                                + "SET 'execution.runtime-mode' = 'batch';")))
                .isTrue();
        assertThat(usesBatchRuntimeMode(List.of("SET 'execution.runtime-mode' = 'streaming';")))
                .isFalse();
        assertThat(
                        usesBatchRuntimeMode(
                                List.of("SELECT 'SET ''execution.runtime-mode'' = ''batch'';'")))
                .isFalse();
    }

    @Test
    void statementSplitterIgnoresTrailingSqlComments() {
        assertThat(
                        splitStatements(
                                "SELECT 1;\n"
                                        + "-- Published guidance may follow the final statement.\n"
                                        + "/* Block guidance is ignored too. */"))
                .containsExactly("SELECT 1;");
    }

    @Test
    @EnabledIfSystemProperty(named = DOCS_PUBLIC_PROPERTY, matches = ".+")
    void everyRenderedSqlBlockIsSourceBacked() throws IOException {
        Path repository = repositoryRoot();
        Path publicDirectory = repository.resolve(System.getProperty(DOCS_PUBLIC_PROPERTY));
        Set<Snippet> documented =
                renderedSqlSnippets(
                        List.of(
                                publicDirectory.resolve("docs/examples"),
                                publicDirectory.resolve("docs/quickstart")));

        Set<Snippet> flinkRegions =
                sourceRegions(
                        repository.resolve(
                                "flink-connector-gcp-docs-validation/src/test/resources/sql-snippets"),
                        "flink");
        Set<Snippet> googleSqlRegions =
                sourceRegions(
                        repository.resolve(
                                "flink-connector-gcp-spanner/src/test/resources/sql-snippets"),
                        "spanner");
        Set<Snippet> allRegions = new HashSet<>(flinkRegions);
        allRegions.addAll(googleSqlRegions);
        assertThat(documented).containsExactlyInAnyOrderElementsOf(allRegions);
    }

    @Test
    void everyFlinkRegionIsPlanned() throws IOException {
        Set<Snippet> flinkRegions =
                sourceRegions(
                        repositoryRoot()
                                .resolve(
                                        "flink-connector-gcp-docs-validation/src/test/resources/sql-snippets"),
                        "flink");
        Set<Snippet> planned = new HashSet<>();
        scenarios()
                .flatMap(scenario -> scenario.snippets().stream())
                .forEach(
                        snippet ->
                                assertThat(planned.add(snippet))
                                        .as(
                                                "%s appears in more than one planning scenario",
                                                snippet)
                                        .isTrue());
        assertThat(planned)
                .as("each Flink SQL source region is planned exactly once")
                .containsExactlyInAnyOrderElementsOf(flinkRegions);
    }

    private static Stream<Scenario> scenarios() {
        return Stream.of(
                scenario(
                        "BigQuery MySQL CDC sink",
                        MYSQL_SOURCE_CHANGES_VIEW,
                        snippet("flink/BigQueryExamples.sql", "mysql-sink-table"),
                        snippet("flink/BigQueryExamples.sql", "mysql-sink-insert")),
                scenario(
                        "BigQuery Debezium CDC sink",
                        SOURCE_CHANGES_VIEW,
                        snippet("flink/BigQueryExamples.sql", "debezium-sink-table"),
                        snippet("flink/BigQueryExamples.sql", "debezium-sink-insert")),
                scenario(
                        "BigQuery TiCDC pipeline",
                        snippet("flink/BigQueryExamples.sql", "ticdc-source-table"),
                        snippet("flink/BigQueryExamples.sql", "ticdc-sink-and-insert")),
                scenario(
                        "BigQuery Debezium JSON sink",
                        SOURCE_CHANGES_VIEW,
                        snippet("flink/BigQueryExamples.sql", "debezium-json-sink-and-insert")),
                scenario(
                        "Spanner to BigQuery change stream pipeline",
                        snippet("flink/BigQueryExamples.sql", "spanner-change-stream-source"),
                        snippet("flink/BigQueryExamples.sql", "spanner-change-stream-sink"),
                        snippet("flink/BigQueryExamples.sql", "spanner-change-stream-insert")),
                scenario(
                        "BigQuery bounded source",
                        snippet("flink/BigQueryExamples.sql", "bounded-source")),
                scenario(
                        "Bigtable bounded source and lookup join",
                        snippet("flink/BigtableExamples.sql", "batch-source"),
                        snippet("flink/BigtableExamples.sql", "lookup-join")),
                scenario(
                        "Bigtable change stream envelope",
                        snippet("flink/BigtableExamples.sql", "change-stream-envelope")),
                scenario(
                        "Bigtable batch upsert",
                        snippet("flink/BigtableExamples.sql", "batch-upsert")),
                scenario(
                        "Cloud Tasks nested JSON request",
                        snippet("flink/CloudTasksExamples.sql", "nested-json")),
                scenario("Cloud Tasks CSV request", snippet("flink/CloudTasksExamples.sql", "csv")),
                scenario("Cloud Tasks raw request", snippet("flink/CloudTasksExamples.sql", "raw")),
                scenario(
                        "Cloud Tasks Avro request",
                        snippet("flink/CloudTasksExamples.sql", "avro")),
                scenario("Pub/Sub sink", snippet("flink/PubSubExamples.sql", "sink")),
                scenario("Pub/Sub source", snippet("flink/PubSubExamples.sql", "source")),
                scenario(
                        "Spanner lookup join", snippet("flink/SpannerExamples.sql", "lookup-join")),
                scenario(
                        "Spanner batch upsert",
                        snippet("flink/SpannerExamples.sql", "batch-upsert")),
                scenario(
                        "Spanner change stream source",
                        snippet("flink/SpannerExamples.sql", "change-stream-source")),
                scenario("Bigtable quickstart", snippet("flink/BigtableQuickstart.sql", "sink")),
                scenario("Pub/Sub sink quickstart", snippet("flink/PubSubQuickstart.sql", "sink")),
                scenario(
                        "Pub/Sub source quickstart",
                        snippet("flink/PubSubQuickstart.sql", "source")));
    }

    private static Scenario scenario(String name, Snippet... snippets) {
        return scenario(name, List.of(), snippets);
    }

    private static Scenario scenario(String name, ScenarioSetup setup, Snippet... snippets) {
        return scenario(name, List.of(setup), snippets);
    }

    private static Scenario scenario(String name, List<ScenarioSetup> setups, Snippet... snippets) {
        return new Scenario(name, setups, List.of(snippets));
    }

    private static Snippet snippet(String file, String tag) {
        return new Snippet(file, tag);
    }

    private static ScenarioSetup upsertView(String name) {
        return (execution, table) -> {
            DataStream<Row> changes =
                    execution.fromData(
                            List.of(
                                    Row.ofKind(
                                            RowKind.UPDATE_AFTER,
                                            "id",
                                            1L,
                                            Map.of("server_id", "1")),
                                    Row.ofKind(RowKind.DELETE, "id", 1L, Map.of("server_id", "2"))),
                            Types.ROW_NAMED(
                                    new String[] {"id", "amount", "source_properties"},
                                    Types.STRING,
                                    Types.LONG,
                                    Types.MAP(Types.STRING, Types.STRING)));
            table.createTemporaryView(
                    name,
                    table.fromChangelogStream(
                            changes,
                            Schema.newBuilder()
                                    .column("id", DataTypes.STRING().notNull())
                                    .column("amount", DataTypes.BIGINT())
                                    .column(
                                            "source_properties",
                                            DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                                    .primaryKey("id")
                                    .build(),
                            ChangelogMode.upsert()));
        };
    }

    private static boolean usesBatchRuntimeMode(List<String> regions) {
        TableEnvironmentInternal parser =
                (TableEnvironmentInternal)
                        TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        return regions.stream()
                .flatMap(sql -> splitStatements(sql).stream())
                .filter(statement -> SET_STATEMENT.matcher(statement).find())
                .flatMap(statement -> parser.getParser().parse(statement).stream())
                .filter(SetOperation.class::isInstance)
                .map(SetOperation.class::cast)
                .anyMatch(
                        operation ->
                                operation
                                                .getKey()
                                                .filter("execution.runtime-mode"::equals)
                                                .isPresent()
                                        && operation
                                                .getValue()
                                                .filter("batch"::equalsIgnoreCase)
                                                .isPresent());
    }

    private static Set<Snippet> renderedSqlSnippets(List<Path> publicDirectories)
            throws IOException {
        Map<Snippet, Path> firstPageBySnippet = new LinkedHashMap<>();
        for (Path publicDirectory : publicDirectories) {
            assertThat(Files.isDirectory(publicDirectory))
                    .as("%s was not built", publicDirectory)
                    .isTrue();
            try (Stream<Path> pages = Files.walk(publicDirectory)) {
                for (Path page :
                        pages.filter(Files::isRegularFile)
                                .filter(path -> path.toString().endsWith(".html"))
                                .collect(Collectors.toList())) {
                    try (Reader html = Files.newBufferedReader(page)) {
                        new ParserDelegator()
                                .parse(
                                        html,
                                        new HTMLEditorKit.ParserCallback() {
                                            @Override
                                            public void handleStartTag(
                                                    HTML.Tag tag,
                                                    MutableAttributeSet attributes,
                                                    int position) {
                                                if (!tag.equals(HTML.Tag.SPAN)) {
                                                    return;
                                                }
                                                Object file =
                                                        attributes.getAttribute(
                                                                "data-sql-snippet-file");
                                                Object region =
                                                        attributes.getAttribute(
                                                                "data-sql-snippet-tag");
                                                if (file == null && region == null) {
                                                    return;
                                                }
                                                assertThat(file != null && region != null)
                                                        .as(
                                                                "%s has an incomplete SQL snippet marker",
                                                                page)
                                                        .isTrue();
                                                Snippet snippet =
                                                        new Snippet(
                                                                file.toString(), region.toString());
                                                Path firstPage =
                                                        firstPageBySnippet.putIfAbsent(
                                                                snippet, page);
                                                assertThat(firstPage)
                                                        .as(
                                                                "SQL source region %s is rendered by both %s and %s",
                                                                snippet, firstPage, page)
                                                        .isNull();
                                            }
                                        },
                                        true);
                    }
                }
            }
        }
        return new LinkedHashSet<>(firstPageBySnippet.keySet());
    }

    private static void plan(TableEnvironmentInternal table, String sql) {
        for (String statement : splitStatements(sql)) {
            for (Operation operation : table.getParser().parse(statement)) {
                if (operation.getClass().getSimpleName().equals("AddJarOperation")) {
                    continue;
                }
                if (operation instanceof SetOperation) {
                    SetOperation set = (SetOperation) operation;
                    String key = set.getKey().orElseThrow();
                    if (!key.equals("execution.runtime-mode")) {
                        table.getConfig()
                                .getConfiguration()
                                .setString(key, set.getValue().orElseThrow());
                    }
                } else if (operation instanceof QueryOperation
                        || operation instanceof ModifyOperation) {
                    table.explainInternal(List.of(operation));
                } else {
                    table.executeInternal(operation);
                }
            }
        }
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
                statements.add(statement.toString());
                statement.setLength(0);
            }
        }
        assertThat(quote)
                .as("SQL region contains an unterminated quoted value")
                .isEqualTo((char) 0);
        assertThat(blockComment).as("SQL region contains an unterminated block comment").isFalse();
        if (!COMMENTS_ONLY.matcher(statement).matches()) {
            statements.add(statement.toString());
        }
        return statements;
    }

    private static String readRegion(Snippet snippet) {
        assertThat(snippet.file()).as("%s is not Flink SQL", snippet).startsWith("flink/");
        Path source =
                repositoryRoot()
                        .resolve(
                                "flink-connector-gcp-docs-validation/src/test/resources/sql-snippets")
                        .resolve(snippet.file().substring("flink/".length()));
        try {
            return region(Files.readString(source), snippet, source.toString());
        } catch (IOException exception) {
            throw new AssertionError("Could not read " + source, exception);
        }
    }

    private static String region(String source, Snippet snippet, String sourceName) {
        String start = "-- tag::" + snippet.tag() + "[]";
        String end = "-- end::" + snippet.tag() + "[]";
        String identity = sourceName + "#" + snippet.tag();
        List<String> lines = List.of(source.split("\\R", -1));
        assertThat(lines.stream().filter(start::equals).count())
                .as("%s start marker", identity)
                .isEqualTo(1);
        assertThat(lines.stream().filter(end::equals).count())
                .as("%s end marker", identity)
                .isEqualTo(1);
        int startLine = lines.indexOf(start);
        int endLine = lines.indexOf(end);
        assertThat(endLine)
                .as("%s has an empty or reversed region", identity)
                .isGreaterThan(startLine + 1);
        return String.join("\n", lines.subList(startLine + 1, endLine));
    }

    private static Set<Snippet> sourceRegions(Path directory, String namespace) throws IOException {
        Set<Snippet> regions = new LinkedHashSet<>();
        try (Stream<Path> files = Files.walk(directory)) {
            for (Path file :
                    files.filter(Files::isRegularFile)
                            .filter(path -> path.toString().endsWith(".sql"))
                            .sorted()
                            .collect(Collectors.toList())) {
                String source =
                        namespace + "/" + directory.relativize(file).toString().replace('\\', '/');
                for (String line : Files.readAllLines(file)) {
                    Matcher marker = START_MARKER.matcher(line);
                    if (marker.matches()) {
                        assertThat(regions.add(new Snippet(source, marker.group(1))))
                                .as("%s repeats tag %s", file, marker.group(1))
                                .isTrue();
                    }
                }
            }
        }
        return regions;
    }

    private static Path repositoryRoot() {
        Path candidate = Path.of("").toAbsolutePath();
        while (candidate != null) {
            if (Files.isRegularFile(candidate.resolve("docs/hugo.toml"))
                    && Files.isRegularFile(candidate.resolve("pom.xml"))) {
                return candidate;
            }
            candidate = candidate.getParent();
        }
        throw new AssertionError("Could not locate the repository root from " + Path.of(""));
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

    private static final class Scenario {
        private final String name;
        private final List<ScenarioSetup> setups;
        private final List<Snippet> snippets;

        private Scenario(String name, List<ScenarioSetup> setups, List<Snippet> snippets) {
            this.name = name;
            this.setups = setups;
            this.snippets = snippets;
        }

        private List<ScenarioSetup> setups() {
            return setups;
        }

        private List<Snippet> snippets() {
            return snippets;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    @FunctionalInterface
    private interface ScenarioSetup {
        void apply(StreamExecutionEnvironment execution, StreamTableEnvironment table);
    }
}
