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
import org.apache.flink.table.functions.ScalarFunction;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Planner validation for the Flink SQL regions rendered by the documentation. */
public class DocumentationSqlPlanTest {

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
    void documentedFlinkSqlMatchesItsValidationContract(Scenario scenario) {
        List<String> regions =
                scenario.steps().stream().map(ValidationStep::sql).collect(Collectors.toList());
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

        for (int index = 0; index < scenario.steps().size(); index++) {
            ValidationStep step = scenario.steps().get(index);
            String sql = regions.get(index);
            if (step.expectedDiagnostics().isEmpty()) {
                try {
                    plan(table, sql);
                } catch (RuntimeException | AssertionError failure) {
                    throw new AssertionError("Failed to plan " + step.snippet(), failure);
                }
            } else {
                var failure =
                        assertThatThrownBy(
                                () -> plan(table, sql),
                                "%s must fail at its documented boundary",
                                step.snippet());
                step.expectedDiagnostics().forEach(failure::hasStackTraceContaining);
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
                                publicDirectory.resolve("docs/quickstart"),
                                publicDirectory.resolve("docs/connectors/table")));

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
    void addJarExamplesNameOneReleasedVersion() throws IOException {
        // The release doc-bump rule (repository guide, Version policy) updates every
        // ADD JAR literal to the version being released. Only two of the three carry an
        // exact plan expectation and plan() skips AddJarOperation, so a partial bump
        // would otherwise stay green while a rendered page names the previous release.
        Pattern jarName = Pattern.compile("flink-sql-connector-gcp-[a-z]+-([^']+)'");
        Map<String, String> suffixes = new LinkedHashMap<>();
        Path directory =
                repositoryRoot()
                        .resolve(
                                "flink-connector-gcp-docs-validation/src/test/resources/sql-snippets");
        try (Stream<Path> files = Files.walk(directory)) {
            for (Path file :
                    files.filter(Files::isRegularFile)
                            .filter(path -> path.toString().endsWith(".sql"))
                            .sorted()
                            .collect(Collectors.toList())) {
                for (String line : Files.readAllLines(file)) {
                    if (!line.contains("ADD JAR")) {
                        continue;
                    }
                    Matcher matcher = jarName.matcher(line);
                    assertThat(matcher.find())
                            .as(
                                    "%s: %s must name a quoted"
                                            + " flink-sql-connector-gcp-<connector> jar",
                                    file.getFileName(), line.trim())
                            .isTrue();
                    suffixes.put(file.getFileName() + ": " + line.trim(), matcher.group(1));
                }
            }
        }
        assertThat(suffixes).as("the docs carry ADD JAR examples").isNotEmpty();
        suffixes.forEach(
                (where, suffix) ->
                        assertThat(suffix)
                                .as(
                                        "%s must name the bare released uber-jar, not a"
                                                + " classifier or the -1.20 line (the docs-wide"
                                                + " convention names the 2.x jar)",
                                        where)
                                .matches("[0-9]+\\.[0-9]+\\.[0-9]+\\.jar"));
        assertThat(new HashSet<>(suffixes.values()))
                .as("every ADD JAR example names the same released version: %s", suffixes)
                .hasSize(1);
    }

    @Test
    void everyFlinkRegionHasOneValidationBoundary() throws IOException {
        Set<Snippet> flinkRegions =
                sourceRegions(
                        repositoryRoot()
                                .resolve(
                                        "flink-connector-gcp-docs-validation/src/test/resources/sql-snippets"),
                        "flink");
        Set<Snippet> validated = new HashSet<>();
        scenarios()
                .flatMap(scenario -> scenario.steps().stream())
                .map(ValidationStep::snippet)
                .forEach(
                        snippet ->
                                assertThat(validated.add(snippet))
                                        .as(
                                                "%s appears in more than one validation scenario",
                                                snippet)
                                        .isTrue());
        assertThat(validated)
                .as("each Flink SQL source region has exactly one validation boundary")
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
                        "BigQuery append-only table sink",
                        snippet("flink/BigQueryExamples.sql", "table-sink")),
                scenario(
                        "BigQuery exactly-once table sink options",
                        fragment(
                                "flink/BigQueryExamples.sql",
                                "table-sink-exactly-once-options",
                                DocumentationSqlPlanTest::bigQuerySinkMethodScenario)),
                scenario(
                        "BigQuery FILE_LOADS table sink options",
                        fragment(
                                "flink/BigQueryExamples.sql",
                                "table-sink-file-loads-options",
                                DocumentationSqlPlanTest::bigQuerySinkMethodScenario)),
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
                        "Bigtable insert-only sink",
                        snippet("flink/BigtableExamples.sql", "insert-only-sink")),
                scenario(
                        "Bigtable writable cell timestamp",
                        snippet("flink/BigtableExamples.sql", "cell-timestamp-sink")),
                scenario(
                        "Pub/Sub event to Bigtable attributes to Cloud Tasks",
                        snippet("flink/BigtableExamples.sql", "attribute-enrichment-pipeline")),
                scenario(
                        "Bigtable selected cell to BigQuery CDC",
                        snippet("flink/BigtableExamples.sql", "selected-cell-bigquery-cdc")),
                scenario(
                        "Cloud Tasks App Engine target",
                        snippet("flink/CloudTasksExamples.sql", "app-engine-target")),
                scenario(
                        "Cloud Tasks Cloud Run function",
                        snippet("flink/CloudTasksExamples.sql", "cloud-run-function")),
                scenario(
                        "Cloud Tasks external API request",
                        snippet("flink/CloudTasksExamples.sql", "external-api")),
                scenario(
                        "Pub/Sub to Bigtable lookup to Cloud Tasks",
                        snippet("flink/CloudTasksExamples.sql", "pubsub-bigtable-cloud-tasks")),
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
                        "Spanner bounded table source",
                        snippet("flink/SpannerExamples.sql", "bounded-table-source")),
                scenario(
                        "Spanner batch upsert",
                        snippet("flink/SpannerExamples.sql", "batch-upsert")),
                scenario(
                        "Spanner full change stream source",
                        snippet("flink/SpannerExamples.sql", "change-stream-full")),
                scenario(
                        "Spanner change stream materialization",
                        snippet("flink/SpannerExamples.sql", "change-stream-materialization")),
                scenario("Bigtable quickstart", snippet("flink/BigtableQuickstart.sql", "sink")),
                scenario("Pub/Sub sink quickstart", snippet("flink/PubSubQuickstart.sql", "sink")),
                scenario(
                        "Pub/Sub source quickstart",
                        snippet("flink/PubSubQuickstart.sql", "source")),
                scenario(
                        "BigQuery table reference overview",
                        setup(
                                "CREATE TEMPORARY VIEW staged_events AS "
                                        + "SELECT 'event-1' AS id, CAST(1 AS BIGINT) AS amount, "
                                        + "CAST('2026-01-01 00:00:00.000000' AS TIMESTAMP_LTZ(6)) "
                                        + "AS event_ts, 'source' AS source, 1 AS version"),
                        snippet("flink/BigQueryTableReference.sql", "overview")),
                scenario(
                        "BigQuery table reference query source",
                        withFollowup(
                                "flink/BigQueryTableReference.sql",
                                "query-source",
                                "SELECT * FROM recent_events;")),
                scenario(
                        "BigQuery table reference formatted CDC sequence",
                        setup(
                                "CREATE TEMPORARY VIEW ordered_changes AS "
                                        + "SELECT 'order-1' AS id, CAST(1 AS BIGINT) AS amount, "
                                        + "'0000000000000001' AS formatted_sequence"),
                        snippet("flink/BigQueryTableReference.sql", "formatted-cdc-sequence")),
                scenario(
                        "Bigtable table reference overview and lookup join",
                        List.of(
                                setup(
                                        "CREATE TEMPORARY VIEW staged_profiles AS "
                                                + "SELECT 'user-1' AS user_id, 'Alice' AS name, "
                                                + "'alice@example.com' AS email, "
                                                + "CAST(1 AS BIGINT) AS requests, "
                                                + "CAST('2026-01-01 00:00:00.000' "
                                                + "AS TIMESTAMP_LTZ(3)) AS last_seen"),
                                setup(
                                        "CREATE TABLE events (event_id STRING, user_id STRING, "
                                                + "proc_time AS PROCTIME()) WITH ("
                                                + "'connector' = 'datagen', "
                                                + "'number-of-rows' = '1')")),
                        snippet("flink/BigtableTableReference.sql", "overview"),
                        snippet("flink/BigtableTableReference.sql", "lookup-join")),
                scenario(
                        "Bigtable table reference change stream envelope",
                        snippet("flink/BigtableTableReference.sql", "change-stream-envelope"),
                        snippet(
                                "flink/BigtableTableReference.sql",
                                "unnest-change-stream-entries")),
                scenario(
                        "Bigtable table reference selected-cell source",
                        withFollowup(
                                "flink/BigtableTableReference.sql",
                                "selected-cell-upserts",
                                "SELECT * FROM current_profiles;")),
                scenario(
                        "Bigtable table reference application watermark fragment",
                        fragment(
                                "flink/BigtableTableReference.sql",
                                "application-watermark",
                                DocumentationSqlPlanTest::bigtableWatermarkScenario)),
                scenario(
                        "Bigtable table reference cell timestamps",
                        setup(
                                "CREATE TEMPORARY VIEW staged_profiles AS "
                                        + "SELECT 'user-1' AS user_id, 'Alice' AS name, "
                                        + "'alice@example.com' AS email, "
                                        + "CAST('2026-01-01 00:00:00.000000' "
                                        + "AS TIMESTAMP_LTZ(6)) AS event_time"),
                        snippet("flink/BigtableTableReference.sql", "cell-timestamps")),
                scenario(
                        "Cloud Tasks table reference overview",
                        setup(
                                "CREATE TEMPORARY VIEW staged_orders AS "
                                        + "SELECT 'order-1' AS order_id, "
                                        + "CAST(12.34 AS DECIMAL(12, 2)) AS amount, "
                                        + "'trace-1' AS trace_id, "
                                        + "CAST('2026-01-01 00:00:00.000000' "
                                        + "AS TIMESTAMP_LTZ(6)) AS dispatch_at"),
                        snippet("flink/CloudTasksTableReference.sql", "overview")),
                scenario(
                        "Cloud Tasks table reference ADD JAR",
                        command(
                                "flink/CloudTasksTableReference.sql",
                                "add-jar",
                                "ADD JAR '/path/to/flink-sql-connector-gcp-cloudtasks-1.0.0.jar';")),
                scenario(
                        "Cloud Tasks table reference form values",
                        snippet("flink/CloudTasksTableReference.sql", "repeated-form-values"),
                        snippet(
                                "flink/CloudTasksTableReference.sql",
                                "null-and-empty-form-values")),
                scenario(
                        "Cloud Tasks table reference structured form inputs",
                        List.of(
                                setup(
                                        "CREATE TEMPORARY VIEW incoming_orders AS "
                                                + "SELECT ARRAY['book', 'pen'] AS items, "
                                                + "CAST(ROW('Alice', '100-0001') AS "
                                                + "ROW<name STRING, postal_code STRING>) "
                                                + "AS customer, "
                                                + "MAP['priority', 'high'] AS attributes"),
                                function("TO_API_FORM", new ToApiForm())),
                        snippet("flink/CloudTasksTableReference.sql", "nested-form-names"),
                        snippet("flink/CloudTasksTableReference.sql", "json-form-field"),
                        snippet("flink/CloudTasksTableReference.sql", "custom-form-body")),
                scenario(
                        "Cloud Tasks table reference GET request",
                        List.of(
                                setup(
                                        "CREATE TEMPORARY VIEW pending_searches AS "
                                                + "SELECT 'flink connectors' AS query_text"),
                                DocumentationSqlPlanTest::registerUrlEncodeForFlink1),
                        snippet("flink/CloudTasksTableReference.sql", "get-request")),
                scenario(
                        "Pub/Sub table reference sink and updating-query rejection",
                        List.of(
                                setup(
                                        "CREATE TEMPORARY VIEW staged_orders AS "
                                                + "SELECT 'order-1' AS order_id, 1 AS amount, "
                                                + "'customer-1' AS customer_id"),
                                setup(
                                        "CREATE TEMPORARY VIEW staged AS "
                                                + "SELECT 'order-1' AS id")),
                        snippet("flink/PubSubTableReference.sql", "sink-overview"),
                        negative(
                                "flink/PubSubTableReference.sql",
                                "updating-query-rejected",
                                "orders",
                                "doesn't support consuming update changes")),
                scenario(
                        "Pub/Sub table reference source and resource-name expressions",
                        snippet("flink/PubSubTableReference.sql", "source-overview"),
                        fragment(
                                "flink/PubSubTableReference.sql",
                                "subscription-resource-spellings",
                                DocumentationSqlPlanTest::subscriptionExpressionScenario)),
                scenario(
                        "Pub/Sub table reference timestamp start position",
                        withFollowup(
                                "flink/PubSubTableReference.sql",
                                "timestamp-start-position",
                                "SELECT * FROM orders;")),
                scenario(
                        "Pub/Sub table reference single-subscription auto-creation",
                        withFollowup(
                                "flink/PubSubTableReference.sql",
                                "single-subscription-auto-creation",
                                "SELECT * FROM orders;")),
                scenario(
                        "Pub/Sub table reference multi-subscription auto-creation",
                        withFollowup(
                                "flink/PubSubTableReference.sql",
                                "multiple-subscription-auto-creation",
                                "SELECT * FROM events;")),
                scenario(
                        "Pub/Sub table reference packed subscription map fragment",
                        fragment(
                                "flink/PubSubTableReference.sql",
                                "packed-subscription-map",
                                DocumentationSqlPlanTest::packedPubSubMapScenario)),
                scenario(
                        "Spanner table reference overview",
                        setup(
                                "CREATE TEMPORARY VIEW staged_orders AS "
                                        + "SELECT CAST(1 AS BIGINT) AS order_id, "
                                        + "'Alice' AS customer, "
                                        + "CAST(12.34 AS DECIMAL(38, 9)) AS total, "
                                        + "CAST('2026-01-01 00:00:00.000000000' "
                                        + "AS TIMESTAMP_LTZ(9)) AS updated_at"),
                        snippet("flink/SpannerTableReference.sql", "overview")),
                scenario(
                        "Spanner table reference ADD JAR",
                        command(
                                "flink/SpannerTableReference.sql",
                                "add-jar",
                                "ADD JAR '/path/to/flink-sql-connector-gcp-spanner-1.0.0.jar';")),
                scenario(
                        "Spanner table reference named schema",
                        withFollowup(
                                "flink/SpannerTableReference.sql",
                                "named-schema",
                                "SELECT * FROM sales_orders;")),
                scenario(
                        "Spanner table reference change stream",
                        withFollowup(
                                "flink/SpannerTableReference.sql",
                                "change-stream",
                                "SELECT * FROM order_changes;")),
                scenario(
                        "Spanner table reference schema marker fragment",
                        fragment(
                                "flink/SpannerTableReference.sql",
                                "schema-markers",
                                DocumentationSqlPlanTest::spannerSchemaMarkerScenario)));
    }

    private static Scenario scenario(String name, ValidationStep... steps) {
        return scenario(name, List.of(), steps);
    }

    private static Scenario scenario(String name, ScenarioSetup setup, ValidationStep... steps) {
        return scenario(name, List.of(setup), steps);
    }

    private static Scenario scenario(
            String name, List<ScenarioSetup> setups, ValidationStep... steps) {
        return new Scenario(name, setups, List.of(steps));
    }

    private static ValidationStep snippet(String file, String tag) {
        return new ValidationStep(new Snippet(file, tag), SqlBoundary.IDENTITY, List.of());
    }

    private static ValidationStep command(String file, String tag, String expectedCommand) {
        return new ValidationStep(
                new Snippet(file, tag),
                region -> {
                    assertThat(region.strip()).isEqualTo(expectedCommand);
                    return region;
                },
                List.of());
    }

    private static ValidationStep withFollowup(String file, String tag, String followup) {
        return fragment(file, tag, region -> region + "\n" + followup);
    }

    private static ValidationStep fragment(String file, String tag, SqlBoundary boundary) {
        return new ValidationStep(new Snippet(file, tag), boundary, List.of());
    }

    private static ValidationStep negative(String file, String tag, String... expectedDiagnostics) {
        return new ValidationStep(
                new Snippet(file, tag), SqlBoundary.IDENTITY, List.of(expectedDiagnostics));
    }

    private static ScenarioSetup setup(String sql) {
        return (execution, table) -> plan((TableEnvironmentInternal) table, sql);
    }

    private static ScenarioSetup function(String name, ScalarFunction function) {
        return (execution, table) -> table.createTemporarySystemFunction(name, function);
    }

    private static void registerUrlEncodeForFlink1(
            StreamExecutionEnvironment execution, StreamTableEnvironment table) {
        if ("flink1".equals(System.getProperty("flink.compat"))) {
            table.createTemporarySystemFunction("URL_ENCODE", new UrlEncode());
        }
        table.explainSql("SELECT URL_ENCODE('flink connectors')");
    }

    private static String bigtableWatermarkScenario(String region) {
        return "CREATE TABLE watermarked_changes (\n"
                + "  name STRING,\n"
                + "  profile_id STRING NOT NULL,\n"
                + region
                + ",\n"
                + "  PRIMARY KEY (profile_id) NOT ENFORCED\n"
                + ") WITH (\n"
                + "  'connector' = 'bigtable',\n"
                + "  'project' = 'my-project',\n"
                + "  'instance' = 'my-instance',\n"
                + "  'table' = 'profiles',\n"
                + "  'scan.mode' = 'change-stream',\n"
                + "  'scan.change-stream.changelog-mode' = 'selected-cell',\n"
                + "  'scan.app-profile-id' = 'single-cluster-profile',\n"
                + "  'scan.change-stream.selected-cell.family' = 'state',\n"
                + "  'scan.change-stream.selected-cell.qualifier-base64' = 'Y3VycmVudA==',\n"
                + "  'scan.change-stream.selected-cell.source-cluster-id' = 'cluster-a',\n"
                + "  'value.format' = 'json'\n"
                + ");";
    }

    private static String bigQuerySinkMethodScenario(String region) {
        return "SET 'execution.checkpointing.interval' = '5 min';\n"
                + "CREATE TABLE analytics_events (\n"
                + "  event_id STRING,\n"
                + "  amount BIGINT\n"
                + ") WITH (\n"
                + "  'connector' = 'bigquery',\n"
                + "  'project' = 'my-project',\n"
                + "  'dataset' = 'analytics',\n"
                + "  'table' = 'events',\n"
                + region
                + "\n);\n"
                + "INSERT INTO analytics_events\n"
                + "SELECT event_id, amount\n"
                + "FROM (VALUES ('event-1', CAST(42 AS BIGINT))) "
                + "AS staged_events(event_id, amount);";
    }

    private static String subscriptionExpressionScenario(String region) {
        return Stream.of(region.split("\\R"))
                .filter(line -> !line.isBlank())
                .map(
                        line -> {
                            int comment = line.indexOf("--");
                            String expression = comment < 0 ? line : line.substring(0, comment);
                            return "SELECT "
                                    + expression.stripTrailing()
                                    + " FROM incoming_orders;";
                        })
                .collect(Collectors.joining("\n"));
    }

    private static String packedPubSubMapScenario(String region) {
        return "CREATE TABLE packed_subscriptions (id STRING) WITH (\n"
                + "  'connector' = 'pubsub',\n"
                + "  'project' = 'my-project',\n"
                + "  'format' = 'json',\n"
                + region
                + "\n);\n"
                + "SELECT * FROM packed_subscriptions;";
    }

    private static String spannerSchemaMarkerScenario(String region) {
        String requiredOptions =
                "WITH (\n"
                        + "  'connector' = 'spanner',\n"
                        + "  'project' = 'my-project',\n"
                        + "  'instance' = 'my-instance',\n"
                        + "  'database' = 'orders-db',\n"
                        + "  'table' = 'schema_values',\n";
        String options =
                region.replaceFirst("WITH \\(\\R", Matcher.quoteReplacement(requiredOptions));
        return "CREATE TABLE schema_values (\n"
                + "  id STRING,\n"
                + "  related_ids ARRAY<STRING>,\n"
                + "  metadata STRING,\n"
                + "  payloads ARRAY<STRING>,\n"
                + "  event BYTES,\n"
                + "  status BIGINT\n"
                + ") "
                + options
                + ";\n"
                + "SELECT * FROM schema_values;";
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
        private final List<ValidationStep> steps;

        private Scenario(String name, List<ScenarioSetup> setups, List<ValidationStep> steps) {
            this.name = name;
            this.setups = setups;
            this.steps = steps;
        }

        private List<ScenarioSetup> setups() {
            return setups;
        }

        private List<ValidationStep> steps() {
            return steps;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    private static final class ValidationStep {
        private final Snippet snippet;
        private final SqlBoundary boundary;
        private final List<String> expectedDiagnostics;

        private ValidationStep(
                Snippet snippet, SqlBoundary boundary, List<String> expectedDiagnostics) {
            this.snippet = snippet;
            this.boundary = boundary;
            this.expectedDiagnostics = expectedDiagnostics;
        }

        private Snippet snippet() {
            return snippet;
        }

        private String sql() {
            return boundary.enclose(readRegion(snippet));
        }

        private List<String> expectedDiagnostics() {
            return expectedDiagnostics;
        }
    }

    @FunctionalInterface
    private interface SqlBoundary {
        SqlBoundary IDENTITY = region -> region;

        String enclose(String region);
    }

    @FunctionalInterface
    private interface ScenarioSetup {
        void apply(StreamExecutionEnvironment execution, StreamTableEnvironment table);
    }

    public static final class ToApiForm extends ScalarFunction {
        public String eval(String[] items, Map<String, String> attributes) {
            return "";
        }
    }

    public static final class UrlEncode extends ScalarFunction {
        public String eval(String value) {
            return value;
        }
    }
}
