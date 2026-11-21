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

package io.github.flink.gcp.connector.pubsub.table.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import io.github.flink.gcp.connector.pubsub.source.streamingpull.PubSubStreamingPullSource;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What the planner does with a {@code pubsub} table's {@code WATERMARK}, with and without {@code
 * scan.watermark-push-down.enabled}, on the Flink line the build runs (#1837).
 *
 * <p>The planner is the subject here, not the source: whether a watermark is pushed, which provider
 * branch translates it, and whether the strategy reaching {@code fromSource} carries the FLIP-296
 * options are all planner decisions this connector only enables. The tests therefore plan real SQL
 * through the planner loader and inspect the plan text and the translated source transformation.
 * Nothing runs; the emulator endpoint only keeps the source build away from application-default
 * credentials.
 */
class PubSubWatermarkPushDownPlanTest {

    private static final String WATERMARKED_COLUMNS =
            "id INT, ts TIMESTAMP_LTZ(3), WATERMARK FOR ts AS ts - INTERVAL '5' SECOND";

    private static final String ALIGNMENT =
            ", 'scan.watermark.alignment.group' = 'aligned',"
                    + " 'scan.watermark.alignment.max-drift' = '1 min'";

    @Test
    void aTableThatDoesNotOptInKeepsThePlannersWatermarkOperator() {
        StreamTableEnvironment table = environment();
        createTable(table, WATERMARKED_COLUMNS, ALIGNMENT);

        String plan = optimizedPlan(table, "SELECT id, ts FROM events");

        assertThat(plan).contains("WatermarkAssigner");
        // The alignment options are accepted and do nothing, which is what the docs say about a
        // table that has not opted in.
        assertThat(sourceStrategy(table, "SELECT id, ts FROM events"))
                .satisfies(
                        strategy ->
                                assertThat(strategy.getAlignmentParameters().isEnabled())
                                        .isFalse());
    }

    @Test
    void anOptedInTableGeneratesTheWatermarkInTheSourceWithAlignment() {
        StreamTableEnvironment table = environment();
        createTable(
                table,
                WATERMARKED_COLUMNS,
                ", 'scan.watermark-push-down.enabled' = 'true'" + ALIGNMENT);

        String plan = optimizedPlan(table, "SELECT id, ts FROM events");

        assertThat(plan).doesNotContain("WatermarkAssigner").contains("watermark=[");
        WatermarkStrategy<?> strategy = sourceStrategy(table, "SELECT id, ts FROM events");
        assertThat(strategy.getAlignmentParameters().isEnabled()).isTrue();
        assertThat(strategy.getAlignmentParameters().getWatermarkGroup()).isEqualTo("aligned");
        assertThat(strategy.getAlignmentParameters().getMaxAllowedWatermarkDrift())
                .isEqualTo(60_000L);
    }

    @Test
    void aWatermarkOnThePublishTimeMetadataColumnIsPushed() {
        StreamTableEnvironment table = environment();
        createTable(
                table,
                "id INT, ts TIMESTAMP_LTZ(3) METADATA FROM 'publish-time' VIRTUAL,"
                        + " WATERMARK FOR ts AS ts",
                ", 'scan.watermark-push-down.enabled' = 'true'");

        String plan = optimizedPlan(table, "SELECT id, ts FROM events");

        assertThat(plan).doesNotContain("WatermarkAssigner").contains("watermark=[");
        assertThat(sourceTransformation(table, "SELECT id, ts FROM events").getSource())
                .isInstanceOf(PubSubStreamingPullSource.class);
    }

    @Test
    void aWatermarkOnAComputedColumnIsPushed() {
        StreamTableEnvironment table = environment();
        createTable(
                table,
                "id INT, millis BIGINT, ts AS TO_TIMESTAMP_LTZ(millis, 3),"
                        + " WATERMARK FOR ts AS ts",
                ", 'scan.watermark-push-down.enabled' = 'true'");

        String plan = optimizedPlan(table, "SELECT id, ts FROM events");

        assertThat(plan).doesNotContain("WatermarkAssigner").contains("watermark=[");
    }

    @Test
    void thePushedScanKeepsTheConfiguredParallelism() {
        StreamTableEnvironment table = environment();
        createTable(
                table,
                WATERMARKED_COLUMNS,
                ", 'scan.watermark-push-down.enabled' = 'true', 'scan.parallelism' = '2'");

        assertThat(sourceTransformation(table, "SELECT id, ts FROM events").getParallelism())
                .isEqualTo(2);
    }

    @Test
    void thePushedScanCarriesThePlannersSourceUid() {
        // Flink 2.3 leaves the uid of a DataStreamScanProvider's stream to the provider for plans
        // it compiles itself; the provider asks generateUid("source"), which is the name the
        // planner gives a SourceProvider's scan. The node-id prefix is a JVM-wide counter, so the
        // shape is what can be compared across two plans.
        String query = "SELECT id, ts FROM events";
        StreamTableEnvironment withoutPushDown = environment();
        withoutPushDown.getConfig().set("table.exec.uid.generation", "ALWAYS");
        createTable(withoutPushDown, WATERMARKED_COLUMNS, "");
        StreamTableEnvironment withPushDown = environment();
        withPushDown.getConfig().set("table.exec.uid.generation", "ALWAYS");
        createTable(
                withPushDown, WATERMARKED_COLUMNS, ", 'scan.watermark-push-down.enabled' = 'true'");

        assertThat(sourceTransformation(withoutPushDown, query).getUid()).matches("\\d+_source");
        assertThat(sourceTransformation(withPushDown, query).getUid()).matches("\\d+_source");
    }

    @Test
    void rejectsASessionIdleTimeoutThePlannerWouldAddToThePushedStrategy() {
        StreamTableEnvironment table = environment();
        table.getConfig().set("table.exec.source.idle-timeout", "30 s");
        createTable(table, WATERMARKED_COLUMNS, ", 'scan.watermark-push-down.enabled' = 'true'");

        assertThatThrownBy(() -> optimizedPlan(table, "SELECT id, ts FROM events"))
                .hasStackTraceContaining(
                        "'table.exec.source.idle-timeout' = 'PT30S' would add source idleness")
                .hasStackTraceContaining("Set 'scan.watermark.idle-timeout' = '0' on this table");
    }

    @Test
    void rejectsASessionIdleTimeoutSetAfterTheQueryWasDefined() {
        // The planner reads the session option when it optimizes, which for a Table defined with
        // sqlQuery is after any change made in between; the check must read it at that moment too.
        StreamTableEnvironment table = environment();
        createTable(table, WATERMARKED_COLUMNS, ", 'scan.watermark-push-down.enabled' = 'true'");
        Table query = table.sqlQuery("SELECT id, ts FROM events");
        table.getConfig().set("table.exec.source.idle-timeout", "30 s");

        assertThatThrownBy(query::explain)
                .hasStackTraceContaining(
                        "'table.exec.source.idle-timeout' = 'PT30S' would add source idleness");
    }

    @Test
    void acceptsASessionIdleTimeoutClearedAfterTheQueryWasDefined() {
        StreamTableEnvironment table = environment();
        table.getConfig().set("table.exec.source.idle-timeout", "30 s");
        createTable(table, WATERMARKED_COLUMNS, ", 'scan.watermark-push-down.enabled' = 'true'");
        Table query = table.sqlQuery("SELECT id, ts FROM events");
        table.getConfig().set("table.exec.source.idle-timeout", "0 ms");

        String plan = query.explain();

        assertThat(plan.substring(plan.indexOf("== Optimized Physical Plan ==")))
                .contains("watermark=[")
                .doesNotContain("idletimeout");
    }

    @Test
    void rejectsATableIdleTimeout() {
        StreamTableEnvironment table = environment();
        createTable(
                table,
                WATERMARKED_COLUMNS,
                ", 'scan.watermark-push-down.enabled' = 'true',"
                        + " 'scan.watermark.idle-timeout' = '1 min'");

        assertThatThrownBy(() -> optimizedPlan(table, "SELECT id, ts FROM events"))
                .hasStackTraceContaining(
                        "'scan.watermark.idle-timeout' = 'PT1M' would add source idleness");
    }

    @Test
    void aZeroTableIdleTimeoutOverridesTheSessionOne() {
        StreamTableEnvironment table = environment();
        table.getConfig().set("table.exec.source.idle-timeout", "30 s");
        createTable(
                table,
                WATERMARKED_COLUMNS,
                ", 'scan.watermark-push-down.enabled' = 'true',"
                        + " 'scan.watermark.idle-timeout' = '0'");

        String plan = optimizedPlan(table, "SELECT id, ts FROM events");

        // With the rejection test above, this holds the factory's copy of the planner's precedence
        // rule to the planner's own: a factory that disagrees either rejects this table or lets
        // that one through. The digest names an idle timeout the planner adds.
        assertThat(plan).contains("watermark=[").doesNotContain("idletimeout");
    }

    @Test
    void aTableThatDoesNotOptInKeepsItsSessionIdleTimeout() {
        StreamTableEnvironment table = environment();
        table.getConfig().set("table.exec.source.idle-timeout", "30 s");
        createTable(table, WATERMARKED_COLUMNS, "");

        assertThat(optimizedPlan(table, "SELECT id, ts FROM events")).contains("WatermarkAssigner");
    }

    /** The optimized plan alone: the abstract syntax tree before it always has the assigner. */
    private static String optimizedPlan(StreamTableEnvironment table, String query) {
        String plan = table.explainSql(query);
        int start = plan.indexOf("== Optimized Physical Plan ==");
        assertThat(start).isNotNegative();
        return plan.substring(start);
    }

    private static StreamTableEnvironment environment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        return StreamTableEnvironment.create(env);
    }

    private static void createTable(StreamTableEnvironment table, String columns, String options) {
        table.executeSql(
                "CREATE TABLE events ("
                        + columns
                        + ") WITH ('connector' = 'pubsub', 'project' = 'p',"
                        + " 'subscription' = 'events-sub', 'format' = 'json',"
                        + " 'emulator-endpoint' = 'localhost:8085'"
                        + options
                        + ")");
    }

    private static WatermarkStrategy<?> sourceStrategy(StreamTableEnvironment table, String query) {
        return sourceTransformation(table, query).getWatermarkStrategy();
    }

    private static SourceTransformation<?, ?, ?> sourceTransformation(
            StreamTableEnvironment table, String query) {
        Transformation<Row> sink = table.toDataStream(table.sqlQuery(query)).getTransformation();
        List<SourceTransformation<?, ?, ?>> sources =
                sink.getTransitivePredecessors().stream()
                        .filter(SourceTransformation.class::isInstance)
                        .map(t -> (SourceTransformation<?, ?, ?>) t)
                        .collect(Collectors.toList());
        assertThat(sources).hasSize(1);
        return sources.get(0);
    }
}
