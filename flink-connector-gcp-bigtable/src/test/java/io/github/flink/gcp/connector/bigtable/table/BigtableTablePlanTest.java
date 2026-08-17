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

package io.github.flink.gcp.connector.bigtable.table;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.abilities.SupportsSourceWatermark;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import io.github.flink.gcp.connector.bigtable.table.source.BigtableChangeStreamDynamicSource;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * When the connector's DDL-shape rejections actually reach a user, and what the planner puts
 * between a changelog and this sink.
 *
 * <p>Deliberately not an ITCase: every case here throws, or is explained, while the job graph is
 * being built, before a client is opened, so the emulator endpoint below is a string that only has
 * to parse.
 *
 * <p>The question it answers is one {@code FactoryMocks} cannot, because that harness calls the
 * factory directly: a {@code CREATE TABLE} does not call the factory at all — it registers a
 * catalog entry — so a table this connector could never write is <em>accepted</em> by the DDL and
 * refused when an {@code INSERT} over it is planned. Any documentation saying such a table is
 * "rejected when the table is created" is wrong, and this is what keeps that from being written
 * again.
 */
class BigtableTablePlanTest {

    private static final String WITH_CLAUSE =
            "WITH (\n"
                    + "  'connector' = 'bigtable',\n"
                    + "  'project' = 'my-project',\n"
                    + "  'instance' = 'my-instance',\n"
                    + "  'table' = 'my-table',\n"
                    + "  'emulator-endpoint' = 'localhost:1'\n"
                    + ")";

    private static final String CHANGE_STREAM_WITH_CLAUSE =
            "WITH (\n"
                    + "  'connector' = 'bigtable',\n"
                    + "  'project' = 'my-project',\n"
                    + "  'instance' = 'my-instance',\n"
                    + "  'table' = 'my-table',\n"
                    + "  'scan.mode' = 'change-stream',\n"
                    + "  'scan.change-stream.changelog-mode' = 'envelope',\n"
                    + "  'scan.app-profile-id' = 'single-cluster-profile'\n"
                    + ")";

    private static TableEnvironment tableEnvironment() {
        return TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    }

    @Test
    void aColumnWithNoCellEncodingIsAcceptedByCreateTableAndRefusedWhenAnInsertIsPlanned() {
        TableEnvironment tEnv = tableEnvironment();

        assertThatCode(
                        () ->
                                tEnv.executeSql(
                                        "CREATE TABLE bt (\n"
                                                + "  rowkey STRING,\n"
                                                + "  cf1 ROW<tags ARRAY<STRING>>,\n"
                                                + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                                                + ") "
                                                + WITH_CLAUSE))
                .doesNotThrowAnyException();

        assertThatThrownBy(() -> tEnv.executeSql("INSERT INTO bt VALUES ('r1', ROW(ARRAY['a']))"))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("no Bigtable cell encoding");
    }

    @Test
    void malformedBase64IsAcceptedByCreateTableAndRefusedWhenASelectIsPlanned() {
        TableEnvironment tEnv = tableEnvironment();
        String options =
                WITH_CLAUSE.replace(
                        "\n)",
                        ",\n"
                                + "  'scan.row-key-encoding' = 'BASE64',\n"
                                + "  'scan.row-prefix' = 'YQ'\n"
                                + ")");

        assertThatCode(
                        () ->
                                tEnv.executeSql(
                                        "CREATE TABLE bt (\n"
                                                + "  rowkey STRING,\n"
                                                + "  cf ROW<q STRING>\n"
                                                + ") "
                                                + options))
                .doesNotThrowAnyException();

        assertThatThrownBy(() -> tEnv.explainSql("SELECT * FROM bt"))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("'scan.row-prefix'")
                .hasStackTraceContaining("canonical padded RFC 4648 standard Base64");
    }

    @Test
    void multipleRangeGrammarPassesThroughASqlWithClause() {
        TableEnvironment tEnv = tableEnvironment();
        String options =
                WITH_CLAUSE.replace(
                        "\n)", ",\n" + "  'scan.row-ranges' = '[a\\,b,c\\;d);[x,z)'\n" + ")");
        tEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  rowkey STRING,\n"
                        + "  cf ROW<q STRING>\n"
                        + ") "
                        + options);

        assertThatCode(() -> tEnv.explainSql("SELECT * FROM bt")).doesNotThrowAnyException();
    }

    @Test
    void malformedMultipleRangeIsAcceptedByCreateTableAndRefusedWhenASelectIsPlanned() {
        TableEnvironment tEnv = tableEnvironment();
        String options =
                WITH_CLAUSE.replace("\n)", ",\n" + "  'scan.row-ranges' = '[a,b);[z,a)'\n" + ")");

        assertThatCode(
                        () ->
                                tEnv.executeSql(
                                        "CREATE TABLE bt (\n"
                                                + "  rowkey STRING,\n"
                                                + "  cf ROW<q STRING>\n"
                                                + ") "
                                                + options))
                .doesNotThrowAnyException();

        assertThatThrownBy(() -> tEnv.explainSql("SELECT * FROM bt"))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("'scan.row-ranges' entry 2")
                .hasStackTraceContaining("decoded start greater than its end");
    }

    @Test
    void theDocumentationsOwnExampleParses() {
        // Copied from docs/content/docs/connectors/table/bigtable.md, which is the first code
        // block a reader meets. Pinned because a column family is a *column name* here, so the
        // example is one reserved word away from not parsing — 'identity' was, and the page shipped
        // it until this test was written.
        TableEnvironment tEnv = tableEnvironment();

        assertThatCode(
                        () ->
                                tEnv.executeSql(
                                        "CREATE TABLE profiles (\n"
                                                + "  rowkey STRING,\n"
                                                + "  profile ROW<name STRING, email STRING>,\n"
                                                + "  usage ROW<requests BIGINT, last_seen"
                                                + " TIMESTAMP_LTZ(3)>,\n"
                                                + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                                                + ") "
                                                + WITH_CLAUSE))
                .doesNotThrowAnyException();
    }

    @Test
    void changeStreamMetadataAndOrderedEntryExpansionArePlanned() {
        TableEnvironment tEnv = tableEnvironment();
        createEnvelopeChangeStreamTable(tEnv, "mutations", "");

        String metadataPlan =
                tEnv.explainSql(
                        "SELECT low_watermark, mutation_type, committed_at, tie FROM mutations");
        String entriesPlan =
                tEnv.explainSql(
                        "SELECT row_key, entry_index, kind, mutation_type "
                                + "FROM mutations CROSS JOIN UNNEST(entries) AS entry_table("
                                + "entry_index, kind, family, qualifier, entry_timestamp,"
                                + " entry_value, delete_range)");

        assertThat(metadataPlan)
                .contains("low_watermark", "mutation_type", "committed_at", "tie")
                .contains("CAST");
        assertThat(entriesPlan)
                .contains("entry_index", "kind", "mutation_type")
                .contains("Uncollect");
    }

    @Test
    void changeStreamDoesNotPushDownNativeSourceWatermarks() {
        TableEnvironment tEnv = tableEnvironment();
        createEnvelopeChangeStreamTable(
                tEnv, "native_watermark", ",\n  WATERMARK FOR committed_at AS SOURCE_WATERMARK()");

        assertThat(
                        SupportsSourceWatermark.class.isAssignableFrom(
                                BigtableChangeStreamDynamicSource.class))
                .isFalse();
        assertThat(tEnv.explainSql("SELECT * FROM native_watermark"))
                .contains(
                        "WatermarkAssigner(rowtime=[committed_at],"
                                + " watermark=[SOURCE_WATERMARK()])");
    }

    @Test
    void changeStreamAcceptsAJobOwnedWatermarkExpression() {
        TableEnvironment tEnv = tableEnvironment();
        createEnvelopeChangeStreamTable(
                tEnv,
                "job_watermark",
                ",\n  WATERMARK FOR committed_at AS committed_at - INTERVAL '5' MINUTE");

        assertThat(tEnv.explainSql("SELECT * FROM job_watermark"))
                .contains("WatermarkAssigner", "300000:INTERVAL MINUTE")
                .doesNotContain("SOURCE_WATERMARK()");
    }

    private static void createEnvelopeChangeStreamTable(
            TableEnvironment tEnv, String tableName, String watermarkDefinition) {
        tEnv.executeSql(
                "CREATE TABLE "
                        + tableName
                        + " (\n"
                        + "  row_key BYTES,\n"
                        + "  entries ARRAY<ROW<\n"
                        + "    entry_index INT,\n"
                        + "    kind STRING,\n"
                        + "    family STRING,\n"
                        + "    qualifier ROW<value_type STRING, bytes_value BYTES, long_value"
                        + " BIGINT>,\n"
                        + "    `timestamp` ROW<value_type STRING, bytes_value BYTES, long_value"
                        + " BIGINT>,\n"
                        + "    `value` ROW<value_type STRING, bytes_value BYTES, long_value"
                        + " BIGINT>,\n"
                        + "    delete_range ROW<start_bound STRING, start_micros BIGINT,"
                        + " end_bound STRING, end_micros BIGINT>\n"
                        + "  >>,\n"
                        + "  mutation_type STRING NOT NULL METADATA FROM 'mutation-type'"
                        + " VIRTUAL,\n"
                        + "  source_cluster STRING METADATA FROM 'source-cluster-id' VIRTUAL,\n"
                        + "  committed_at TIMESTAMP_LTZ(3) NOT NULL METADATA FROM"
                        + " 'commit-timestamp' VIRTUAL,\n"
                        + "  tie BIGINT NOT NULL METADATA FROM 'tie-breaker' VIRTUAL,\n"
                        + "  low_watermark TIMESTAMP_LTZ(9) NOT NULL METADATA FROM"
                        + " 'estimated-low-watermark' VIRTUAL"
                        + watermarkDefinition
                        + "\n"
                        + ") "
                        + CHANGE_STREAM_WITH_CLAUSE);
    }

    @Test
    void writableTimestampMetadataAtOtherPrecisionsIsCastToMicroseconds() {
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE bt3 (\n"
                        + "  rowkey STRING,\n"
                        + "  cf ROW<v STRING>,\n"
                        + "  cell_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'\n"
                        + ") "
                        + WITH_CLAUSE);
        tEnv.executeSql(
                "CREATE TABLE bt9 (\n"
                        + "  rowkey STRING,\n"
                        + "  cf ROW<v STRING>,\n"
                        + "  cell_timestamp TIMESTAMP_LTZ(9) METADATA FROM 'timestamp'\n"
                        + ") "
                        + WITH_CLAUSE);

        String millisPlan =
                tEnv.explainSql(
                        "INSERT INTO bt3 VALUES ('r1', ROW('v'),"
                                + " CAST('2023-11-14 22:13:20.123' AS TIMESTAMP_LTZ(3)))");
        String nanosPlan =
                tEnv.explainSql(
                        "INSERT INTO bt9 VALUES ('r1', ROW('v'),"
                                + " CAST('2023-11-14 22:13:20.123456789' AS TIMESTAMP_LTZ(9)))");

        assertThat(millisPlan)
                .contains("TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)")
                .doesNotContain("TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)");
        assertThat(nanosPlan)
                .contains("20.123456")
                .contains("TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)")
                .doesNotContain("20.123456789")
                .doesNotContain("TIMESTAMP_WITH_LOCAL_TIME_ZONE(9)");
    }

    @Test
    void theRowIsCompletedForADeleteAndForNothingElse() {
        // #470. Without a declared key the planner keys its upserts on whatever the query is
        // unique by — here 'id', a column the Bigtable table does not even have — so a key-only
        // delete would otherwise reach the sink with the row-key column null. Asserting the plan
        // rather than the changelog mode is what makes this portable: ChangelogMode.keyOnlyDeletes
        // does not exist on the 1.20 LTS build, and naming it here would break that build and not
        // this one.
        //
        // The two halves are one test on purpose. Alone, the second is an assertion that cannot
        // fail: an append-only VALUES query gets no ChangelogNormalize whatever the sink answers —
        // measured, it passes under insertOnly(), upsert(), upsert(false) and all(). Its meaning
        // comes entirely from being the *same* string, through the *same* environment, that the
        // first half has just found present. That also makes a rename of the operator loud in both
        // directions rather than silently vacuous in the negative one.
        //
        // The key is pinned as well as the node: a normalize appearing for some unrelated reason
        // is not the one this test is about.
        StreamTableEnvironment tEnv = streamTableEnvironmentWithAnUpsertSourceKeyedOnId();

        assertThat(
                        planOfInsertInto(
                                tEnv,
                                "no_pk",
                                "SELECT rowkey, CAST(ROW(v) AS ROW<v STRING>)" + " FROM src"))
                .contains("ChangelogNormalize(key=[id])");
        assertThat(
                        planOfInsertInto(
                                tEnv, "no_pk", "VALUES ('r1', CAST(ROW('x') AS ROW<v STRING>))"))
                .doesNotContain("ChangelogNormalize");
    }

    /**
     * A streaming environment holding a {@code no_pk} Bigtable table with no {@code PRIMARY KEY}
     * and an upsert view {@code src} keyed on {@code id}, which is not the row-key column.
     */
    private static StreamTableEnvironment streamTableEnvironmentWithAnUpsertSourceKeyedOnId() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(
                "CREATE TABLE no_pk (\n"
                        + "  rowkey STRING,\n"
                        + "  cf1 ROW<v STRING>\n"
                        + ") "
                        + WITH_CLAUSE);

        DataStream<Row> changelog =
                env.fromData(
                        Collections.singletonList(
                                Row.ofKind(RowKind.DELETE, "id1", "rk1", "hello")),
                        Types.ROW_NAMED(
                                new String[] {"id", "rowkey", "v"},
                                Types.STRING,
                                Types.STRING,
                                Types.STRING));
        tEnv.createTemporaryView(
                "src",
                tEnv.fromChangelogStream(
                        changelog,
                        Schema.newBuilder()
                                .column("id", DataTypes.STRING().notNull())
                                .column("rowkey", DataTypes.STRING())
                                .column("v", DataTypes.STRING())
                                .primaryKey("id")
                                .build(),
                        ChangelogMode.upsert()));
        return tEnv;
    }

    private static String planOfInsertInto(
            StreamTableEnvironment tEnv, String table, String query) {
        return tEnv.explainSql("INSERT INTO " + table + " " + query);
    }

    @Test
    void aTableWithNoColumnFamilyIsRefusedWhenAnInsertIsPlanned() {
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE keys_only (\n"
                        + "  rowkey STRING,\n"
                        + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                        + ") "
                        + WITH_CLAUSE);

        assertThatThrownBy(() -> tEnv.executeSql("INSERT INTO keys_only VALUES ('r1')"))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("needs at least one column family");

        // The deliberate asymmetry: the same table a write is refused over is a legitimate thing
        // to read, served by a keys-only filter chain.
        assertThatCode(() -> tEnv.explainSql("SELECT * FROM keys_only")).doesNotThrowAnyException();
    }

    @Test
    void aProjectionIsPushedIntoTheScan() {
        // The planner rewrites the scan only when the source declares the ability and accepts the
        // push; the projected column list appearing inside the TableSourceScan node is what says
        // applyProjection ran, rather than the planner projecting after a full read.
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  rowkey STRING,\n"
                        + "  cf1 ROW<v STRING>,\n"
                        + "  cf2 ROW<m DOUBLE>\n"
                        + ") "
                        + WITH_CLAUSE);

        assertThat(tEnv.explainSql("SELECT cf1 FROM bt")).contains("project=[cf1]");
        assertThat(tEnv.explainSql("SELECT rowkey FROM bt")).contains("project=[rowkey]");
        assertThat(tEnv.explainSql("SELECT rowkey, cf2 FROM bt")).contains("project=[rowkey, cf2]");
    }

    @Test
    void exactRowKeyFiltersAreConsumedByTheTableSource() {
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  rowkey STRING,\n"
                        + "  cf1 ROW<v STRING>\n"
                        + ") "
                        + WITH_CLAUSE);

        String plan = tEnv.explainSql("SELECT cf1 FROM bt WHERE rowkey >= 'b' AND rowkey < 'm'");

        assertThat(plan)
                // Flink renders AND in lower case through 2.3 and in upper case on 2.4-SNAPSHOT.
                // Which spelling it picks is not what this test is about, so both are matched.
                .containsIgnoringCase("filter=[and(>=(rowkey")
                .contains("<(rowkey")
                .doesNotContain("Calc(select=[cf1]");

        assertThat(tEnv.explainSql("SELECT cf1 FROM bt WHERE rowkey = ''"))
                .contains("Calc(select=[cf1], where=[=(rowkey");
    }

    @Test
    void aQualifierPrefilterLeavesTheSqlPredicateAsAResidual() {
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  rowkey STRING,\n"
                        + "  cf1 ROW<v STRING>\n"
                        + ") "
                        + WITH_CLAUSE);

        String plan = tEnv.explainSql("SELECT rowkey FROM bt WHERE cf1.v = 'alice'");

        assertThat(plan)
                .contains("filter=[=(cf1.v")
                .contains("Calc(select=[rowkey], where=[=(cf1.v");
    }

    @Test
    void equalityAccountsForDecoderAliasesAndDoesNotInventByteOrdering() {
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE ints (\n"
                        + "  rowkey INT,\n"
                        + "  cf1 ROW<v STRING>\n"
                        + ") "
                        + WITH_CLAUSE);
        tEnv.executeSql(
                "CREATE TABLE doubles (\n"
                        + "  rowkey DOUBLE,\n"
                        + "  cf1 ROW<v STRING>\n"
                        + ") "
                        + WITH_CLAUSE);
        tEnv.executeSql(
                "CREATE TABLE booleans (\n"
                        + "  rowkey BOOLEAN,\n"
                        + "  cf1 ROW<v STRING>\n"
                        + ") "
                        + WITH_CLAUSE);
        tEnv.executeSql(
                "CREATE TABLE decimals (\n"
                        + "  rowkey DECIMAL(8, 2),\n"
                        + "  cf1 ROW<v STRING>\n"
                        + ") "
                        + WITH_CLAUSE);

        assertThat(tEnv.explainSql("SELECT cf1 FROM ints WHERE rowkey = 7"))
                .contains("filter=[=(rowkey")
                .doesNotContain("Calc(select=[cf1]");
        assertThat(tEnv.explainSql("SELECT cf1 FROM ints WHERE rowkey < 7"))
                .contains("Calc(select=[cf1], where=[<(rowkey");
        assertThat(tEnv.explainSql("SELECT cf1 FROM doubles WHERE rowkey = 0.0"))
                .contains("Calc(select=[cf1], where=[=(rowkey");
        assertThat(tEnv.explainSql("SELECT cf1 FROM booleans WHERE rowkey = TRUE"))
                .contains("Calc(select=[cf1], where=[rowkey");
        assertThat(tEnv.explainSql("SELECT cf1 FROM decimals WHERE rowkey = 7.00"))
                .contains("Calc(select=[cf1], where=[=(rowkey");
    }

    @Test
    void aTemporalJoinUsesTheRowKeyLookupAfterProjection() {
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE facts (\n"
                        + "  id STRING,\n"
                        + "  lookup_key STRING,\n"
                        + "  event_time AS PROCTIME()\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'number-of-rows' = '1'\n"
                        + ")");
        tEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  cf1 ROW<v STRING>,\n"
                        + "  rowkey STRING,\n"
                        + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                        + ") "
                        + WITH_CLAUSE);

        assertThat(
                        tEnv.explainSql(
                                "SELECT f.id, b.cf1.v FROM facts AS f "
                                        + "LEFT JOIN bt FOR SYSTEM_TIME AS OF f.event_time AS b "
                                        + "ON f.lookup_key = b.rowkey"))
                .contains("LookupJoin")
                .contains("lookup=[rowkey=lookup_key]");
    }

    @Test
    void aTemporalJoinKeepsARightSidePredicateInTheLookupOperator() {
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE facts (\n"
                        + "  lookup_key STRING,\n"
                        + "  event_time AS PROCTIME()\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'number-of-rows' = '1'\n"
                        + ")");
        tEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  rowkey STRING,\n"
                        + "  cf1 ROW<v STRING>,\n"
                        + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                        + ") "
                        + WITH_CLAUSE);

        assertThat(
                        tEnv.explainSql(
                                "SELECT f.lookup_key, b.cf1.v FROM facts AS f "
                                        + "LEFT JOIN bt FOR SYSTEM_TIME AS OF f.event_time AS b "
                                        + "ON f.lookup_key = b.rowkey AND b.cf1.v = 'keep'"))
                .contains("LookupJoin")
                .contains("where=[=(cf1.v")
                .doesNotContain("filter=[=(cf1.v");
    }

    @Test
    void aTemporalJoinOnAColumnFamilyIsRejected() {
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE facts (\n"
                        + "  lookup_value STRING,\n"
                        + "  event_time AS PROCTIME()\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'number-of-rows' = '1'\n"
                        + ")");
        tEnv.executeSql(
                "CREATE TABLE bt (\n"
                        + "  rowkey STRING,\n"
                        + "  cf1 ROW<v STRING>,\n"
                        + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                        + ") "
                        + WITH_CLAUSE);

        assertThatThrownBy(
                        () ->
                                tEnv.explainSql(
                                        "SELECT b.rowkey FROM facts AS f "
                                                + "LEFT JOIN bt FOR SYSTEM_TIME AS OF f.event_time"
                                                + " AS b ON f.lookup_value = b.cf1.v"))
                .isInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("row-key column 'rowkey'");
    }

    @Test
    void theDocumentationsOwnExampleIsSelectable() {
        // The read half of theDocumentationsOwnExampleParses: before the table source existed,
        // this SELECT failed with "can only be used as a sink".
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                "CREATE TABLE profiles (\n"
                        + "  rowkey STRING,\n"
                        + "  profile ROW<name STRING, email STRING>,\n"
                        + "  usage ROW<requests BIGINT, last_seen TIMESTAMP_LTZ(3)>,\n"
                        + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                        + ") "
                        + WITH_CLAUSE);

        assertThatCode(() -> tEnv.explainSql("SELECT * FROM profiles")).doesNotThrowAnyException();
    }
}
