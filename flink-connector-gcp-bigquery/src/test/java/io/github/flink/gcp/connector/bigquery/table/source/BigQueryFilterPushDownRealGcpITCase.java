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

package io.github.flink.gcp.connector.bigquery.table.source;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import io.github.flink.gcp.connector.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.bigquery.source.BigQuerySourceBuilder;
import io.github.flink.gcp.connector.bigquery.source.enumerator.ReadClientSessionCreator;
import io.github.flink.gcp.connector.bigquery.source.enumerator.ReadSessionCreator;
import io.github.flink.gcp.connector.bigquery.source.reader.ReadClientRowStreamOpener;
import io.github.flink.gcp.connector.bigquery.source.reader.RowStream;
import io.github.flink.gcp.connector.bigquery.source.reader.RowStreamOpener;
import io.github.flink.gcp.connector.testutils.TestNames;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Bounded measurement of generated filter pushdown against the real Storage Read API. */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@Timeout(300)
class BigQueryFilterPushDownRealGcpITCase {

    private static final String TABLE = "filter_pushdown_" + TestNames.runId();

    @BeforeAll
    static void seed() throws Exception {
        try {
            RealBigQuery.queryRows(
                    "CREATE TABLE "
                            + RealBigQuery.tablePath(TABLE)
                            + " AS SELECT id, REPEAT(CONCAT('payload-', CAST(id AS STRING)), 128) "
                            + "AS payload, IF(id = 64, NULL, 'present') AS alias, "
                            + "CONCAT('name-', CAST(id AS STRING)) AS label, "
                            + "CAST(id AS BIGNUMERIC) + BIGNUMERIC '0.0000000004' AS amount, "
                            + "CAST(id AS NUMERIC) + NUMERIC '0.000000001' AS numeric_amount, "
                            + "CAST(id AS FLOAT64) + 0.25 AS double_value, "
                            + "CAST(id AS FLOAT64) + 0.0000001 AS single_value, "
                            + "DATETIME_ADD(DATETIME '2026-08-29 12:00:00', "
                            + "INTERVAL id MICROSECOND) AS civil_time, "
                            + "TIMESTAMP_ADD(TIMESTAMP '2026-08-29 03:00:00+00', "
                            + "INTERVAL id MICROSECOND) AS event_time "
                            + "FROM UNNEST(GENERATE_ARRAY(1, 64)) AS id");
        } catch (Exception e) {
            RealBigQuery.deleteTables(TABLE);
            throw e;
        }
    }

    @AfterAll
    static void cleanUp() {
        RealBigQuery.deleteTables(TABLE);
    }

    @Test
    void generatedRestrictionReducesReturnedRowsAndSerializedAvroBytes() throws Exception {
        DataType physical =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("payload", DataTypes.STRING()));
        BigQueryFilterPushDown.State translated =
                BigQueryFilterPushDown.translate(
                        (RowType) physical.getLogicalType(),
                        Collections.singletonList(
                                CallExpression.permanent(
                                        BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                                        Arrays.asList(
                                                new FieldReferenceExpression(
                                                        "id", DataTypes.BIGINT(), 0, 0),
                                                new ValueLiteralExpression(BigDecimal.valueOf(4))),
                                        DataTypes.BOOLEAN())),
                        null);
        assertThat(translated.rowRestriction()).isEqualTo("((`id` <= 4))");

        ReadMeasurement unrestricted = read(null);
        ReadMeasurement filtered = read(translated.rowRestriction());

        assertThat(unrestricted.rows).isEqualTo(64);
        assertThat(filtered.rows).isEqualTo(4);
        assertThat(filtered.serializedAvroBytes).isLessThan(unrestricted.serializedAvroBytes);
    }

    @Test
    void generatedNullRestrictionSelectsTheNullRow() throws Exception {
        DataType physical =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("payload", DataTypes.STRING()),
                        DataTypes.FIELD("alias", DataTypes.STRING()));
        BigQueryFilterPushDown.State translated =
                BigQueryFilterPushDown.translate(
                        (RowType) physical.getLogicalType(),
                        Collections.singletonList(
                                CallExpression.permanent(
                                        BuiltInFunctionDefinitions.IS_NULL,
                                        Collections.singletonList(
                                                new FieldReferenceExpression(
                                                        "alias", DataTypes.STRING(), 0, 2)),
                                        DataTypes.BOOLEAN())),
                        null);

        assertThat(translated.rowRestriction()).isEqualTo("((`alias` IS NULL))");
        assertThat(read(translated.rowRestriction()).rows).isEqualTo(1);
    }

    @Test
    void generatedScalarRestrictionsAreAcceptedByTheRealStorageReadApi() throws Exception {
        DataType physical =
                DataTypes.ROW(
                        DataTypes.FIELD("label", DataTypes.STRING()),
                        DataTypes.FIELD("amount", DataTypes.DECIMAL(38, 9)),
                        DataTypes.FIELD("double_value", DataTypes.DOUBLE()),
                        DataTypes.FIELD("single_value", DataTypes.FLOAT()),
                        DataTypes.FIELD("civil_time", DataTypes.TIMESTAMP(6)),
                        DataTypes.FIELD("event_time", DataTypes.TIMESTAMP_LTZ(6)),
                        DataTypes.FIELD("numeric_amount", DataTypes.DECIMAL(38, 9)));

        assertRows(
                physical,
                "label",
                DataTypes.STRING(),
                0,
                BuiltInFunctionDefinitions.EQUALS,
                new ValueLiteralExpression("name-7"),
                1);
        assertRows(
                physical,
                "amount",
                DataTypes.DECIMAL(38, 9),
                1,
                BuiltInFunctionDefinitions.EQUALS,
                new ValueLiteralExpression(new BigDecimal("7.000000000")),
                1);
        assertRows(
                physical,
                "double_value",
                DataTypes.DOUBLE(),
                2,
                BuiltInFunctionDefinitions.GREATER_THAN,
                new ValueLiteralExpression(62.25d),
                2);
        assertRows(
                physical,
                "single_value",
                DataTypes.FLOAT(),
                3,
                BuiltInFunctionDefinitions.EQUALS,
                new ValueLiteralExpression(7.0f),
                1);
        assertRows(
                physical,
                "civil_time",
                DataTypes.TIMESTAMP(6),
                4,
                BuiltInFunctionDefinitions.EQUALS,
                new ValueLiteralExpression(LocalDateTime.parse("2026-08-29T12:00:00.000007")),
                1);
        assertRows(
                physical,
                "event_time",
                DataTypes.TIMESTAMP_LTZ(6),
                5,
                BuiltInFunctionDefinitions.EQUALS,
                new ValueLiteralExpression(
                        Instant.parse("2026-08-29T03:00:00.000007Z"),
                        DataTypes.TIMESTAMP_LTZ(6).notNull()),
                1);
        assertRows(
                physical,
                "numeric_amount",
                DataTypes.DECIMAL(38, 9),
                6,
                BuiltInFunctionDefinitions.EQUALS,
                new ValueLiteralExpression(new BigDecimal("7.000000001")),
                1);
    }

    private static void assertRows(
            DataType physical,
            String fieldName,
            DataType fieldType,
            int fieldIndex,
            BuiltInFunctionDefinition function,
            ValueLiteralExpression literal,
            long expectedRows)
            throws Exception {
        BigQueryFilterPushDown.State translated =
                BigQueryFilterPushDown.translate(
                        (RowType) physical.getLogicalType(),
                        Collections.singletonList(
                                CallExpression.permanent(
                                        function,
                                        Arrays.asList(
                                                new FieldReferenceExpression(
                                                        fieldName, fieldType, 0, fieldIndex),
                                                literal),
                                        DataTypes.BOOLEAN())),
                        null);

        assertThat(translated.rowRestriction()).isNotNull();
        assertThat(read(translated.rowRestriction()).rows).isEqualTo(expectedRows);
    }

    private static ReadMeasurement read(String rowRestriction) throws Exception {
        ReadSession.TableReadOptions.Builder options =
                ReadSession.TableReadOptions.newBuilder()
                        .addSelectedFields("id")
                        .addSelectedFields("payload");
        if (rowRestriction != null) {
            options.setRowRestriction(rowRestriction);
        }
        ReadSession session;
        try (ReadSessionCreator creator = new ReadClientSessionCreator(null)) {
            session =
                    creator.create(
                            CreateReadSessionRequest.newBuilder()
                                    .setParent("projects/" + RealBigQuery.project())
                                    .setMaxStreamCount(1)
                                    .setReadSession(
                                            ReadSession.newBuilder()
                                                    .setTable(
                                                            RealBigQuery.destination(TABLE)
                                                                    .toTablePath())
                                                    .setDataFormat(DataFormat.AVRO)
                                                    .setReadOptions(options))
                                    .build());
        }
        assertThat(session.getStreamsList()).isNotEmpty();
        long rows = 0;
        long bytes = 0;
        try (RowStreamOpener opener =
                        new ReadClientRowStreamOpener(
                                null, BigQuerySourceBuilder.DEFAULT_RETRY_MAX_ATTEMPTS);
                RowStream stream = opener.open(session.getStreams(0).getName(), 0)) {
            ReadRowsResponse response;
            while ((response = stream.next()) != null) {
                rows += response.getRowCount();
                bytes += response.getAvroRows().getSerializedBinaryRows().size();
            }
        }
        return new ReadMeasurement(rows, bytes);
    }

    private static final class ReadMeasurement {
        private final long rows;
        private final long serializedAvroBytes;

        private ReadMeasurement(long rows, long serializedAvroBytes) {
            this.rows = rows;
            this.serializedAvroBytes = serializedAvroBytes;
        }
    }
}
