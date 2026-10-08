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

package io.github.flink.gcp.connector.bigquery.sink.tables;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.testutils.TestNames;
import io.github.flink.gcp.connector.testutils.bigquery.RealBigQuery;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.IOException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Real-service acceptance for the conditional, schema-only update {@link
 * BigQueryTableAdmin#updateSchema} sends (#1667).
 *
 * <p>The emulator returns no table etag, so it never sees the precondition and both races are
 * reproduced here: two writers that read the same table and evolve it differently, and a writer
 * whose read predates a change to the table's other attributes. Each update that loses is retried
 * from a fresh read, as the reconcilers do.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@Timeout(value = 600, threadMode = ThreadMode.SEPARATE_THREAD)
class BigQueryTableAdminSchemaUpdateRealGcpITCase {

    private static final String UNIONS_TABLE = "schema_update_unions_" + TestNames.runId();
    private static final String ATTRIBUTES_TABLE = "schema_update_attributes_" + TestNames.runId();

    /** Fresh-read attempts; the per-table metadata quota may also answer a lost race. */
    private static final int ATTEMPTS = 10;

    private static final long BACKOFF_MS = 2_000;

    @AfterAll
    static void dropTables() {
        RealBigQuery.deleteTables(UNIONS_TABLE, ATTRIBUTES_TABLE);
    }

    @Test
    void concurrentUnionsConvergeAndTheStaleOneLosesOnItsPrecondition() throws Exception {
        RealBigQuery.createTable(UNIONS_TABLE, Schema.of(stringField("id")));
        TableDestination destination = destination(UNIONS_TABLE);
        BigQueryTableAdmin admin = new BigQueryTableAdmin();
        TableSchemaSnapshot first = admin.getSchema(destination);
        TableSchemaSnapshot second = admin.getSchema(destination);

        applyUntilApplied(admin, destination, first, "a");

        // The second writer's union lacks the column the first one added. BigQuery answers the
        // stale etag before it judges the schema, so this is a lost race and not the `400
        // invalid` ("Provided Schema does not match Table") that would fail the job.
        assertThat(staleUpdateStatus(admin, destination, second, "b")).isEqualTo(412);

        // The second writer then proceeds as a reconciler does: from its stale read first.
        applyUntilApplied(admin, destination, second, "b");
        assertThat(RealBigQuery.tableFields(UNIONS_TABLE))
                .extracting(Field::getName)
                .containsExactly("id", "a", "b");
    }

    @Test
    void aSchemaUpdateNeverRevertsAConcurrentChangeToTheTablesOtherAttributes() throws Exception {
        // The read must carry a description and labels of its own: an unset attribute is omitted
        // from a written-back resource, so only a set one can revert the concurrent change.
        RealBigQuery.createTableWithMetadata(
                ATTRIBUTES_TABLE,
                Schema.of(stringField("id")),
                "set at creation",
                Map.of("owner", "creator"),
                null);
        TableDestination destination = destination(ATTRIBUTES_TABLE);
        BigQueryTableAdmin admin = new BigQueryTableAdmin();
        TableSchemaSnapshot stale = admin.getSchema(destination);

        // Another party changes the description and labels after the read. A write-back of the
        // read's attributes — what an unconditional full-table update did — would revert both.
        RealBigQuery.queryRows(
                "ALTER TABLE "
                        + RealBigQuery.tablePath(ATTRIBUTES_TABLE)
                        + " SET OPTIONS (description = 'changed concurrently',"
                        + " labels = [('owner', 'someone_else')])");

        applyUntilApplied(admin, destination, stale, "a");

        assertThat(RealBigQuery.tableFields(ATTRIBUTES_TABLE))
                .extracting(Field::getName)
                .containsExactly("id", "a");
        assertThat(RealBigQuery.tableDescription(ATTRIBUTES_TABLE))
                .isEqualTo("changed concurrently");
        assertThat(RealBigQuery.tableLabels(ATTRIBUTES_TABLE))
                .containsExactlyEntriesOf(Map.of("owner", "someone_else"));
    }

    /**
     * Adds {@code column} the way the reconcilers do: the first attempt from {@code snapshot}, each
     * lost race from a fresh read. An {@link IOException} — a rejection that is not a lost race —
     * fails the test, which is the failure #1667 reported.
     */
    private static void applyUntilApplied(
            BigQueryTableAdmin admin,
            TableDestination destination,
            TableSchemaSnapshot snapshot,
            String column)
            throws IOException, InterruptedException {
        TableSchemaSnapshot base = snapshot;
        for (int attempt = 1; attempt <= ATTEMPTS; attempt++) {
            if (admin.updateSchema(destination, base, withColumn(base.getSchema(), column))) {
                return;
            }
            Thread.sleep(BACKOFF_MS);
            base = admin.getSchema(destination);
        }
        throw new AssertionError(
                "Adding " + column + " to " + destination + " lost " + ATTEMPTS + " races");
    }

    /**
     * The status BigQuery answers the request {@link BigQueryTableAdmin#updateSchema} sends from
     * {@code stale} to add {@code column}. A metadata-quota answer is waited out, since it says
     * nothing about the precondition and {@code updateSchema} folds both into {@code false}.
     */
    private static int staleUpdateStatus(
            BigQueryTableAdmin admin,
            TableDestination destination,
            TableSchemaSnapshot stale,
            String column)
            throws IOException, InterruptedException {
        for (int attempt = 1; attempt <= ATTEMPTS; attempt++) {
            try {
                admin.patchSchema(destination, stale, withColumn(stale.getSchema(), column));
                throw new AssertionError("A stale update of " + destination + " was applied");
            } catch (BigQueryException e) {
                if (!BigQueryTableAdmin.isRetriable(e)) {
                    return e.getCode();
                }
            }
            Thread.sleep(BACKOFF_MS);
        }
        throw new AssertionError("The metadata quota of " + destination + " never cleared");
    }

    private static TableSchema withColumn(TableSchema schema, String column) {
        for (TableFieldSchema field : schema.getFieldsList()) {
            if (field.getName().equals(column)) {
                return schema;
            }
        }
        return schema.toBuilder()
                .addFields(
                        TableFieldSchema.newBuilder()
                                .setName(column)
                                .setType(TableFieldSchema.Type.STRING)
                                .setMode(TableFieldSchema.Mode.NULLABLE))
                .build();
    }

    private static Field stringField(String name) {
        return Field.newBuilder(name, StandardSQLTypeName.STRING)
                .setMode(Field.Mode.NULLABLE)
                .build();
    }

    private static TableDestination destination(String table) {
        return TableDestination.of(RealBigQuery.project(), RealBigQuery.dataset(), table);
    }
}
