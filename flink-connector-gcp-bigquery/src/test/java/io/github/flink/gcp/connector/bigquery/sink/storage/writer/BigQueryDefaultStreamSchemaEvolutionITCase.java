/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import io.github.flink.gcp.connector.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.SchemaUpdateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.tables.BigQueryTableAdmin;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A <b>manual probe</b>, not a weekly E2E test: connector-driven schema evolution on the default
 * stream against <b>real</b> BigQuery, run end to end through the production {@code
 * StreamWriterRowAppenderFactory} and the production schema-wait schedule.
 *
 * <p><b>Measured 2026-07-31</b> (us-central1, one run): the REST {@code tables.update} succeeds
 * instantly and the first append with the new column is rejected fast ({@code
 * SchemaMismatchedException}), but from there the service did not accept the new column for <b>~1 h
 * 56 m</b> — one retry's append future hung ~35 minutes before failing (with {@code
 * MaximumRequestCallbackWaitTimeExceededException} underneath), the next hung ~79 minutes before
 * succeeding, and the ~35-minute "failed" append had been applied server-side anyway, landing the
 * row twice (at-least-once duplicates working as specified, but showing the hang is not a clean
 * rejection). The emulator's instant success for the same scenario ({@link
 * BigQuerySchemaEvolutionITCase}) is exactly the false design signal issue #16 warns about.
 *
 * <p>That wall time is why this class is <b>deliberately outside the weekly E2E suite</b>: it would
 * consume the whole runner budget. {@code scripts/e2e-gated-its.sh} derives the suite from the
 * {@code BQ_IT_PROJECT} annotation literal, so this class gates on {@code BQ_IT_SCHEMA_EVOLUTION}
 * (plus {@code BQ_IT_DATASET}) instead and the script never sees it — <b>do not "fix" the gating to
 * match the other real-GCP ITCases</b>. {@code BQ_IT_PROJECT} and {@code GOOGLE_CLOUD_PROJECT} must
 * still be set for the run to work:
 *
 * <pre>{@code
 * BQ_IT_SCHEMA_EVOLUTION=1 ./mvnw -pl flink-connector-gcp-bigquery test-compile \
 *   surefire:test@integration-tests -Dtest=BigQueryDefaultStreamSchemaEvolutionITCase
 * }</pre>
 *
 * <p>The evolution <em>mechanics</em> — fingerprint change detection, reconcile, continued writes
 * on one writer — stay pinned by the emulator ITCase and by {@link
 * BigQueryDefaultStreamWriterSchemaEvolutionTest} against fakes; this probe exists to measure the
 * real service's propagation behavior, and reruns feed the investigation of the hang recorded
 * above.
 */
@EnabledIfEnvironmentVariable(named = "BQ_IT_SCHEMA_EVOLUTION", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@Timeout(10_800)
class BigQueryDefaultStreamSchemaEvolutionITCase {

    private static final String TABLE =
            "default_stream_evolution_it_" + UUID.randomUUID().toString().substring(0, 8);

    private static final SinkWriter.Context CONTEXT =
            new SinkWriter.Context() {
                @Override
                public long currentWatermark() {
                    return 0;
                }

                @Override
                public Long timestamp() {
                    return null;
                }
            };

    private static TableFieldSchema nullableString(String name) {
        return TableFieldSchema.newBuilder()
                .setName(name)
                .setType(TableFieldSchema.Type.STRING)
                .setMode(TableFieldSchema.Mode.NULLABLE)
                .build();
    }

    private static final TableSchema V1 =
            TableSchema.newBuilder().addFields(nullableString("name")).build();
    private static final TableSchema V2 =
            TableSchema.newBuilder()
                    .addFields(nullableString("name"))
                    .addFields(nullableString("note"))
                    .build();

    @AfterAll
    static void dropTable() {
        RealBigQuery.deleteTables(TABLE);
    }

    @Test
    void connectorWidensTheTableAndWritesThroughRealPropagation() throws Exception {
        // Fail loud, not with a bare NPE: this class's gate deliberately omits the BQ_IT_PROJECT
        // annotation (see the class javadoc), so nothing else checks the variable is set.
        assertThat(RealBigQuery.project())
                .as(
                        "BQ_IT_PROJECT (and GOOGLE_CLOUD_PROJECT) must be set alongside"
                                + " BQ_IT_SCHEMA_EVOLUTION")
                .isNotNull();
        RealBigQuery.createTable(TABLE, V1);
        EvolvingSerializer serializer = new EvolvingSerializer(V1);
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destination(
                                        TableDestination.of(
                                                RealBigQuery.project(),
                                                RealBigQuery.dataset(),
                                                TABLE))
                                .serializer(serializer)
                                .createDisposition(CreateDisposition.CREATE_NEVER)
                                .schemaUpdateOptions(
                                        SchemaUpdateOptions.builder().allowNewFields().build())
                                .build();
        SinkWriter<String> writer =
                sink.createWriter(
                        new StreamWriterRowAppenderFactory(sink.getOptions()),
                        new BigQueryTableAdmin());
        try {
            writer.write("alice", CONTEXT);
            writer.flush(false);

            serializer.evolveTo(V2);

            writer.write("bob:hello", CONTEXT);
            writer.flush(false);
        } finally {
            writer.close();
        }

        // The sink widened the table itself, and the evolved column's value is queryable — the
        // half the emulator cannot show. DISTINCT because the propagation wait retries appends,
        // and a hung-but-applied append lands its row twice (measured; at-least-once permits it).
        assertThat(tableFieldNames()).containsExactly("name", "note");
        List<String> rows = new ArrayList<>();
        for (FieldValueList row :
                RealBigQuery.queryRows(
                        "SELECT DISTINCT name, note FROM "
                                + RealBigQuery.tablePath(TABLE)
                                + " ORDER BY name")) {
            rows.add(
                    row.get(0).getStringValue()
                            + "|"
                            + (row.get(1).isNull() ? "" : row.get(1).getStringValue()));
        }
        assertThat(rows).containsExactly("alice|", "bob|hello");
    }

    private static List<String> tableFieldNames() {
        List<String> fieldNames = new ArrayList<>();
        for (Field field :
                RealBigQuery.client()
                        .getTable(TableId.of(RealBigQuery.project(), RealBigQuery.dataset(), TABLE))
                        .<StandardTableDefinition>getDefinition()
                        .getSchema()
                        .getFields()) {
            fieldNames.add(field.getName());
        }
        return fieldNames;
    }
}
