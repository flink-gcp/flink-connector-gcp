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

package io.github.flink.gcp.connector.bigquery.sink;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.bigquery.sink.MixedCaseTables.MixedCaseSerializer;
import io.github.flink.gcp.connector.bigquery.sink.serializer.RowDescriptors;
import io.github.flink.gcp.connector.testutils.TestNames;
import io.github.flink.gcp.connector.testutils.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.testutils.bigquery.RealGcs;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import static io.github.flink.gcp.connector.bigquery.sink.MixedCaseTables.appendSink;
import static io.github.flink.gcp.connector.bigquery.sink.MixedCaseTables.assertMixedCaseSpelling;
import static io.github.flink.gcp.connector.bigquery.sink.MixedCaseTables.assertRow;
import static io.github.flink.gcp.connector.bigquery.sink.MixedCaseTables.createTable;
import static io.github.flink.gcp.connector.bigquery.sink.MixedCaseTables.loadSink;
import static io.github.flink.gcp.connector.bigquery.sink.MixedCaseTables.run;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Protobuf field names with upper-case letters against <b>real</b> BigQuery (#2179): a table
 * created beforehand with the columns {@code hasNext}, {@code beforeUpdate} and so on takes rows
 * from both write methods with no schema change. The columns keep the field names as written, while
 * the client library lower-cases the row descriptor's field names, so the Storage Write API half
 * rests on the service matching a column name case-insensitively. The load job half stages Avro
 * under the column names themselves. A table whose columns are spelled in another case, here all
 * lower-case as another writer may have created them, takes the same rows with no schema change
 * too. {@code ProtoRowConverterTest} and {@code ProtoToAvroConverterTest} pin the client-side
 * pairing offline.
 *
 * <p>Real GCP because load jobs have no emulator; skipped unless {@code BQ_IT_PROJECT}, {@code
 * BQ_IT_DATASET} and {@code BQ_IT_GCS_BUCKET} are set.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_GCS_BUCKET", matches = ".+")
@Timeout(value = 600, threadMode = ThreadMode.SEPARATE_THREAD)
class BigQueryProtoMixedCaseFieldNamesRealGcpITCase {

    private static final String RUN_ID = TestNames.runId();
    private static final String LOAD_TABLE = "mixed_case_load_" + RUN_ID;
    private static final String APPEND_TABLE = "mixed_case_append_" + RUN_ID;
    private static final String LOWER_LOAD_TABLE = "mixed_case_lower_load_" + RUN_ID;
    private static final String LOWER_APPEND_TABLE = "mixed_case_lower_append_" + RUN_ID;
    private static final String STAGING_PREFIX = "flink-mixed-case-it/" + RUN_ID;

    @AfterAll
    static void cleanUp() throws Exception {
        Closers.closeAll(
                () ->
                        RealBigQuery.deleteTables(
                                LOAD_TABLE, APPEND_TABLE, LOWER_LOAD_TABLE, LOWER_APPEND_TABLE),
                () -> RealGcs.deletePrefix(STAGING_PREFIX));
    }

    @Test
    void aLoadJobWritesToTheMixedCaseColumns() throws Exception {
        MixedCaseSerializer serializer = new MixedCaseSerializer();
        FieldList created = createTable(LOAD_TABLE, serializer.getTableSchema(null));
        assertMixedCaseSpelling(created);

        run(loadSink(LOAD_TABLE, serializer, STAGING_PREFIX), "a");

        assertRow(LOAD_TABLE, created);
    }

    @Test
    void theStorageWriteApiAppendsToTheMixedCaseColumns() throws Exception {
        MixedCaseSerializer serializer = new MixedCaseSerializer();
        FieldList created = createTable(APPEND_TABLE, serializer.getTableSchema(null));
        assertMixedCaseSpelling(created);

        run(appendSink(APPEND_TABLE, serializer), "a");

        assertRow(APPEND_TABLE, created);
    }

    @Test
    void aLoadJobWritesToColumnsSpelledInAnotherCase() throws Exception {
        MixedCaseSerializer serializer = new MixedCaseSerializer();
        FieldList created =
                createTable(
                        LOWER_LOAD_TABLE,
                        RowDescriptors.lowerCased(serializer.getTableSchema(null)));
        assertLowerCaseSpelling(created);

        run(loadSink(LOWER_LOAD_TABLE, serializer, STAGING_PREFIX), "a");

        assertRow(LOWER_LOAD_TABLE, created);
    }

    @Test
    void theStorageWriteApiAppendsToColumnsSpelledInAnotherCase() throws Exception {
        MixedCaseSerializer serializer = new MixedCaseSerializer();
        FieldList created =
                createTable(
                        LOWER_APPEND_TABLE,
                        RowDescriptors.lowerCased(serializer.getTableSchema(null)));
        assertLowerCaseSpelling(created);

        run(appendSink(LOWER_APPEND_TABLE, serializer), "a");

        assertRow(LOWER_APPEND_TABLE, created);
    }

    /** The premise of the two other-case tests: the table's spelling differs from the fields'. */
    private static void assertLowerCaseSpelling(FieldList fields) {
        assertThat(fields)
                .extracting(Field::getName)
                .containsExactly(
                        "id", "hasnext", "beforeupdate", "items", "countsbyname", "userid");
        assertThat(fields.get("beforeupdate").getSubFields())
                .extracting(Field::getName)
                .containsExactly("innervalue", "count");
    }
}
