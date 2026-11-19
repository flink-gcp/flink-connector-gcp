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

import com.google.cloud.bigquery.FieldList;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.bigquery.sink.MixedCaseTables.MixedCaseSerializer;
import io.github.flink.gcp.connector.bigquery.sink.serializer.json.JsonDocumentOptions;
import io.github.flink.gcp.connector.bigquery.sink.serializer.json.JsonDocumentSerializationSchema;
import io.github.flink.gcp.connector.bigquery.sink.serializer.proto.ProtoMessageSerializationSchema;
import io.github.flink.gcp.connector.bigquery.testproto.MixedCase;
import io.github.flink.gcp.connector.testutils.TestNames;
import io.github.flink.gcp.connector.testutils.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.testutils.bigquery.RealGcs;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.Locale;

import static io.github.flink.gcp.connector.bigquery.sink.MixedCaseTables.DOCUMENT;
import static io.github.flink.gcp.connector.bigquery.sink.MixedCaseTables.appendSink;
import static io.github.flink.gcp.connector.bigquery.sink.MixedCaseTables.assertMixedCaseSpelling;
import static io.github.flink.gcp.connector.bigquery.sink.MixedCaseTables.assertRow;
import static io.github.flink.gcp.connector.bigquery.sink.MixedCaseTables.createTable;
import static io.github.flink.gcp.connector.bigquery.sink.MixedCaseTables.loadSink;
import static io.github.flink.gcp.connector.bigquery.sink.MixedCaseTables.run;

/**
 * Column names holding an upper-case {@code I} against <b>real</b> BigQuery, with the JVM's default
 * locale Turkish for the whole class, client and MiniCluster alike (#2188). Under that locale the
 * client library lower-cases {@code userId} to {@code userıd}: the Storage Write API then rejected
 * the append with "Input schema has more fields than BigQuery schema, extra fields:
 * 'ıtems,userıd'", and a load failed before staging anything, on {@code ProtoToAvroConverter}'s "no
 * field for schema field Items". The protobuf serializer stands for the three serializers whose
 * converters pair by position; the JSON serializer, whose client-side converter matches keys by
 * name, is the other mechanism, with keys normalized for the default locale as a user running under
 * such a locale is told to ask for. Each writes through both methods into a table created
 * beforehand with the mixed-case columns, and the table must be unchanged afterwards.
 *
 * <p>Real GCP because the rejection is the service's and load jobs have no emulator; skipped unless
 * {@code BQ_IT_PROJECT}, {@code BQ_IT_DATASET} and {@code BQ_IT_GCS_BUCKET} are set.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_GCS_BUCKET", matches = ".+")
@Timeout(value = 600, threadMode = ThreadMode.SEPARATE_THREAD)
class BigQueryTurkishLocaleRealGcpITCase {

    private static final String RUN_ID = TestNames.runId();
    private static final String PROTO_LOAD_TABLE = "turkish_proto_load_" + RUN_ID;
    private static final String PROTO_APPEND_TABLE = "turkish_proto_append_" + RUN_ID;
    private static final String JSON_LOAD_TABLE = "turkish_json_load_" + RUN_ID;
    private static final String JSON_APPEND_TABLE = "turkish_json_append_" + RUN_ID;
    private static final String STAGING_PREFIX = "flink-turkish-locale-it/" + RUN_ID;

    private static Locale previousLocale;

    @BeforeAll
    static void useATurkishDefaultLocale() {
        previousLocale = Locale.getDefault();
        Locale.setDefault(new Locale("tr", "TR"));
    }

    @AfterAll
    static void cleanUp() throws Exception {
        try {
            Closers.closeAll(
                    () ->
                            RealBigQuery.deleteTables(
                                    PROTO_LOAD_TABLE,
                                    PROTO_APPEND_TABLE,
                                    JSON_LOAD_TABLE,
                                    JSON_APPEND_TABLE),
                    () -> RealGcs.deletePrefix(STAGING_PREFIX));
        } finally {
            Locale.setDefault(previousLocale);
        }
    }

    @Test
    void aLoadJobWritesProtobufRows() throws Exception {
        MixedCaseSerializer serializer = new MixedCaseSerializer();
        FieldList created = createTable(PROTO_LOAD_TABLE, serializer.getTableSchema(null));
        assertMixedCaseSpelling(created);

        run(loadSink(PROTO_LOAD_TABLE, serializer, STAGING_PREFIX), "a");

        assertRow(PROTO_LOAD_TABLE, created);
    }

    @Test
    void theStorageWriteApiAppendsProtobufRows() throws Exception {
        MixedCaseSerializer serializer = new MixedCaseSerializer();
        FieldList created = createTable(PROTO_APPEND_TABLE, serializer.getTableSchema(null));
        assertMixedCaseSpelling(created);

        run(appendSink(PROTO_APPEND_TABLE, serializer), "a");

        assertRow(PROTO_APPEND_TABLE, created);
    }

    @Test
    void aLoadJobWritesJsonDocuments() throws Exception {
        JsonDocumentSerializationSchema serializer = jsonSerializer();
        FieldList created = createTable(JSON_LOAD_TABLE, serializer.getTableSchema(null));
        assertMixedCaseSpelling(created);

        run(loadSink(JSON_LOAD_TABLE, serializer, STAGING_PREFIX), DOCUMENT);

        assertRow(JSON_LOAD_TABLE, created);
    }

    @Test
    void theStorageWriteApiAppendsJsonDocuments() throws Exception {
        JsonDocumentSerializationSchema serializer = jsonSerializer();
        FieldList created = createTable(JSON_APPEND_TABLE, serializer.getTableSchema(null));
        assertMixedCaseSpelling(created);

        run(appendSink(JSON_APPEND_TABLE, serializer), DOCUMENT);

        assertRow(JSON_APPEND_TABLE, created);
    }

    /** The JSON serializer over the protobuf message's own schema, so one assertion fits both. */
    private static JsonDocumentSerializationSchema jsonSerializer() {
        return JsonDocumentSerializationSchema.of(
                ProtoMessageSerializationSchema.of(MixedCase.class).getTableSchema(null),
                JsonDocumentOptions.builder().normalizeKeysForDefaultLocale().build());
    }
}
