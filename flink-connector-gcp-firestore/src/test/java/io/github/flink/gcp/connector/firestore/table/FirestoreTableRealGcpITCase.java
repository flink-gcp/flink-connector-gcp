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

package io.github.flink.gcp.connector.firestore.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.google.cloud.firestore.Firestore;
import com.google.firestore.admin.v1.Database;
import io.github.flink.gcp.connector.firestore.AbstractFirestoreRealGcpITCase;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code firestore} table against the real service: an array directly inside an array, which
 * the table maps, and the default {@code merge} and {@code replace} write modes, which {@code
 * FirestoreTableSinkITCase} holds on the emulator. Whether an array may hold an array depends on
 * the database's edition. An Enterprise-edition database stores one, and the docs claim support for
 * that edition only. Google's documentation says a Standard-edition database refuses one, as the
 * emulator does, but the service's answer there has changed twice: stored when measured on
 * 2026-10-10, refused with {@code INVALID_ARGUMENT} on 2026-10-17, stored again on 2026-10-24. This
 * class therefore holds the Enterprise edition's round trip and accepts either Standard-edition
 * answer. The unit tests cover the mapping, and this class is the one end-to-end check that a
 * column of that type survives a SQL write and read.
 */
@EnabledIfEnvironmentVariable(named = "FIRESTORE_IT_PROJECT", matches = ".+")
@Tag("gated")
class FirestoreTableRealGcpITCase extends AbstractFirestoreRealGcpITCase {

    private static final Logger LOG = LoggerFactory.getLogger(FirestoreTableRealGcpITCase.class);

    private static final String INSERT_MATRIX =
            "INSERT INTO matrices VALUES"
                    + " ('m1', ARRAY[ARRAY[CAST(1 AS BIGINT), 2], ARRAY[CAST(3 AS BIGINT)]])";

    @Test
    void theDefaultMergeKeepsUndeclaredFieldsAndReplaceRemovesThem() throws Exception {
        String collection = uniqueCollection();
        client().document(collection + "/m")
                .set(Map.of("other", 1L, "attrs", Map.of("a", 1L), "row", Map.of("y", "kept")))
                .get(30, TimeUnit.SECONDS);
        client().document(collection + "/r").set(Map.of("other", 1L)).get(30, TimeUnit.SECONDS);
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        String columns =
                " (id STRING NOT NULL, attrs MAP<STRING, BIGINT>, `row` ROW<x BIGINT>,"
                        + " PRIMARY KEY (id) NOT ENFORCED) ";
        table.executeSql("CREATE TABLE merged" + columns + with(collection, ""));
        table.executeSql(
                "CREATE TABLE replaced"
                        + columns
                        + with(collection, ", 'sink.write-mode' = 'replace'"));

        table.executeSql(
                        "INSERT INTO merged VALUES ('m', MAP['b', CAST(2 AS BIGINT)],"
                                + " ROW(CAST(1 AS BIGINT)))")
                .await(120, TimeUnit.SECONDS);
        table.executeSql(
                        "INSERT INTO replaced VALUES ('r', MAP['b', CAST(2 AS BIGINT)],"
                                + " ROW(CAST(1 AS BIGINT)))")
                .await(120, TimeUnit.SECONDS);

        assertThat(read(collection + "/m").getData())
                .isEqualTo(
                        Map.of(
                                "other",
                                1L,
                                "attrs",
                                Map.of("a", 1L, "b", 2L),
                                "row",
                                Map.of("x", 1L, "y", "kept")));
        assertThat(read(collection + "/r").getData())
                .isEqualTo(Map.of("attrs", Map.of("b", 2L), "row", Map.of("x", 1L)));
    }

    private static String with(String collection, String extra) {
        return "WITH ('connector' = 'firestore', 'project' = '"
                + PROJECT
                + "', 'database' = '"
                + database().getDatabaseId()
                + "', 'collection' = '"
                + collection
                + "'"
                + extra
                + ")";
    }

    @Test
    void anArrayOfArraysIsWrittenAndReadBackThroughSqlInEnterpriseEdition() throws Exception {
        DatabaseDestination enterprise =
                createDatabase(
                        Database.DatabaseType.FIRESTORE_NATIVE,
                        Database.DatabaseEdition.ENTERPRISE);
        String collection = uniqueCollection();
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.executeSql(matrices(enterprise, collection));

        table.executeSql(INSERT_MATRIX).await(120, TimeUnit.SECONDS);

        try (Firestore client = open(enterprise)) {
            assertThat(
                            client.document(collection + "/m1")
                                    .get()
                                    .get(30, TimeUnit.SECONDS)
                                    .get("cells"))
                    .isEqualTo(List.of(List.of(1L, 2L), List.of(3L)));
        }
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> collected =
                table.executeSql("SELECT id, cells FROM matrices").collect()) {
            collected.forEachRemaining(rows::add);
        }
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getField("id")).isEqualTo("m1");
        assertThat((Long[][]) rows.get(0).getField("cells"))
                .isDeepEqualTo(new Long[][] {{1L, 2L}, {3L}});
    }

    @Test
    void aStandardEditionDatabaseStoresOrRefusesAnArrayOfArrays() throws Exception {
        // Either answer passes, because the service has given both within two weeks (the class
        // comment). What the test holds is that neither is half of the other: a refusal fails the
        // job and leaves no document, since the table has no dead-letter path, and a stored row
        // reads back unchanged. A timeout is neither, so it is not caught. Each answer is logged
        // only once its assertions hold, so the weekly log records the service's answer.
        String collection = uniqueCollection();
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.executeSql(matrices(database(), collection));

        @Nullable ExecutionException refusal = null;
        try {
            table.executeSql(INSERT_MATRIX).await(120, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            refusal = e;
        }
        if (refusal != null) {
            assertThat(refusal).hasStackTraceContaining("Nested arrays are not allowed");
            assertThat(read(collection + "/m1").exists()).isFalse();
            LOG.info("A Standard-edition database refused an array of arrays written through SQL.");
        } else {
            assertThat(read(collection + "/m1").get("cells"))
                    .isEqualTo(List.of(List.of(1L, 2L), List.of(3L)));
            LOG.info("A Standard-edition database stored an array of arrays written through SQL.");
        }
    }

    private static String matrices(DatabaseDestination destination, String collection) {
        return "CREATE TABLE matrices ("
                + " id STRING NOT NULL,"
                + " cells ARRAY<ARRAY<BIGINT>>,"
                + " PRIMARY KEY (id) NOT ENFORCED) WITH ("
                + " 'connector' = 'firestore',"
                + " 'project' = '"
                + PROJECT
                + "', 'database' = '"
                + destination.getDatabaseId()
                + "', 'collection' = '"
                + collection
                + "')";
    }
}
