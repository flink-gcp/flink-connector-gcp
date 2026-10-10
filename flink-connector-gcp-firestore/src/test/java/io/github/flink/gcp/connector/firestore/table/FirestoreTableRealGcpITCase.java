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

import io.github.flink.gcp.connector.firestore.AbstractFirestoreRealGcpITCase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code firestore} table against the real service, for what the emulator refuses: an array
 * directly inside an array, which the emulator answers with {@code INVALID_ARGUMENT} and Google's
 * documentation says a Standard-edition database does not store, but which the service stored when
 * measured (2026-10-10). The table maps {@code ARRAY<ARRAY<T>>} on that measurement; the unit tests
 * cover the mapping and {@code FirestoreRejectionRealGcpITCase} the service's answer, and this
 * class is the one end-to-end check that a column of that type survives a SQL write and read.
 */
@EnabledIfEnvironmentVariable(named = "FIRESTORE_IT_PROJECT", matches = ".+")
@Tag("gated")
class FirestoreTableRealGcpITCase extends AbstractFirestoreRealGcpITCase {

    @Test
    void anArrayOfArraysIsWrittenAndReadBackThroughSql() throws Exception {
        String collection = uniqueCollection();
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.executeSql(
                "CREATE TABLE matrices ("
                        + " id STRING NOT NULL,"
                        + " cells ARRAY<ARRAY<BIGINT>>,"
                        + " PRIMARY KEY (id) NOT ENFORCED) WITH ("
                        + " 'connector' = 'firestore',"
                        + " 'project' = '"
                        + PROJECT
                        + "', 'database' = '"
                        + database().getDatabaseId()
                        + "', 'collection' = '"
                        + collection
                        + "')");

        table.executeSql(
                        "INSERT INTO matrices VALUES"
                                + " ('m1', ARRAY[ARRAY[CAST(1 AS BIGINT), 2], ARRAY[CAST(3 AS"
                                + " BIGINT)]])")
                .await(120, TimeUnit.SECONDS);

        assertThat(read(collection + "/m1").get("cells"))
                .isEqualTo(List.of(List.of(1L, 2L), List.of(3L)));
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
}
