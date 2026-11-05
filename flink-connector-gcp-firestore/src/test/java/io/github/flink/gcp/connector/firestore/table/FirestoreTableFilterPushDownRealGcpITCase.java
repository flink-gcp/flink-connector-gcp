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

import com.google.cloud.firestore.Blob;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Int32Value;
import com.google.firestore.admin.v1.Database;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.firestore.AbstractFirestoreRealGcpITCase;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code firestore} scan's pushed equalities against the service ({@code docs/adr/0197}): what
 * the emulator cannot show. A Standard-edition database answers two equalities on one collection
 * without a composite index, and would refuse the same filter on a collection group, which is why
 * that scan pushes none. An Enterprise-edition database matches its 32-bit integers by the equality
 * a {@code BIGINT} column pushes, and refuses to store a BSON binary of subtype 0, so every value a
 * {@code BYTES} column reads is native bytes, which its pushed equality matches: no row the
 * converter reads is filtered out by the service.
 */
@EnabledIfEnvironmentVariable(named = "FIRESTORE_IT_PROJECT", matches = ".+")
@Tag("gated")
class FirestoreTableFilterPushDownRealGcpITCase extends AbstractFirestoreRealGcpITCase {

    private static TableEnvironment batch() {
        return TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
    }

    private static String with(DatabaseDestination database, String collection, String extra) {
        return "WITH ('connector' = 'firestore', 'project' = '"
                + PROJECT
                + "', 'database' = '"
                + database.getDatabaseId()
                + "', 'collection' = '"
                + collection
                + "', 'type-mismatch-policy' = 'null'"
                + extra
                + ")";
    }

    private static List<Row> collect(TableEnvironment table, String query) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = table.executeSql(query).collect()) {
            iterator.forEachRemaining(rows::add);
        }
        return rows;
    }

    @Test
    void aStandardEditionCollectionAnswersTwoEqualitiesAndAGroupScanIsLeftUnfiltered()
            throws Exception {
        String collection = uniqueCollection();
        client().document(collection + "/a")
                .set(Map.of("s", "x", "n", 5L))
                .get(30, TimeUnit.SECONDS);
        // The service matches 5.0 to 5; the converter reads a double under BIGINT as NULL, and
        // Flink's residual removes the row.
        client().document(collection + "/b")
                .set(Map.of("s", "x", "n", 5.0d))
                .get(30, TimeUnit.SECONDS);
        client().document(collection + "/c")
                .set(Map.of("s", "y", "n", 5L))
                .get(30, TimeUnit.SECONDS);
        client().document(collection + "/d").set(Map.of("s", "x")).get(30, TimeUnit.SECONDS);
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE keyed (id STRING NOT NULL, s STRING, n BIGINT,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(database(), collection, ""));
        table.executeSql(
                "CREATE TABLE grouped (s STRING, n BIGINT,"
                        + " path STRING METADATA FROM 'document-path' VIRTUAL) "
                        + with(database(), collection, ", 'scan.collection-group' = 'true'"));
        String predicate = " WHERE s = 'x' AND n = 5";

        assertThat(table.explainSql("SELECT id FROM keyed" + predicate))
                .containsPattern("filter=\\[[^\\]]");
        assertThat(collect(table, "SELECT id FROM keyed" + predicate)).containsExactly(Row.of("a"));
        // Pushed, this filter would need a collection-group index the service does not create,
        // and the read would fail with FAILED_PRECONDITION.
        assertThat(collect(table, "SELECT path FROM grouped" + predicate))
                .containsExactly(Row.of(collection + "/a"));
    }

    @Test
    void anEnterpriseEditionDatabaseMatchesItsBsonIntegersAndStoresNoSubtypeZeroBinary()
            throws Exception {
        DatabaseDestination enterprise =
                createDatabase(
                        Database.DatabaseType.FIRESTORE_NATIVE,
                        Database.DatabaseEdition.ENTERPRISE);
        String collection = uniqueCollection();
        try (Firestore client = open(enterprise)) {
            client.document(collection + "/int32")
                    .set(Map.of("n", new Int32Value(5)))
                    .get(30, TimeUnit.SECONDS);
            client.document(collection + "/bytes")
                    .set(Map.of("y", Blob.fromBytes(new byte[] {1, 2})))
                    .get(30, TimeUnit.SECONDS);
            // The library sends a subtype-0 Blob as native bytes, so the reserved form is built by
            // hand. Were it stored, the converter would read it as BYTES and the pushed equality
            // on native bytes would have to be measured against it.
            Value subtypeZero =
                    Value.newBuilder()
                            .setMapValue(
                                    MapValue.newBuilder()
                                            .putFields(
                                                    "__binary__",
                                                    Value.newBuilder()
                                                            .setBytesValue(
                                                                    ByteString.copyFrom(
                                                                            new byte[] {0, 1, 2}))
                                                            .build()))
                            .build();
            assertThatThrownBy(
                            () ->
                                    client.document(collection + "/bson")
                                            .set(Map.of("y", subtypeZero))
                                            .get(30, TimeUnit.SECONDS))
                    .hasStackTraceContaining("subtype 0 is not permitted");
        }
        TableEnvironment table = batch();
        table.executeSql(
                "CREATE TABLE t (id STRING NOT NULL, n BIGINT, y BYTES,"
                        + " PRIMARY KEY (id) NOT ENFORCED) "
                        + with(enterprise, collection, ""));

        for (String predicate : new String[] {"n = 5", "y = x'0102'"}) {
            // Pushed, or the residual alone would produce the rows and the match go unguarded.
            assertThat(table.explainSql("SELECT id FROM t WHERE " + predicate))
                    .containsPattern("filter=\\[[^\\]]");
        }
        assertThat(collect(table, "SELECT id FROM t WHERE n = 5")).containsExactly(Row.of("int32"));
        assertThat(collect(table, "SELECT id FROM t WHERE y = x'0102'"))
                .containsExactly(Row.of("bytes"));
    }
}
