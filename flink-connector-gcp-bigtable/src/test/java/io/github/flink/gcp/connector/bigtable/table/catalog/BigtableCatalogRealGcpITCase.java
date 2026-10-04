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

package io.github.flink.gcp.connector.bigtable.table.catalog;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import com.google.bigtable.admin.v2.Type;
import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableRealGcpITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.github.flink.gcp.connector.bigtable.BigtableAdminProtos.bigEndianInt64;
import static io.github.flink.gcp.connector.bigtable.BigtableAdminProtos.int64Hll;
import static io.github.flink.gcp.connector.bigtable.BigtableAdminProtos.int64Max;
import static io.github.flink.gcp.connector.bigtable.BigtableAdminProtos.int64Min;
import static io.github.flink.gcp.connector.bigtable.BigtableAdminProtos.int64Sum;
import static io.github.flink.gcp.connector.bigtable.BigtableAdminProtos.rawFamily;
import static io.github.flink.gcp.connector.bigtable.BigtableAdminProtos.typedFamily;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code bigtable} catalog against real Bigtable, for what the emulator cannot show: aggregate
 * families exist only in the service, so their value types as the admin API reports them, and the
 * state the catalog reads through them, are measured here (docs/adr/0178).
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BIGTABLE_IT_PROJECT", matches = ".+")
class BigtableCatalogRealGcpITCase extends AbstractBigtableRealGcpITCase {

    private static final Logger LOG = LoggerFactory.getLogger(BigtableCatalogRealGcpITCase.class);

    @Test
    void aggregateFamiliesResolveFromTheServicesMetadataAndReadTheirState() throws Exception {
        String id = "catalog-aggregates";
        TableDestination destination =
                createTable(
                        id,
                        Map.of(
                                "cf", rawFamily(),
                                "totals", typedFamily(int64Sum()),
                                "minimums", typedFamily(int64Min()),
                                "maximums", typedFamily(int64Max()),
                                "users", typedFamily(int64Hll())));

        // The value types as the catalog's own client reads them, recorded for ADR-0178: whether
        // the service reports a state type, and which one it reports for HLL.
        Map<String, Type> reported;
        try (BigtableServiceCatalogClient client =
                BigtableServiceCatalogClient.open(PROJECT, destination.getInstance(), null, null)) {
            reported = client.columnFamilies(id);
        }
        assertThat(reported).containsOnlyKeys("cf", "totals", "minimums", "maximums", "users");
        reported.forEach(
                (family, type) ->
                        LOG.info(
                                "Reported value type of family {}: {}",
                                family,
                                TextFormat.shortDebugString(type)));
        // Measured 2026-10-04: every aggregate family reports its state type; sum, min and max an
        // int64 in big-endian bytes, HLL raw bytes.
        Type int64State = bigEndianInt64();
        for (String family : new String[] {"totals", "minimums", "maximums"}) {
            assertThat(reported.get(family).getAggregateType().getStateType())
                    .as(family)
                    .isEqualTo(int64State);
        }
        assertThat(reported.get("users").getAggregateType().getStateType().hasBytesType()).isTrue();
        assertThat(reported.get("cf").hasAggregateType()).isFalse();

        mutateRow(
                destination,
                ByteString.copyFromUtf8("r"),
                mutation -> mutation.setCell("cf", "name", "alice"));
        TableEnvironment writer = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        writer.getConfig().getConfiguration().setString("restart-strategy.type", "none");
        writer.getConfig().getConfiguration().setString("parallelism.default", "1");
        writer.executeSql(
                "CREATE TABLE contributions (rowkey STRING, totals MAP<STRING, BIGINT>, "
                        + "minimums MAP<STRING, BIGINT>, maximums MAP<STRING, BIGINT>, "
                        + "users MAP<STRING, BIGINT>, "
                        + "ts TIMESTAMP_LTZ(6) METADATA FROM 'timestamp'"
                        + ") WITH ('connector'='bigtable', 'project'='"
                        + PROJECT
                        + "', 'instance'='"
                        + destination.getInstance()
                        + "', 'table'='"
                        + id
                        + "', 'sink.write-mode'='aggregate', "
                        + "'sink.aggregate.column-family-types'='totals:int64-sum,"
                        + "minimums:int64-min,maximums:int64-max,users:int64-hll')");
        // One timestamp, so each family keeps one aggregate version.
        writer.executeSql(
                        "INSERT INTO contributions VALUES "
                                + "('r', MAP['d1', CAST(3 AS BIGINT)], MAP['d1', CAST(3 AS BIGINT)],"
                                + " MAP['d1', CAST(3 AS BIGINT)], MAP['campaign', CAST(101 AS BIGINT)],"
                                + " TO_TIMESTAMP_LTZ(1000, 3)),"
                                + "('r', MAP['d1', CAST(5 AS BIGINT)], MAP['d1', CAST(5 AS BIGINT)],"
                                + " MAP['d1', CAST(5 AS BIGINT)], MAP['campaign', CAST(102 AS BIGINT)],"
                                + " TO_TIMESTAMP_LTZ(1000, 3))")
                .await();

        TableEnvironment table = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        table.executeSql(
                "CREATE CATALOG bt WITH ('type' = 'bigtable', 'project' = '"
                        + PROJECT
                        + "', 'instance' = '"
                        + destination.getInstance()
                        + "', 'key-type' = 'string')");
        table.executeSql("USE CATALOG bt");
        try {
            List<String> described =
                    CollectionUtil.iteratorToList(
                                    table.executeSql("DESCRIBE `catalog-aggregates`").collect())
                            .stream()
                            .map(row -> row.getField(0) + " " + row.getField(1))
                            .collect(Collectors.toList());
            assertThat(described)
                    .containsExactly(
                            "_key STRING",
                            "cf MAP<STRING, BYTES>",
                            "maximums MAP<STRING, BIGINT>",
                            "minimums MAP<STRING, BIGINT>",
                            "totals MAP<STRING, BIGINT>",
                            "users MAP<STRING, BYTES>");

            List<Row> rows =
                    CollectionUtil.iteratorToList(
                            table.executeSql(
                                            "SELECT CAST(cf['name'] AS STRING), totals['d1'],"
                                                    + " minimums['d1'], maximums['d1'],"
                                                    + " users['campaign']"
                                                    + " FROM `catalog-aggregates`"
                                                    + " WHERE _key = 'r'")
                                    .collect());
            assertThat(rows).hasSize(1);
            Row row = rows.get(0);
            byte[] sketch = (byte[]) row.getField(4);
            LOG.info(
                    "HLL state read through the catalog: {} bytes",
                    sketch == null ? null : sketch.length);
            assertThat(Row.of(row.getField(0), row.getField(1), row.getField(2), row.getField(3)))
                    .isEqualTo(Row.of("alice", 8L, 3L, 5L));
            // A sketch, not the count estimate: the catalog reads the stored bytes.
            assertThat(sketch).isNotNull();
            assertThat(sketch.length).isNotEqualTo(8);
        } finally {
            table.getCatalog("bt").ifPresent(catalog -> catalog.close());
        }
    }
}
