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

package io.github.flink.gcp.connector.spanner.source.batch;

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import io.github.flink.gcp.connector.spanner.AbstractSpannerEmulatorITCase;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins what the <em>emulator</em> does, so that an image bump has to declare a change.
 *
 * <p>None of this is evidence about Cloud Spanner. Every assertion here is a deviation the
 * connector's docs page records, and the real service's behaviour is the gated suite's to measure.
 * Measured against {@code emulator:1.5.56} with {@code google-cloud-spanner} 6.119.0 on 2026-08-10.
 */
class SpannerEmulatorReadDeviationITCase extends AbstractSpannerEmulatorITCase {

    private static final int ROWS = 500;

    @Test
    void theEmulatorPlansTwoPartitionsAndIgnoresBothHints() throws Exception {
        SpannerDatabase database = seededDatabase();
        Statement query = Statement.of("SELECT id FROM singers");

        try (Spanner spanner = client()) {
            BatchReadOnlyTransaction txn = transaction(spanner, database);

            assertThat(txn.partitionQuery(PartitionOptions.getDefaultInstance(), query)).hasSize(2);
            // Google documents both as hints the service may ignore, and the emulator ignores them
            // outright: a job that needs a partition count has no way to ask for one here.
            assertThat(
                            txn.partitionQuery(
                                    PartitionOptions.newBuilder().setMaxPartitions(16).build(),
                                    query))
                    .hasSize(2);
            assertThat(
                            txn.partitionQuery(
                                    PartitionOptions.newBuilder()
                                            .setPartitionSizeBytes(1024)
                                            .build(),
                                    query))
                    .hasSize(2);
        }
    }

    @Test
    void oneOfTheEmulatorsPartitionsIsEmpty() throws Exception {
        // Which is why an empty partition is a normal thing for the split reader to finish, and why
        // the emulator gives that path real coverage even though it gives split planning none.
        SpannerDatabase database = seededDatabase();

        try (Spanner spanner = client()) {
            BatchReadOnlyTransaction txn = transaction(spanner, database);
            List<Partition> partitions =
                    txn.partitionQuery(
                            PartitionOptions.getDefaultInstance(),
                            Statement.of("SELECT id FROM singers"));

            List<Integer> counts = new ArrayList<>();
            for (Partition partition : partitions) {
                counts.add(rowsIn(txn, partition));
            }

            assertThat(counts).contains(0);
            assertThat(counts.stream().mapToInt(Integer::intValue).sum()).isEqualTo(ROWS);
        }
    }

    @Test
    void theEmulatorAcceptsTheDataBoostFlagAndDoesNothingWithIt() throws Exception {
        SpannerDatabase database = seededDatabase();

        try (Spanner spanner = client()) {
            BatchReadOnlyTransaction txn = transaction(spanner, database);

            assertThat(
                            txn.partitionQuery(
                                    PartitionOptions.getDefaultInstance(),
                                    Statement.of("SELECT id FROM singers"),
                                    Options.dataBoostEnabled(true)))
                    .hasSize(2);
            assertThat(
                            txn.partitionRead(
                                    PartitionOptions.getDefaultInstance(),
                                    "singers",
                                    KeySet.all(),
                                    Collections.singletonList("id"),
                                    Options.dataBoostEnabled(true)))
                    .hasSize(2);
        }
    }

    @Test
    void theEmulatorRefusesQueryShapesTheServiceAccepts() throws Exception {
        // The deviation that matters most, and it runs the *opposite* way from the usual one: the
        // emulator's check is its own, and its message says so — "not able to determine whether
        // this query is partitionable". So a query rejected here may be one Spanner would plan.
        SpannerDatabase database = seededDatabase();

        try (Spanner spanner = client()) {
            BatchReadOnlyTransaction txn = transaction(spanner, database);

            for (String sql :
                    new String[] {
                        "SELECT COUNT(*) AS c FROM singers",
                        "SELECT id FROM singers ORDER BY id",
                        "SELECT id FROM singers LIMIT 10"
                    }) {
                assertThatThrownBy(
                                () ->
                                        txn.partitionQuery(
                                                PartitionOptions.getDefaultInstance(),
                                                Statement.of(sql)))
                        .isInstanceOf(SpannerException.class)
                        .hasMessageContaining(
                                "not able to determine whether this query is" + " partitionable");
            }

            // And it accepts predicates and projections, which is what the source's own emulator
            // tests are able to cover.
            assertThat(
                            txn.partitionQuery(
                                    PartitionOptions.getDefaultInstance(),
                                    Statement.of("SELECT id FROM singers WHERE id > 10")))
                    .hasSize(2);
        }
    }

    @Test
    void theEmulatorsBypassHintOnlyWorksBeforeTheSelect() throws Exception {
        // Recorded because it is what an integration test has to write to cover a query shape the
        // emulator's check refuses — and because the obvious placement is rejected outright.
        SpannerDatabase database = seededDatabase();
        String hint = "@{spanner_emulator.disable_query_partitionability_check=true}";

        try (Spanner spanner = client()) {
            BatchReadOnlyTransaction txn = transaction(spanner, database);

            assertThat(
                            txn.partitionQuery(
                                    PartitionOptions.getDefaultInstance(),
                                    Statement.of(hint + " SELECT COUNT(*) AS c FROM singers")))
                    .hasSize(2);
            assertThatThrownBy(
                            () ->
                                    txn.partitionQuery(
                                            PartitionOptions.getDefaultInstance(),
                                            Statement.of(
                                                    "SELECT "
                                                            + hint
                                                            + " COUNT(*) AS c FROM"
                                                            + " singers")))
                    .isInstanceOf(SpannerException.class)
                    .hasMessageContaining("Invalid emulator-only hint");
        }
    }

    @Test
    void theClientRefusesTheTwoSingleUseOnlyBoundsBeforeTheEmulatorSeesThem() throws Exception {
        // Not an emulator deviation but a client one, pinned here for the same reason: the source's
        // builder refuses these too, and this is what says the second guard is not the only guard.
        SpannerDatabase database = seededDatabase();

        try (Spanner spanner = client()) {
            BatchClient batch = batchClient(spanner, database);

            assertThatThrownBy(
                            () ->
                                    batch.batchReadOnlyTransaction(
                                            TimestampBound.ofMaxStaleness(
                                                    1, java.util.concurrent.TimeUnit.SECONDS)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("not supported for multi-use read-only transactions");
        }
    }

    private static int rowsIn(BatchReadOnlyTransaction txn, Partition partition) {
        int rows = 0;
        try (com.google.cloud.spanner.ResultSet resultSet = txn.execute(partition)) {
            while (resultSet.next()) {
                rows++;
            }
        }
        return rows;
    }

    private static Spanner client() {
        return SpannerOptions.newBuilder()
                .setProjectId(PROJECT)
                .setEmulatorHost(emulatorEndpoint())
                .build()
                .getService();
    }

    private static BatchClient batchClient(Spanner spanner, SpannerDatabase database) {
        return spanner.getBatchClient(
                DatabaseId.of(
                        database.getProject(), database.getInstance(), database.getDatabase()));
    }

    private static BatchReadOnlyTransaction transaction(Spanner spanner, SpannerDatabase database) {
        return batchClient(spanner, database).batchReadOnlyTransaction(TimestampBound.strong());
    }

    private static SpannerDatabase seededDatabase() throws Exception {
        SpannerDatabase database =
                createDatabase(
                        Dialect.GOOGLE_STANDARD_SQL,
                        "CREATE TABLE singers (id INT64 NOT NULL, name STRING(64))"
                                + " PRIMARY KEY (id)");
        List<Mutation> rows = new ArrayList<>();
        for (long id = 0; id < ROWS; id++) {
            rows.add(
                    Mutation.newInsertOrUpdateBuilder("singers")
                            .set("id")
                            .to(id)
                            .set("name")
                            .to("singer-" + id)
                            .build());
            if (rows.size() == 250) {
                client(database).write(rows);
                rows.clear();
            }
        }
        if (!rows.isEmpty()) {
            client(database).write(rows);
        }
        return database;
    }
}
