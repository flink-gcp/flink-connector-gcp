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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import io.github.flink.gcp.connector.spanner.AbstractSpannerRealGcpITCase;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperation;
import io.github.flink.gcp.connector.spanner.source.SpannerSource;
import io.github.flink.gcp.connector.spanner.source.SpannerSourceBuilder;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerStructDeserializationSchema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What real Cloud Spanner does with a partitioned read — the three things {@code docs/adr/0085}
 * could not settle against the emulator.
 *
 * <p>The emulator planned exactly two partitions for every table it was asked about, one of them
 * empty, and ignored both partition hints, so split planning had no coverage at all. Its
 * partitionability check is its own and runs <em>stricter</em> than the service's, so which query
 * shapes Spanner will plan was unmeasured. And it accepts {@code dataBoostEnabled} while doing
 * nothing with it, so nothing showed the flag reaching anything.
 *
 * <p>What this class can and cannot claim about split planning: its table is a few thousand small
 * rows, which is far below the size at which Spanner splits a table, so a small partition count
 * here is a measurement of this scale and not evidence about a large one. That is worth having
 * anyway — it is what a reader of the docs page needs in order to know the hints are hints — and it
 * is a better answer than seeding gigabytes to make a number look impressive.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "SPANNER_IT_PROJECT", matches = ".+")
class SpannerSourceRealGcpITCase extends AbstractSpannerRealGcpITCase {

    private static final Logger LOG = LoggerFactory.getLogger(SpannerSourceRealGcpITCase.class);

    private static final int ROWS = 5_000;

    /** Spanner's commit limits are per transaction, so the seed goes in several. */
    private static final int SEED_BATCH = 500;

    private static SpannerDatabase database;

    @BeforeAll
    static void createAndSeedDatabase() throws Exception {
        database =
                createDatabase(
                        Dialect.GOOGLE_STANDARD_SQL,
                        "CREATE TABLE singers (id INT64 NOT NULL, name STRING(64))"
                                + " PRIMARY KEY (id)");
        List<Mutation> rows = new ArrayList<>(SEED_BATCH);
        for (long id = 0; id < ROWS; id++) {
            rows.add(
                    Mutation.newInsertOrUpdateBuilder("singers")
                            .set("id")
                            .to(id)
                            .set("name")
                            .to("singer-" + id)
                            .build());
            if (rows.size() == SEED_BATCH) {
                client(database).write(rows);
                rows.clear();
            }
        }
        if (!rows.isEmpty()) {
            client(database).write(rows);
        }
    }

    @Test
    void measuresHowManyPartitionsTheServicePlans() {
        Statement query = Statement.of("SELECT id FROM singers");
        BatchReadOnlyTransaction txn = transaction();

        int byDefault = txn.partitionQuery(PartitionOptions.getDefaultInstance(), query).size();
        int withMaxPartitions =
                txn.partitionQuery(
                                PartitionOptions.newBuilder().setMaxPartitions(16).build(), query)
                        .size();
        int withPartitionSize =
                txn.partitionQuery(
                                PartitionOptions.newBuilder().setPartitionSizeBytes(1024).build(),
                                query)
                        .size();
        int byRead =
                txn.partitionRead(
                                PartitionOptions.getDefaultInstance(),
                                "singers",
                                KeySet.all(),
                                Collections.singletonList("id"),
                                Options.dataBoostEnabled(false))
                        .size();

        LOG.info(
                "Cloud Spanner partition planning over {} rows:"
                        + "\n  partitionQuery, default hints      : {}"
                        + "\n  partitionQuery, maxPartitions=16   : {}"
                        + "\n  partitionQuery, sizeBytes=1024     : {}"
                        + "\n  partitionRead,  default hints      : {}",
                ROWS,
                byDefault,
                withMaxPartitions,
                withPartitionSize,
                byRead);

        // The service always plans at least one partition for a partitionable read; a count of
        // zero would mean the enumerator has nothing to assign and the job silently reads nothing.
        assertThat(byDefault).isPositive();
        assertThat(byRead).isPositive();
    }

    @Test
    void everyRowAppearsInExactlyOnePartition() {
        BatchReadOnlyTransaction txn = transaction();
        List<Partition> partitions =
                txn.partitionQuery(
                        PartitionOptions.getDefaultInstance(),
                        Statement.of("SELECT id FROM singers"));

        Set<Long> seen = new HashSet<>();
        int total = 0;
        for (Partition partition : partitions) {
            for (long id : idsIn(txn, partition)) {
                seen.add(id);
                total++;
            }
        }

        // Complete and disjoint, which is what makes one partition per split a correct plan and
        // what an at-least-once bounded read is otherwise free to violate unnoticed.
        assertThat(total).isEqualTo(ROWS);
        assertThat(seen).hasSize(ROWS);
    }

    @Test
    void measuresWhichQueryShapesTheServiceWillPlan() {
        BatchReadOnlyTransaction txn = transaction();
        Map<String, String> verdicts = new LinkedHashMap<>();
        for (String sql :
                new String[] {
                    "SELECT id FROM singers",
                    "SELECT id FROM singers WHERE id > 10",
                    "SELECT COUNT(*) AS c FROM singers",
                    "SELECT id FROM singers ORDER BY id",
                    "SELECT id FROM singers LIMIT 10"
                }) {
            verdicts.put(sql, plan(txn, sql));
        }

        LOG.info(
                "Cloud Spanner root-partitionability, one shape per line:\n  {}",
                verdicts.entrySet().stream()
                        .map(entry -> entry.getKey() + "\n    -> " + entry.getValue())
                        .collect(Collectors.joining("\n  ")));

        // The controls: a plain scan and a predicate are root-partitionable, so a refusal anywhere
        // above is about the shape rather than about the table, the transaction or the account.
        assertThat(verdicts.get("SELECT id FROM singers")).startsWith("planned");
        assertThat(verdicts.get("SELECT id FROM singers WHERE id > 10")).startsWith("planned");

        // Measured 2026-08-10: the service refuses the same three shapes the emulator does, so
        // the emulator's conservatism — real in principle, since its check is its own — did not
        // manifest on any shape tried here. What differs is the message, and that difference is
        // the whole reason the connector surfaces it unwrapped: Spanner names the condition and
        // links the documentation for it, where the emulator says only that it could not tell.
        // Asserted rather than merely logged so that a reworded refusal makes someone revisit the
        // docs sentence that quotes this.
        for (String sql :
                new String[] {
                    "SELECT COUNT(*) AS c FROM singers",
                    "SELECT id FROM singers ORDER BY id",
                    "SELECT id FROM singers LIMIT 10"
                }) {
            assertThat(verdicts.get(sql))
                    .startsWith("refused, INVALID_ARGUMENT")
                    .contains("root partitionable");
        }
    }

    @Test
    void dataBoostServesAPartitionedRead() {
        BatchReadOnlyTransaction txn = transaction();
        List<Partition> partitions =
                txn.partitionQuery(
                        PartitionOptions.getDefaultInstance(),
                        Statement.of("SELECT id FROM singers"),
                        Options.dataBoostEnabled(true));

        int total = 0;
        for (Partition partition : partitions) {
            total += idsIn(txn, partition).size();
        }

        LOG.info("Data Boost planned {} partitions and returned {} rows", partitions.size(), total);
        // The first evidence anywhere in this repository that the flag does something rather than
        // being accepted and ignored: the emulator takes it and changes nothing, so a boosted read
        // returning the whole table is the measurement. It also exercises the
        // spanner.databases.useDataBoost permission, which a reader role does not carry.
        assertThat(total).isEqualTo(ROWS);
    }

    @Test
    void readsEveryRowThroughTheProductionClient() throws Exception {
        // No emulatorEndpoint(...), so the source builds its client over application-default
        // credentials — the one path every emulator test in this module skips.
        assertThat(run(builder -> builder)).containsExactlyInAnyOrderElementsOf(allIds());
    }

    @Test
    void readsEveryRowWithDataBoostThroughTheSource() throws Exception {
        assertThat(run(builder -> builder.dataBoostEnabled(true)))
                .containsExactlyInAnyOrderElementsOf(allIds());
    }

    // ---------------------------------------------------------------- helpers

    /** Plans the query and reports what the service said, for the table this test logs. */
    private static String plan(BatchReadOnlyTransaction txn, String sql) {
        try {
            int partitions =
                    txn.partitionQuery(PartitionOptions.getDefaultInstance(), Statement.of(sql))
                            .size();
            return "planned, " + partitions + " partition(s)";
        } catch (SpannerException e) {
            // The message, not only the code: the emulator refuses these shapes too, and what
            // distinguishes the two refusals — and tells a user which constraint they met — is the
            // wording.
            return "refused, " + e.getErrorCode() + ": " + e.getMessage();
        }
    }

    private static BatchReadOnlyTransaction transaction() {
        BatchClient batch =
                spanner()
                        .getBatchClient(
                                DatabaseId.of(
                                        database.getProject(),
                                        database.getInstance(),
                                        database.getDatabase()));
        return batch.batchReadOnlyTransaction(TimestampBound.strong());
    }

    private static List<Long> idsIn(BatchReadOnlyTransaction txn, Partition partition) {
        List<Long> ids = new ArrayList<>();
        try (ResultSet resultSet = txn.execute(partition)) {
            while (resultSet.next()) {
                ids.add(resultSet.getLong("id"));
            }
        }
        return ids;
    }

    private static List<Long> allIds() {
        return LongStream.range(0, ROWS).boxed().collect(Collectors.toList());
    }

    private static List<Long> run(UnaryOperator<SpannerSourceBuilder<Long>> configure)
            throws Exception {
        Configuration configuration = new Configuration();
        // A retry would hide a read failure behind a green job, which is the one outcome these
        // tests must not produce.
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(2);

        List<Long> ids = new ArrayList<>();
        try (CloseableIterator<Long> collected =
                env.fromSource(
                                configure
                                        .apply(
                                                SpannerSource.<Long>builder()
                                                        .database(database)
                                                        .readOperation(
                                                                SpannerReadOperation.query(
                                                                        Statement.of(
                                                                                "SELECT id FROM"
                                                                                        + " singers")))
                                                        .deserializer(new IdDeserializer()))
                                        .build(),
                                WatermarkStrategy.noWatermarks(),
                                "spanner")
                        .executeAndCollect()) {
            collected.forEachRemaining(ids::add);
        }
        return ids;
    }

    /** Reads the one column these tests select. */
    private static final class IdDeserializer implements SpannerStructDeserializationSchema<Long> {

        private static final long serialVersionUID = 1L;

        @Override
        @Nullable
        public Long deserialize(Struct row) {
            return row.getLong("id");
        }

        @Override
        public TypeInformation<Long> getProducedType() {
            return TypeInformation.of(Long.class);
        }
    }
}
