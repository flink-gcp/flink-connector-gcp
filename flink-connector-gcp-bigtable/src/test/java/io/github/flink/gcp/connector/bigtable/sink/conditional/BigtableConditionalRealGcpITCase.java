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

package io.github.flink.gcp.connector.bigtable.sink.conditional;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.ExceptionUtils;

import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.AppProfile;
import com.google.cloud.bigtable.admin.v2.models.CreateAppProfileRequest;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.Type;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableRealGcpITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Service acceptance using one ephemeral instance, removed by the inherited class teardown. */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BIGTABLE_IT_PROJECT", matches = ".+")
class BigtableConditionalRealGcpITCase extends AbstractBigtableRealGcpITCase {
    private static final Logger LOG =
            LoggerFactory.getLogger(BigtableConditionalRealGcpITCase.class);
    private static final String ENABLED = "conditional-enabled";

    @BeforeAll
    static void createProfiles() throws Exception {
        String instance = tableDestination("unused").getInstance();
        try (BigtableInstanceAdminClient admin = BigtableInstanceAdminClient.create(PROJECT)) {
            String cluster = admin.listClusters(instance).get(0).getId();
            admin.createAppProfile(
                    CreateAppProfileRequest.of(instance, ENABLED)
                            .setRoutingPolicy(
                                    AppProfile.SingleClusterRoutingPolicy.of(cluster, true)));
            admin.createAppProfile(
                    CreateAppProfileRequest.of(instance, "conditional-disabled")
                            .setRoutingPolicy(
                                    AppProfile.SingleClusterRoutingPolicy.of(cluster, false)));
            admin.createAppProfile(
                    CreateAppProfileRequest.of(instance, "conditional-multi")
                            .setRoutingPolicy(AppProfile.MultiClusterRoutingPolicy.of()));
        }
    }

    @Test
    void sqlChecksUndeclaredFamiliesAndPreservesAnExistingRow() throws Exception {
        TableDestination table = tableDestination("conditional-sql");
        try (BigtableTableAdminClient admin =
                BigtableTableAdminClient.create(PROJECT, table.getInstance())) {
            admin.createTable(
                    CreateTableRequest.of(table.getTable()).addFamily("cf").addFamily("hidden"));
        }
        mutateRow(
                table,
                bytes("existing"),
                mutation -> mutation.setCell("hidden", "q", 1000, "keep"));
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        env.executeSql(
                "CREATE TABLE bt (k STRING, cf ROW<v STRING>) WITH ('connector'='bigtable',"
                        + " 'project'='"
                        + PROJECT
                        + "', 'instance'='"
                        + table.getInstance()
                        + "', 'table'='"
                        + table.getTable()
                        + "', 'sink.app-profile-id'='"
                        + ENABLED
                        + "', 'sink.write-mode'='insert-if-absent')");
        env.executeSql("INSERT INTO bt VALUES ('existing', ROW('replace')), ('new', ROW('first'))")
                .await();
        env.executeSql("INSERT INTO bt SELECT 'new', ROW('second')").await();
        List<Row> rows = readRows(table);
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).getCells("cf", "v")).isEmpty();
        assertThat(rows.get(0).getCells("hidden", "q").get(0).getValue()).isEqualTo(bytes("keep"));
        assertThat(rows.get(1).getCells("cf", "v")).hasSize(1);
        assertThat(rows.get(1).getCells("cf", "v").get(0).getValue()).isEqualTo(bytes("first"));
    }

    @Test
    void asyncExecutesBothOrderedBranchesIncludingCompatibleAggregateUpdates() throws Exception {
        TableDestination table = tableDestination("conditional-aggregate");
        try (BigtableTableAdminClient admin =
                BigtableTableAdminClient.create(PROJECT, table.getInstance())) {
            admin.createTable(
                    CreateTableRequest.of(table.getTable())
                            .addFamily("cf")
                            .addFamily("sum", Type.int64Sum()));
        }
        mutateRow(
                table,
                bytes("existing"),
                mutation ->
                        mutation.setCell("cf", "q", 1000, "seed").addToCell("sum", "count", 0, 3));
        // Read the service's encoded accumulator instead of assuming a state encoding.
        ByteString accumulator = readRows(table).get(0).getCells("sum", "count").get(0).getValue();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> input = env.fromData("existing", "absent");
        BigtableConditionalAsync<String> async =
                BigtableConditionalAsync.<String>builder()
                        .table(table)
                        .appProfileId(ENABLED)
                        .serializer(
                                (key, context) ->
                                        ConditionalRequest.of(
                                                bytes(key),
                                                ConditionalFilter.rowExists(),
                                                List.of(
                                                        ConditionalMutation.deleteRow(),
                                                        ConditionalMutation.setCell(
                                                                "cf",
                                                                bytes("done"),
                                                                2000,
                                                                bytes("then")),
                                                        ConditionalMutation.addToCell(
                                                                "sum",
                                                                bytes("count"),
                                                                0,
                                                                AggregateValue.int64(2)),
                                                        ConditionalMutation.mergeToCell(
                                                                "sum",
                                                                bytes("count"),
                                                                0,
                                                                AggregateValue.bytes(accumulator))),
                                                List.of(
                                                        ConditionalMutation.setCell(
                                                                "cf",
                                                                bytes("done"),
                                                                2000,
                                                                bytes("otherwise")))))
                        .build();
        List<Tuple2<String, ConditionalResult>> results = new ArrayList<>();
        try (CloseableIterator<Tuple2<String, ConditionalResult>> iterator =
                async.orderedWait(input, Duration.ofSeconds(30)).executeAndCollect()) {
            iterator.forEachRemaining(results::add);
        }
        assertThat(results).hasSize(2);
        assertThat(results.get(0).f1.isPredicateMatched()).isTrue();
        assertThat(results.get(1).f1.isPredicateMatched()).isFalse();
        assertThat(results)
                .allSatisfy(pair -> assertThat(pair.f1.getDestination()).isEqualTo(table));
        List<Row> rows = readRows(table);
        assertThat(rows.get(0).getCells("cf", "done").get(0).getValue())
                .isEqualTo(bytes("otherwise"));
        assertThat(rows.get(1).getCells("cf", "q")).isEmpty();
        assertThat(rows.get(1).getCells("cf", "done").get(0).getValue()).isEqualTo(bytes("then"));
        assertThat(
                        ByteBuffer.wrap(
                                        rows.get(1)
                                                .getCells("sum", "count")
                                                .get(0)
                                                .getValue()
                                                .toByteArray())
                                .getLong())
                .isEqualTo(5);
    }

    @ParameterizedTest
    @ValueSource(strings = {"conditional-disabled", "conditional-multi"})
    void theServiceRejectsIncompatibleRoutingAndTheConnectorPreservesItsCause(String profile) {
        TableDestination table = createTable("routing-" + profile);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.fromData("row")
                .sinkTo(
                        BigtableConditionalSink.<String>builder()
                                .table(table)
                                .appProfileId(profile)
                                .serializer(
                                        (key, context) ->
                                                ConditionalRequest.of(
                                                        bytes(key),
                                                        ConditionalFilter.rowExists(),
                                                        List.of(),
                                                        List.of(
                                                                ConditionalMutation.setCell(
                                                                        "cf",
                                                                        bytes("q"),
                                                                        1000,
                                                                        bytes("value")))))
                                .build());
        Throwable failure = catchThrowable(env::execute);
        assertThat(failure).isNotNull().hasStackTraceContaining("single-cluster routing");
        // Flink may transport the vendor exception as SerializedThrowable, preserving its
        // status and explanation in the message rather than its Java exception class.
        Throwable service =
                ExceptionUtils.findThrowableWithMessage(failure, "INVALID_ARGUMENT:")
                        .or(
                                () ->
                                        ExceptionUtils.findThrowableWithMessage(
                                                failure, "FAILED_PRECONDITION:"))
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "Missing Bigtable rejection status", failure));
        LOG.info(
                "Application profile {} rejected CheckAndMutateRow: {}",
                profile,
                service.getMessage());
        assertThat(service.getMessage())
                .containsPattern(
                        "(?is).*(transaction|conditional|single.cluster|check.?and.?mutate).*");
        assertThat(readRows(table)).isEmpty();
    }

    private static ByteString bytes(String value) {
        return ByteString.copyFromUtf8(value);
    }
}
