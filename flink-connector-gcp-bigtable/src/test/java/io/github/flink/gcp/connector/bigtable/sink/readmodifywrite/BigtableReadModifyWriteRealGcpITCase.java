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

package io.github.flink.gcp.connector.bigtable.sink.readmodifywrite;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.util.CloseableIterator;

import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.models.AppProfile;
import com.google.cloud.bigtable.admin.v2.models.CreateAppProfileRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableRealGcpITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRow;
import io.github.flink.gcp.connector.bigtable.sink.writer.BigtableErrorClassifier;
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
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Service acceptance on one ephemeral instance removed by the inherited teardown. */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BIGTABLE_IT_PROJECT", matches = ".+")
class BigtableReadModifyWriteRealGcpITCase extends AbstractBigtableRealGcpITCase {
    private static final Logger LOG =
            LoggerFactory.getLogger(BigtableReadModifyWriteRealGcpITCase.class);
    private static final String ENABLED = "rmw-enabled";

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
                    CreateAppProfileRequest.of(instance, "rmw-disabled")
                            .setRoutingPolicy(
                                    AppProfile.SingleClusterRoutingPolicy.of(cluster, false)));
            admin.createAppProfile(
                    CreateAppProfileRequest.of(instance, "rmw-multi")
                            .setRoutingPolicy(AppProfile.MultiClusterRoutingPolicy.of()));
        }
    }

    @Test
    void orderedMixedRulesReturnTheFinalChangedCellsAndMatchDirectReads() throws Exception {
        TableDestination table = createTable("rmw-ordered");
        mutateRow(
                table,
                bytes("row"),
                mutation ->
                        mutation.setCell("cf", "note", 1000, "some")
                                .setCell("cf", "untouched", 1000, "keep"));
        ReadModifyWriteRequest request =
                ReadModifyWriteRequest.of(
                        bytes("row"),
                        List.of(
                                ReadModifyWriteRule.append("cf", bytes("note"), bytes("thing")),
                                ReadModifyWriteRule.increment("cf", bytes("count"), 8),
                                ReadModifyWriteRule.append("cf", bytes("note"), bytes("body")),
                                ReadModifyWriteRule.increment("cf", bytes("count"), -3)));
        BigtableRow result = execute(table, request).getRow();
        assertThat(result.getCells()).hasSize(2);
        Row stored = readRows(table).get(0);
        for (BigtableRow.Cell cell : result.getCells()) {
            com.google.cloud.bigtable.data.v2.models.RowCell direct =
                    stored.getCells().stream()
                            .filter(
                                    c ->
                                            c.getFamily().equals(cell.getFamily())
                                                    && c.getQualifier().equals(cell.getQualifier()))
                            .findFirst()
                            .orElseThrow();
            assertThat(cell.getValue()).isEqualTo(direct.getValue());
            assertThat(cell.getTimestampMicros()).isEqualTo(direct.getTimestamp());
        }
        assertThat(stored.getCells("cf", "note").get(0).getValue())
                .isEqualTo(bytes("somethingbody"));
        assertThat(number(stored.getCells("cf", "count").get(0).getValue())).isEqualTo(5);
        assertThat(stored.getCells("cf", "untouched").get(0).getValue()).isEqualTo(bytes("keep"));
    }

    @Test
    void signedBoundariesMatchNativeSuccessesOrRejections() throws Exception {
        TableDestination table = createTable("rmw-boundaries");
        long[] initial = {Long.MAX_VALUE, Long.MIN_VALUE, 0, 0};
        long[] amounts = {1, -1, Long.MIN_VALUE, Long.MAX_VALUE};
        try (BigtableDataClient nativeClient =
                BigtableDataClient.create(
                        BigtableDataSettings.newBuilder()
                                .setProjectId(PROJECT)
                                .setInstanceId(table.getInstance())
                                .setAppProfileId(ENABLED)
                                .build())) {
            for (int i = 0; i < initial.length; i++) {
                long value = initial[i];
                long amount = amounts[i];
                String nativeKey = "native-" + i;
                String connectorKey = "connector-" + i;
                mutateRow(
                        table,
                        bytes(nativeKey),
                        mutation -> mutation.setCell("cf", bytes("n"), 1000, integer(value)));
                mutateRow(
                        table,
                        bytes(connectorKey),
                        mutation -> mutation.setCell("cf", bytes("n"), 1000, integer(value)));
                AtomicReference<ByteString> expected = new AtomicReference<>();
                Throwable nativeFailure =
                        catchThrowable(
                                () ->
                                        expected.set(
                                                nativeClient
                                                        .readModifyWriteRow(
                                                                ReadModifyWriteRow.create(
                                                                                TableId.of(
                                                                                        table
                                                                                                .getTable()),
                                                                                bytes(nativeKey))
                                                                        .increment(
                                                                                "cf",
                                                                                bytes("n"),
                                                                                amount))
                                                        .getCells()
                                                        .get(0)
                                                        .getValue()));
                AtomicReference<BigtableRow> actual = new AtomicReference<>();
                Throwable connectorFailure =
                        catchThrowable(
                                () ->
                                        actual.set(
                                                execute(
                                                                table,
                                                                ReadModifyWriteRequest.of(
                                                                        bytes(connectorKey),
                                                                        List.of(
                                                                                ReadModifyWriteRule
                                                                                        .increment(
                                                                                                "cf",
                                                                                                bytes(
                                                                                                        "n"),
                                                                                                amount))))
                                                        .getRow()));
                if (nativeFailure == null) {
                    assertThat(connectorFailure).isNull();
                    assertThat(actual.get().getCells().get(0).getValue()).isEqualTo(expected.get());
                    assertThat(expected.get().size()).isEqualTo(Long.BYTES);
                    assertThat(storedInteger(table, connectorKey)).isEqualTo(expected.get());
                    LOG.info(
                            "ReadModifyWriteRow native boundary {} + {} = {}",
                            value,
                            amount,
                            number(expected.get()));
                } else {
                    StatusCode.Code status = BigtableErrorClassifier.statusCode(nativeFailure);
                    assertThat(status)
                            .isIn(
                                    StatusCode.Code.INVALID_ARGUMENT,
                                    StatusCode.Code.FAILED_PRECONDITION);
                    assertThat(connectorFailure).isNotNull();
                    // Flink can transport the vendor failure as SerializedThrowable.
                    assertThat(connectorFailure).hasStackTraceContaining(status.name() + ":");
                    assertThat(storedInteger(table, nativeKey)).isEqualTo(integer(value));
                    assertThat(storedInteger(table, connectorKey)).isEqualTo(integer(value));
                    LOG.info(
                            "ReadModifyWriteRow native boundary {} + {} rejected with {}",
                            value,
                            amount,
                            status);
                }
            }
        }
    }

    @Test
    void invalidStoredIntegerRejectsTheWholeRequest() throws Exception {
        TableDestination table = createTable("rmw-invalid-integer");
        mutateRow(
                table,
                bytes("row"),
                mutation ->
                        mutation.setCell("cf", "n", 1000, "bad")
                                .setCell("cf", "note", 1000, "before"));
        ReadModifyWriteRequest request =
                ReadModifyWriteRequest.of(
                        bytes("row"),
                        List.of(
                                ReadModifyWriteRule.append("cf", bytes("note"), bytes("after")),
                                ReadModifyWriteRule.increment("cf", bytes("n"), 1)));
        assertThatThrownBy(() -> execute(table, request))
                .hasStackTraceContaining("INVALID_ARGUMENT");
        assertThat(readRows(table).get(0).getCells("cf", "note").get(0).getValue())
                .isEqualTo(bytes("before"));
        assertThat(readRows(table).get(0).getCells("cf", "n").get(0).getValue())
                .isEqualTo(bytes("bad"));
    }

    @Test
    void sqlExecutesBothModesWithNullOmissionAndRepeatedInputs() throws Exception {
        TableDestination table = createTable("rmw-sql");
        mutateRow(
                table, bytes("row"), mutation -> mutation.setCell("cf", "keep", 1000, "unchanged"));
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        String destination =
                "'connector'='bigtable', 'project'='"
                        + PROJECT
                        + "', 'instance'='"
                        + table.getInstance()
                        + "', 'table'='"
                        + table.getTable()
                        + "', 'sink.app-profile-id'='"
                        + ENABLED
                        + "'";
        env.executeSql(
                "CREATE TABLE notes (k STRING, cf ROW<note STRING, keep STRING, `raw` BYTES>) WITH ("
                        + destination
                        + ", 'sink.write-mode'='append')");
        env.executeSql(
                        "INSERT INTO notes VALUES ('row', ROW('a', CAST(NULL AS STRING), X'00FF')), ('row', ROW('a', CAST(NULL AS STRING), X'00FF'))")
                .await();
        env.executeSql(
                "CREATE TABLE counts (k STRING, cf ROW<n BIGINT>) WITH ("
                        + destination
                        + ", 'sink.write-mode'='increment')");
        env.executeSql(
                        "INSERT INTO counts VALUES ('row', ROW(3)), ('row', ROW(3)), ('row', ROW(-2)), ('row', ROW(0))")
                .await();
        Row stored = readRows(table).get(0);
        assertThat(stored.getCells("cf", "note").get(0).getValue()).isEqualTo(bytes("aa"));
        assertThat(stored.getCells("cf", "keep").get(0).getValue()).isEqualTo(bytes("unchanged"));
        assertThat(stored.getCells("cf", "raw").get(0).getValue().toByteArray())
                .containsExactly(0, -1, 0, -1);
        assertThat(number(stored.getCells("cf", "n").get(0).getValue())).isEqualTo(4);
    }

    @ParameterizedTest
    @ValueSource(strings = {"rmw-disabled", "rmw-multi"})
    void incompatibleProfilesPreserveTheServiceFailureAndRoutingHint(String profile)
            throws Exception {
        TableDestination table = createTable("routing-" + profile);
        Throwable nativeFailure;
        try (BigtableDataClient nativeClient =
                BigtableDataClient.create(
                        BigtableDataSettings.newBuilder()
                                .setProjectId(PROJECT)
                                .setInstanceId(table.getInstance())
                                .setAppProfileId(profile)
                                .build())) {
            nativeFailure =
                    catchThrowable(
                            () ->
                                    nativeClient.readModifyWriteRow(
                                            ReadModifyWriteRow.create(
                                                            TableId.of(table.getTable()),
                                                            bytes("native"))
                                                    .increment("cf", bytes("n"), 1)));
        }
        assertThat(nativeFailure).isNotNull();
        StatusCode.Code status = BigtableErrorClassifier.statusCode(nativeFailure);
        assertThat(status)
                .isIn(StatusCode.Code.INVALID_ARGUMENT, StatusCode.Code.FAILED_PRECONDITION);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.fromData("row")
                .sinkTo(
                        BigtableReadModifyWriteSink.<String>builder()
                                .table(table)
                                .appProfileId(profile)
                                .serializer(
                                        (key, context) ->
                                                ReadModifyWriteRequest.of(
                                                        bytes(key),
                                                        List.of(
                                                                ReadModifyWriteRule.increment(
                                                                        "cf", bytes("n"), 1))))
                                .build());
        Throwable failure = catchThrowable(env::execute);
        assertThat(failure).isNotNull().hasStackTraceContaining("single-cluster routing");
        // Flink can transport the vendor failure as SerializedThrowable.
        assertThat(failure).hasStackTraceContaining(status.name() + ":");
        LOG.info("Application profile {} rejected ReadModifyWriteRow", profile, failure);
        assertThat(readRows(table)).isEmpty();
    }

    private static ByteString storedInteger(TableDestination table, String key) {
        return readRows(table).stream()
                .filter(row -> row.getKey().equals(bytes(key)))
                .findFirst()
                .orElseThrow()
                .getCells("cf", "n")
                .get(0)
                .getValue();
    }

    private static ReadModifyWriteResult execute(
            TableDestination table, ReadModifyWriteRequest request) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        BigtableReadModifyWriteAsync<String> async =
                BigtableReadModifyWriteAsync.<String>builder()
                        .table(table)
                        .appProfileId(ENABLED)
                        .serializer((key, context) -> request)
                        .build();
        List<Tuple2<String, ReadModifyWriteResult>> results = new ArrayList<>();
        try (CloseableIterator<Tuple2<String, ReadModifyWriteResult>> iterator =
                async.unorderedWait(
                                env.fromData(request.getRowKey().toStringUtf8()),
                                Duration.ofSeconds(30))
                        .executeAndCollect()) {
            iterator.forEachRemaining(results::add);
        }
        assertThat(results).hasSize(1);
        assertThat(results.get(0).f1.getDestination()).isEqualTo(table);
        return results.get(0).f1;
    }

    private static ByteString bytes(String value) {
        return ByteString.copyFromUtf8(value);
    }

    private static ByteString integer(long value) {
        return ByteString.copyFrom(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
    }

    private static long number(ByteString value) {
        return ByteBuffer.wrap(value.toByteArray()).getLong();
    }
}
