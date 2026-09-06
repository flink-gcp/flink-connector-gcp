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

package io.github.flink.gcp.connector.bigtable.table.function;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.Row;

import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import io.grpc.Status;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.github.flink.gcp.connector.bigtable.table.function.SqlWriteTemplateTest.compile;
import static io.github.flink.gcp.connector.bigtable.table.function.SqlWriteTemplateTest.config;
import static io.github.flink.gcp.connector.bigtable.table.function.SqlWriteTemplateTest.increment;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(10)
class BigtableAsyncSqlRuntimeTest {
    private final SqlWriteTemplateTest.FakeClients clients = new SqlWriteTemplateTest.FakeClients();
    private final TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();

    @Test
    void falseIsSuccessfulAndOneClientIsReusedAndClosedOnce() throws Exception {
        var template =
                compile(
                        config("predicate.type", "row-exists", "then.0.operation", "delete-row"),
                        true);
        var function = new BigtableCheckAndMutateFunction(template);
        function.openRuntime(metrics, clients);
        try {
            CompletableFuture<Boolean> first = new CompletableFuture<>();
            CompletableFuture<Boolean> second = new CompletableFuture<>();
            function.eval(first, "test", StringData.fromString("a"));
            function.eval(second, "test", StringData.fromString("b"));
            assertThat(first).isNotDone();
            assertThat(second).isNotDone();
            assertThat(clients.opens).isEqualTo(1);
            clients.conditions.get(1).set(false);
            clients.conditions.get(0).set(true);
            assertThat(first.join()).isTrue();
            assertThat(second.join()).isFalse();
            assertThat(metrics.counterValue("bigtableSql", "test", "requestsCompleted"))
                    .isEqualTo(2);
            assertThat(metrics.counterValue("bigtableSql", "test", "predicatesNotMatched"))
                    .isEqualTo(1);
        } finally {
            function.close();
            function.close();
        }
        assertThat(clients.closes).isEqualTo(1);
    }

    @Test
    void nullOperandsNeverAcquireAClientAndDoNotPoisonTheNextInvocation() throws Exception {
        var function =
                new BigtableReadModifyWriteFunction(
                        compile(increment(), false, DataTypes.BIGINT()));
        function.openRuntime(metrics, clients);
        try {
            CompletableFuture<Row> invalid = new CompletableFuture<>();
            function.eval(invalid, "test", StringData.fromString("key"), (Object) null);
            assertThat(invalid).isCompletedExceptionally();
            assertThat(clients.opens).isZero();
            CompletableFuture<Row> valid = new CompletableFuture<>();
            function.eval(valid, "test", StringData.fromString("key"), 1L);
            assertThat(clients.rmw).hasSize(1);
            clients.rows
                    .get(0)
                    .set(
                            com.google.cloud.bigtable.data.v2.models.Row.create(
                                    ByteString.copyFromUtf8("key"),
                                    List.of(
                                            RowCell.create(
                                                    "cf",
                                                    ByteString.copyFromUtf8("count"),
                                                    1000,
                                                    List.of(),
                                                    ByteString.copyFrom(
                                                            new byte[] {
                                                                0, 0, 0, 0, 0, 0, 0, 1
                                                            })))));
            assertThat(((Row[]) valid.join().getField(1))[0].getField(4)).isEqualTo(1L);
        } finally {
            function.close();
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = Status.Code.class,
            names = {
                "INVALID_ARGUMENT",
                "NOT_FOUND",
                "DEADLINE_EXCEEDED",
                "UNAVAILABLE",
                "ABORTED",
                "CANCELLED"
            })
    void serviceFailuresCompleteExceptionallyWithoutResubmission(Status.Code code)
            throws Exception {
        var function =
                new BigtableReadModifyWriteFunction(
                        compile(increment(), false, DataTypes.BIGINT()));
        function.openRuntime(metrics, clients);
        try {
            CompletableFuture<Row> result = new CompletableFuture<>();
            function.eval(result, "test", StringData.fromString("key"), 1L);
            clients.rows.get(0).setException(Status.fromCode(code).asRuntimeException());
            assertThatThrownBy(result::join).hasStackTraceContaining(code.name());
            assertThat(clients.rmw).hasSize(1);
            assertThat(metrics.counterValue("bigtableSql", "test", "requestsFailed")).isEqualTo(1);
            assertThat(metrics.<Integer>gaugeValue("bigtableSql", "test", "inFlightRequests"))
                    .isZero();
        } finally {
            function.close();
        }
    }

    @Test
    void closeCancelsOutstandingRpcWithoutPublishingALateResult() throws Exception {
        var function =
                new BigtableReadModifyWriteFunction(
                        compile(increment(), false, DataTypes.BIGINT()));
        function.openRuntime(metrics, clients);
        CompletableFuture<Row> result = new CompletableFuture<>();
        function.eval(result, "test", StringData.fromString("key"), 1L);
        function.close();
        assertThat(clients.rows.get(0).isCancelled()).isTrue();
        // Flink owns completion during operator teardown; the request runtime drops late answers.
        assertThat(result).isNotDone();
        assertThat(
                        clients.rows
                                .get(0)
                                .set(
                                        com.google.cloud.bigtable.data.v2.models.Row.create(
                                                ByteString.copyFromUtf8("key"), List.of())))
                .isFalse();
        function.close();
        assertThat(clients.closes).isEqualTo(1);
        assertThat(metrics.<Integer>gaugeValue("bigtableSql", "test", "inFlightRequests")).isZero();
    }
}
