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

package io.github.flink.gcp.connector.bigtable.table.sink;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowRequestConfig;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowRequestWriter;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import io.grpc.Status;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static io.github.flink.gcp.connector.bigtable.table.sink.ConditionalCommandTestSupport.cell;
import static io.github.flink.gcp.connector.bigtable.table.sink.ConditionalCommandTestSupport.config;
import static io.github.flink.gcp.connector.bigtable.table.sink.ConditionalCommandTestSupport.options;
import static io.github.flink.gcp.connector.bigtable.table.sink.ConditionalCommandTestSupport.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(30)
class ConditionalCommandRuntimeTest {
    private final ConditionalCommandTestSupport.FakeClients client =
            new ConditionalCommandTestSupport.FakeClients();
    private final FakeMailboxExecutor mailbox = new FakeMailboxExecutor();
    private final TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();

    @Test
    void countsBothOutcomesAndCheckpointDrainWaitsForEveryAcceptedRequest() throws Exception {
        Map<String, String> options = options();
        options.put("sink.conditional.otherwise.0.operation", "delete-row");
        options.put("sink.conditional.empty-branch-policy", "fail");
        try (SingleRowRequestWriter<RowData> writer = writer(options, false)) {
            writer.write(row(), TestContexts.NO_OP);
            writer.write(row(), TestContexts.NO_OP);
            assertThat(metrics.counterValue("requestsCompleted")).isZero();
            mailbox.execute(() -> client.answers.get(0).set(true), "match");
            mailbox.execute(() -> client.answers.get(1).set(false), "miss");
            writer.flush(false);
            assertThat(metrics.counterValue("requestsCompleted")).isEqualTo(2);
            assertThat(metrics.counterValue("predicatesMatched")).isEqualTo(1);
            assertThat(metrics.counterValue("predicatesNotMatched")).isEqualTo(1);
            assertThat(metrics.counterValue("emptyBranchesSelected")).isZero();
            assertThat(metrics.<Integer>gaugeValue("inFlightRequests")).isZero();
        }
        assertThat(client.closes).isEqualTo(1);
    }

    @Test
    void policyFailureFollowsSuccessfulRpcAccountingEvenWithDroppingHandler() throws Exception {
        Map<String, String> options = options();
        options.put("sink.conditional.empty-branch-policy", "fail");
        try (SingleRowRequestWriter<RowData> writer = writer(options, true)) {
            writer.write(row(), TestContexts.NO_OP);
            client.answers.get(0).set(false);
            mailbox.drain();
            assertThatThrownBy(() -> writer.flush(false))
                    .hasMessageContaining("EmptyBranchPolicy.FAIL");
            assertThat(metrics.counterValue("requestsCompleted")).isEqualTo(1);
            assertThat(metrics.counterValue("predicatesNotMatched")).isEqualTo(1);
            assertThat(metrics.counterValue("emptyBranchesSelected")).isEqualTo(1);
            assertThat(metrics.counterValue("requestsFailed")).isZero();
            assertThat(metrics.<Integer>gaugeValue("inFlightRequests")).isZero();
        }
    }

    @Test
    void emptyBranchesSucceedWithTheDefaultPolicy() throws Exception {
        try (SingleRowRequestWriter<RowData> writer = writer(options(), false)) {
            writer.write(row(), TestContexts.NO_OP);
            client.answers.get(0).set(false);
            writer.flush(false);
            assertThat(metrics.counterValue("requestsCompleted")).isEqualTo(1);
            assertThat(metrics.counterValue("emptyBranchesSelected")).isEqualTo(1);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"then", "otherwise"})
    void nullInEitherBranchFailsBeforeClientCreationOrRpc(String branch) throws Exception {
        Map<String, String> options = options();
        cell(options, branch, 0, "set-cell");
        options.put("sink.conditional." + branch + ".0.value-column", "value");
        try (SingleRowRequestWriter<RowData> writer = writer(options, false)) {
            GenericRowData row = row();
            row.setField(2, null);
            assertThatThrownBy(() -> writer.write(row, TestContexts.NO_OP))
                    .hasStackTraceContaining("references a NULL input column");
            assertThat(client.sent).isEmpty();
            assertThat(client.opened).isZero();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"DEADLINE_EXCEEDED", "UNAVAILABLE", "INVALID_ARGUMENT"})
    void rpcFailuresReleaseCapacityAndNeverCountAPredicateOutcome(String code) throws Exception {
        try (SingleRowRequestWriter<RowData> writer = writer(options(), false)) {
            writer.write(row(), TestContexts.NO_OP);
            client.answers
                    .get(0)
                    .setException(
                            Status.fromCode(Status.Code.valueOf(code))
                                    .withDescription("test service failure")
                                    .asRuntimeException());
            mailbox.drain();
            assertThatThrownBy(() -> writer.flush(false)).hasStackTraceContaining(code);
            assertThat(metrics.counterValue("requestsCompleted")).isZero();
            assertThat(metrics.counterValue("predicatesMatched")).isZero();
            assertThat(metrics.counterValue("predicatesNotMatched")).isZero();
            assertThat(metrics.<Integer>gaugeValue("inFlightRequests")).isZero();
            assertThat(client.sent).hasSize(1);
        }
    }

    @Test
    void closeCancelsRequestsAndCancellationMailDoesNotCountAnOutcome() throws Exception {
        SingleRowRequestWriter<RowData> writer = writer(options(), false);
        writer.write(row(), TestContexts.NO_OP);
        writer.close();
        assertThat(client.answers.get(0).isCancelled()).isTrue();
        mailbox.drain();
        assertThat(metrics.counterValue("requestsCompleted")).isZero();
        assertThat(metrics.counterValue("predicatesMatched")).isZero();
        assertThat(client.closes).isEqualTo(1);
    }

    @Test
    void successQueuedBeforeCloseDoesNotCountAfterClose() throws Exception {
        SingleRowRequestWriter<RowData> writer = writer(options(), false);
        writer.write(row(), TestContexts.NO_OP);
        client.answers.get(0).set(true);
        assertThat(metrics.counterValue("requestsCompleted")).isZero();
        writer.close();
        mailbox.drain();
        assertThat(metrics.counterValue("requestsCompleted")).isZero();
        assertThat(metrics.counterValue("predicatesMatched")).isZero();
        assertThat(client.closes).isEqualTo(1);
    }

    private SingleRowRequestWriter<RowData> writer(Map<String, String> options, boolean drop)
            throws Exception {
        SingleRowRequestConfig<RowData> config = config(options);
        if (drop) {
            config =
                    new SingleRowRequestConfig<>(
                            config.getDestinationResolver(),
                            config.getSerializer(),
                            config.getAppProfileId(),
                            config.getRequestOptions(),
                            FailureHandler.logAndDrop(),
                            config.getServiceAccountKeyFile(),
                            config.getEmulatorEndpoint());
        }
        return new SingleRowRequestWriter<>(config, client, mailbox, metrics);
    }
}
