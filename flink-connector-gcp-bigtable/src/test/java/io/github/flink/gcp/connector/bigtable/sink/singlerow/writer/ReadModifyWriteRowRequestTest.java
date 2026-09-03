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

package io.github.flink.gcp.connector.bigtable.sink.singlerow.writer;

import com.google.api.core.ApiFuture;
import com.google.bigtable.v2.ReadModifyWriteRule;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRow;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.RowOperation;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.ReadModifyWriteRowRequest.Rule;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ReadModifyWriteRowRequest}. */
class ReadModifyWriteRowRequestTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "orders");
    private static final RequestContext CONTEXT = RequestContext.create("p", "i", "profile");
    private static final ByteString KEY = ByteString.copyFromUtf8("row-1");
    private static final ByteString QUALIFIER = ByteString.copyFromUtf8("q");

    @Test
    void appliesTheRulesInOrderAgainstTheDestinationItIsStartedFor() {
        FakeSingleRowClient client = new FakeSingleRowClient("p/i");
        ReadModifyWriteRowRequest request =
                new ReadModifyWriteRowRequest(
                        KEY,
                        Arrays.asList(
                                Rule.append("cf", QUALIFIER, ByteString.copyFromUtf8("-tail")),
                                Rule.increment("counters", QUALIFIER, 5L)));

        request.start(client, TABLE);

        com.google.bigtable.v2.ReadModifyWriteRowRequest proto = sent(client, 0);
        assertThat(proto.getTableName()).isEqualTo("projects/p/instances/i/tables/orders");
        assertThat(proto.getRowKey()).isEqualTo(KEY);
        assertThat(proto.getRulesList()).hasSize(2);
        ReadModifyWriteRule append = proto.getRules(0);
        assertThat(append.getFamilyName()).isEqualTo("cf");
        assertThat(append.getColumnQualifier()).isEqualTo(QUALIFIER);
        assertThat(append.getAppendValue().toStringUtf8()).isEqualTo("-tail");
        ReadModifyWriteRule increment = proto.getRules(1);
        assertThat(increment.getFamilyName()).isEqualTo("counters");
        assertThat(increment.getIncrementAmount()).isEqualTo(5L);
        assertThat(request.operation()).isEqualTo(RowOperation.READ_MODIFY_WRITE_ROW);
        assertThat(request.rowKey()).isEqualTo(KEY);
    }

    @Test
    void aRuleRefusesWhatTheClientsBuilderWouldRefuseAtStartTime() {
        // Mirrors the client's own checks (Validations.validateFamily, "Value can't be empty") so
        // the failure lands in the serializer that built the rule, not in the runtime's start.
        assertThatThrownBy(() -> Rule.append("", QUALIFIER, ByteString.copyFromUtf8("x")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("family must not be empty");
        assertThatThrownBy(() -> Rule.increment("", QUALIFIER, 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("family must not be empty");
        assertThatThrownBy(() -> Rule.append("cf", QUALIFIER, ByteString.EMPTY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("value must not be empty");
        assertThatThrownBy(() -> Rule.append("cf", QUALIFIER, null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> Rule.increment("cf", null, 1L))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> Rule.increment(null, QUALIFIER, 1L))
                .isInstanceOf(NullPointerException.class);
        // An empty qualifier is the client's and the service's to accept: no check here.
        assertThatCode(() -> Rule.increment("cf", ByteString.EMPTY, 1L)).doesNotThrowAnyException();
    }

    @Test
    void answersWithTheConnectorOwnedRowWhereTheClientAnswers() {
        FakeSingleRowClient client = new FakeSingleRowClient("p/i");
        ReadModifyWriteRowRequest request =
                new ReadModifyWriteRowRequest(
                        KEY, Collections.singletonList(Rule.increment("cf", QUALIFIER, 1L)));
        ApiFuture<BigtableRow> future = request.start(client, TABLE);
        AtomicReference<Thread> completingThread = new AtomicReference<>();
        AtomicReference<BigtableRow> answered = new AtomicReference<>();
        future.addListener(
                () -> {
                    completingThread.set(Thread.currentThread());
                    try {
                        answered.set(future.get());
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                },
                Runnable::run);
        Row row =
                Row.create(
                        KEY,
                        Arrays.asList(
                                RowCell.create(
                                        "cf",
                                        QUALIFIER,
                                        1_000L,
                                        Collections.singletonList("label"),
                                        ByteString.copyFromUtf8("v1")),
                                RowCell.create(
                                        "cf",
                                        ByteString.copyFromUtf8("q2"),
                                        2_000L,
                                        Collections.emptyList(),
                                        ByteString.copyFromUtf8("v2"))));

        assertThat(future.isDone()).isFalse();
        Thread answering =
                new Thread(() -> client.readModifyWriteFutures.get(0).set(row), "gax-answer");
        answering.start();
        try {
            answering.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }

        // The conversion ran on the answering thread — no executor sits between the client and
        // the runtime's callback — and what it produced mentions no client type.
        assertThat(completingThread.get()).isSameAs(answering);
        assertThat(answered.get())
                .isEqualTo(
                        new BigtableRow(
                                KEY,
                                Arrays.asList(
                                        new BigtableRow.Cell(
                                                "cf",
                                                QUALIFIER,
                                                1_000L,
                                                ByteString.copyFromUtf8("v1"),
                                                Collections.singletonList("label")),
                                        new BigtableRow.Cell(
                                                "cf",
                                                ByteString.copyFromUtf8("q2"),
                                                2_000L,
                                                ByteString.copyFromUtf8("v2"),
                                                Collections.emptyList()))));
    }

    @Test
    void cancellingTheAnswerCancelsTheClientsCallThroughItsOwnCancel() {
        // The runtime cancels on close and on the async operator's timeout. The client cancels the
        // RPC on the wire only from its future's own cancel override, and ApiFutures.transform
        // would not reach it: api-common unwraps an AbstractApiFuture to its internal future, and
        // Guava's cancel propagation marks that one cancelled directly. The fake records the
        // difference — isCancelled() alone was true under the transform too.
        FakeSingleRowClient client = new FakeSingleRowClient("p/i");
        ApiFuture<BigtableRow> future =
                new ReadModifyWriteRowRequest(
                                KEY, Collections.singletonList(Rule.increment("cf", QUALIFIER, 1L)))
                        .start(client, TABLE);

        assertThat(future.cancel(true)).isTrue();

        FakeAnswerFuture<Row> answer = client.readModifyWriteFutures.get(0);
        assertThat(answer.isCancelled()).isTrue();
        assertThat(answer.upstreamCancelled()).isTrue();
    }

    @Test
    void theClientsOwnCancellationCancelsTheAnswer() {
        // The other direction: the client giving up (its deadline, a shutdown) must read as a
        // cancellation to the runtime, not as a mapped failure with a CancellationException cause.
        FakeSingleRowClient client = new FakeSingleRowClient("p/i");
        ApiFuture<BigtableRow> future =
                new ReadModifyWriteRowRequest(
                                KEY, Collections.singletonList(Rule.increment("cf", QUALIFIER, 1L)))
                        .start(client, TABLE);

        client.readModifyWriteFutures.get(0).cancel(false);

        assertThat(future.isCancelled()).isTrue();
    }

    @Test
    void copiesTheRulesAndRejectsAnEmptyOrNullBearingList() {
        List<Rule> rules = new ArrayList<>();
        rules.add(Rule.increment("cf", QUALIFIER, 1L));
        ReadModifyWriteRowRequest request = new ReadModifyWriteRowRequest(KEY, rules);
        FakeSingleRowClient client = new FakeSingleRowClient("p/i");

        rules.clear();
        request.start(client, TABLE);

        assertThat(sent(client, 0).getRulesList()).hasSize(1);
        assertThatThrownBy(() -> new ReadModifyWriteRowRequest(KEY, Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one rule");
        assertThatThrownBy(() -> new ReadModifyWriteRowRequest(KEY, Arrays.asList((Rule) null)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("null");
        assertThatThrownBy(() -> new ReadModifyWriteRowRequest(null, rules))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("rowKey");
        assertThatThrownBy(() -> Rule.append(null, QUALIFIER, ByteString.EMPTY))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("family");
        assertThatThrownBy(() -> Rule.append("cf", QUALIFIER, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("value");
    }

    /** The wire form of the {@code index}-th read-modify-write the client received. */
    private static com.google.bigtable.v2.ReadModifyWriteRowRequest sent(
            FakeSingleRowClient client, int index) {
        return client.readModifyWrites.get(index).toProto(CONTEXT);
    }
}
