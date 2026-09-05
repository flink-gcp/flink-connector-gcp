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

import org.apache.flink.util.InstantiationUtil;

import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConditionalRequestTest {
    private static final ByteString KEY = ByteString.copyFromUtf8("key");
    private static final ByteString QUALIFIER =
            ByteString.copyFrom(new byte[] {0, 10, (byte) 255, '.'});
    private static final TableDestination TABLE = TableDestination.of("p", "i", "actual");

    @Test
    void immutableBranchesSurviveJobSerializationAndResolveTheTableAtStart() throws Exception {
        List<ConditionalMutation> branch = new ArrayList<>();
        branch.add(ConditionalMutation.setCell("cf", QUALIFIER, -1, ByteString.EMPTY));
        ConditionalRequest original =
                ConditionalRequest.of(KEY, ConditionalFilter.rowExists(), List.of(), branch);
        branch.clear();
        assertThat(original.getOtherwiseMutations()).hasSize(1);
        assertThatThrownBy(() -> original.getOtherwiseMutations().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        ConditionalRequest restored =
                InstantiationUtil.clone(original, getClass().getClassLoader());
        CheckAndMutateRowRequest sent = sent(restored);
        assertThat(sent.getTableName()).isEqualTo("projects/p/instances/i/tables/actual");
        assertThat(sent.getRowKey()).isEqualTo(KEY);
        assertThat(sent.hasPredicateFilter()).isFalse();
        assertThat(sent.getTrueMutationsCount()).isZero();
        assertThat(sent.getFalseMutations(0).getSetCell().getTimestampMicros()).isEqualTo(-1);
        assertThat(sent.getFalseMutations(0).getSetCell().getColumnQualifier())
                .isEqualTo(QUALIFIER);
    }

    @Test
    void binaryEqualityAndLatestSelectionHaveAGoldenWireOrder() {
        ConditionalFilter predicate =
                ConditionalFilter.latestCellValueEquals("cf", QUALIFIER, QUALIFIER);
        List<com.google.bigtable.v2.RowFilter> chain =
                ConditionalRowRequest.filter(predicate).toProto().getChain().getFiltersList();
        assertThat(chain).hasSize(4);
        assertThat(chain.get(0).getFamilyNameRegexFilter()).isEqualTo("cf");
        ByteString quoted = ByteString.copyFrom(new byte[] {0, 92, 10, (byte) 255, 92, '.'});
        assertThat(chain.get(1).getColumnQualifierRegexFilter()).isEqualTo(quoted);
        assertThat(chain.get(2).getCellsPerColumnLimitFilter()).isEqualTo(1);
        assertThat(chain.get(3).getValueRegexFilter()).isEqualTo(quoted);
        assertThat(
                        ConditionalRowRequest.filter(
                                        ConditionalFilter.valueEquals(ByteString.EMPTY))
                                .toProto()
                                .getValueRegexFilter())
                .isEmpty();
    }

    @Test
    void allMutationKindsKeepOrderAndTypedAggregateValues() {
        List<ConditionalMutation> branch =
                List.of(
                        ConditionalMutation.setCell("cf", QUALIFIER, 1234, KEY),
                        ConditionalMutation.deleteCells("cf", QUALIFIER, 1L, 2345L),
                        ConditionalMutation.deleteFamily("cf"),
                        ConditionalMutation.deleteRow(),
                        ConditionalMutation.addToCell(
                                "sum", QUALIFIER, 0, AggregateValue.int64(-5)),
                        ConditionalMutation.mergeToCell(
                                "sum", QUALIFIER, 1000, AggregateValue.raw(KEY)));
        CheckAndMutateRowRequest request =
                sent(
                        ConditionalRequest.of(
                                KEY,
                                ConditionalFilter.cellExists("cf", QUALIFIER),
                                branch,
                                List.of(ConditionalMutation.deleteCells("cf", QUALIFIER))));
        List<com.google.bigtable.v2.Mutation> mutations = request.getTrueMutationsList();
        assertThat(mutations)
                .extracting(com.google.bigtable.v2.Mutation::getMutationCase)
                .containsExactly(
                        com.google.bigtable.v2.Mutation.MutationCase.SET_CELL,
                        com.google.bigtable.v2.Mutation.MutationCase.DELETE_FROM_COLUMN,
                        com.google.bigtable.v2.Mutation.MutationCase.DELETE_FROM_FAMILY,
                        com.google.bigtable.v2.Mutation.MutationCase.DELETE_FROM_ROW,
                        com.google.bigtable.v2.Mutation.MutationCase.ADD_TO_CELL,
                        com.google.bigtable.v2.Mutation.MutationCase.MERGE_TO_CELL);
        assertThat(mutations.get(0).getSetCell().getTimestampMicros()).isEqualTo(1234);
        assertThat(mutations.get(1).getDeleteFromColumn().getTimeRange().getStartTimestampMicros())
                .isEqualTo(1);
        assertThat(mutations.get(1).getDeleteFromColumn().getTimeRange().getEndTimestampMicros())
                .isEqualTo(2345);
        assertThat(mutations.get(4).getAddToCell().getInput().getIntValue()).isEqualTo(-5);
        assertThat(mutations.get(4).getAddToCell().getTimestamp().getKindCase())
                .isEqualTo(com.google.bigtable.v2.Value.KindCase.RAW_TIMESTAMP_MICROS);
        assertThat(mutations.get(4).getAddToCell().getTimestamp().getRawTimestampMicros()).isZero();
        assertThat(mutations.get(5).getMergeToCell().getInput().getRawValue()).isEqualTo(KEY);
        assertThat(request.getFalseMutations(0).getDeleteFromColumn().hasTimeRange()).isFalse();
    }

    @Test
    void typedBytesSurviveJobSerializationWithoutBecomingRawBytes() throws Exception {
        ConditionalRequest original =
                ConditionalRequest.of(
                        KEY,
                        ConditionalFilter.rowExists(),
                        List.of(
                                ConditionalMutation.addToCell(
                                        "bytes", QUALIFIER, 0, AggregateValue.bytes(QUALIFIER))),
                        List.of(
                                ConditionalMutation.mergeToCell(
                                        "sum",
                                        QUALIFIER,
                                        1000,
                                        AggregateValue.bytes(ByteString.EMPTY))));
        CheckAndMutateRowRequest request =
                sent(InstantiationUtil.clone(original, getClass().getClassLoader()));
        assertThat(request.getTrueMutations(0).getAddToCell().getInput().getKindCase())
                .isEqualTo(com.google.bigtable.v2.Value.KindCase.BYTES_VALUE);
        assertThat(request.getTrueMutations(0).getAddToCell().getInput().getBytesValue())
                .isEqualTo(QUALIFIER);
        assertThat(request.getFalseMutations(0).getMergeToCell().getInput().getKindCase())
                .isEqualTo(com.google.bigtable.v2.Value.KindCase.BYTES_VALUE);
        assertThat(request.getFalseMutations(0).getMergeToCell().getInput().getBytesValue())
                .isEqualTo(ByteString.EMPTY);
        assertThatThrownBy(() -> AggregateValue.bytes(null)).hasMessageContaining("bytes");
    }

    @Test
    void guardsEmptyRequestsCountsAndAggregateTimestamps() {
        assertThatThrownBy(
                        () ->
                                ConditionalRequest.of(
                                        KEY, ConditionalFilter.rowExists(), List.of(), List.of()))
                .hasMessageContaining("nonempty");
        List<ConditionalMutation> maximum =
                Collections.nCopies(100_000, ConditionalMutation.deleteRow());
        assertThat(
                        ConditionalRequest.of(KEY, ConditionalFilter.rowExists(), maximum, maximum)
                                .getThenMutations())
                .hasSize(100_000);
        assertThatThrownBy(
                        () ->
                                ConditionalRequest.of(
                                        KEY,
                                        ConditionalFilter.rowExists(),
                                        Collections.nCopies(
                                                100_001, ConditionalMutation.deleteRow()),
                                        List.of()))
                .hasMessageContaining("100000");
        assertThatThrownBy(
                        () -> ConditionalMutation.addToCell("cf", KEY, -1, AggregateValue.int64(1)))
                .hasMessageContaining("nonnegative");
        assertThatThrownBy(
                        () ->
                                ConditionalMutation.mergeToCell(
                                        "cf", KEY, -1, AggregateValue.raw(KEY)))
                .hasMessageContaining("nonnegative");
        assertThatThrownBy(() -> ConditionalMutation.setCell("cf", KEY, -2, KEY))
                .hasMessageContaining("-1");
        assertThatThrownBy(() -> ConditionalMutation.deleteCells("cf", KEY, 1L, 1L))
                .hasMessageContaining("exceed");
        assertThatThrownBy(() -> ConditionalFilter.chain()).hasMessageContaining("empty");
    }

    private static CheckAndMutateRowRequest sent(ConditionalRequest request) {
        ConditionalTestClients client = new ConditionalTestClients();
        new ConditionalRowRequest(request, EmptyBranchPolicy.IGNORE).start(client, TABLE);
        return client.sent.get(0).toProto(RequestContext.create("p", "i", "profile"));
    }
}
