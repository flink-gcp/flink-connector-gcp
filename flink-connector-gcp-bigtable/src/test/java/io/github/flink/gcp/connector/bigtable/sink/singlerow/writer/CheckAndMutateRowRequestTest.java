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
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.RowOperation;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CheckAndMutateRowRequest}. */
class CheckAndMutateRowRequestTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "orders");
    private static final RequestContext CONTEXT = RequestContext.create("p", "i", "profile");
    private static final ByteString KEY = ByteString.copyFromUtf8("row-1");

    @Test
    void buildsTheClientsRequestAgainstTheDestinationItIsStartedFor() {
        FakeSingleRowClient client = new FakeSingleRowClient("p/i");
        CheckAndMutateRowRequest request =
                new CheckAndMutateRowRequest(
                        KEY,
                        Filters.FILTERS.family().exactMatch("cf"),
                        Mutation.create().setCell("cf", "matched", "yes"),
                        Mutation.create().setCell("cf", "matched", "no"));

        ApiFuture<Boolean> future = request.start(client, TABLE);

        // The table is the destination's, taken at start time, not something the request carries:
        // a destination resolved per record must be able to route one request type anywhere.
        com.google.bigtable.v2.CheckAndMutateRowRequest proto = sent(client, 0);
        assertThat(proto.getTableName()).isEqualTo("projects/p/instances/i/tables/orders");
        assertThat(proto.getRowKey()).isEqualTo(KEY);
        assertThat(proto.getPredicateFilter().getFamilyNameRegexFilter()).isEqualTo("cf");
        assertThat(proto.getTrueMutationsList()).hasSize(1);
        assertThat(proto.getTrueMutations(0).getSetCell().getValue().toStringUtf8())
                .isEqualTo("yes");
        assertThat(proto.getFalseMutationsList()).hasSize(1);
        assertThat(proto.getFalseMutations(0).getSetCell().getValue().toStringUtf8())
                .isEqualTo("no");
        assertThat(future).isSameAs(client.conditionalFutures.get(0));
        assertThat(request.operation()).isEqualTo(RowOperation.CHECK_AND_MUTATE_ROW);
        assertThat(request.rowKey()).isEqualTo(KEY);
    }

    @Test
    void leavesTheUnsetPartsUnset() {
        FakeSingleRowClient client = new FakeSingleRowClient("p/i");
        CheckAndMutateRowRequest request =
                new CheckAndMutateRowRequest(KEY, null, null, Mutation.create().deleteRow());

        request.start(client, TABLE);

        com.google.bigtable.v2.CheckAndMutateRowRequest proto = sent(client, 0);
        // No predicate means "the row has any value": the service's semantics, not a default the
        // connector substitutes.
        assertThat(proto.hasPredicateFilter()).isFalse();
        assertThat(proto.getTrueMutationsList()).isEmpty();
        assertThat(proto.getFalseMutationsList()).hasSize(1);
        assertThat(proto.getFalseMutations(0).hasDeleteFromRow()).isTrue();
    }

    @Test
    void eachStartBuildsItsOwnClientObject() {
        // The client's object is a mutable builder; sharing one across two destinations would
        // carry the first table into the second request.
        FakeSingleRowClient client = new FakeSingleRowClient("p/i");
        CheckAndMutateRowRequest request =
                new CheckAndMutateRowRequest(KEY, null, Mutation.create().deleteRow(), null);

        request.start(client, TABLE);
        request.start(client, TableDestination.of("p", "i", "other"));

        assertThat(client.conditionalMutations.get(0))
                .isNotSameAs(client.conditionalMutations.get(1));
        assertThat(sent(client, 0).getTableName())
                .isEqualTo("projects/p/instances/i/tables/orders");
        assertThat(sent(client, 1).getTableName()).isEqualTo("projects/p/instances/i/tables/other");
    }

    @Test
    void needsAThenOrAnOtherwise() {
        // The service accepts a request with neither and applies nothing; a program that built
        // one has a bug that would otherwise be one RPC per record for no effect.
        assertThatThrownBy(() -> new CheckAndMutateRowRequest(KEY, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("then or an otherwise");
        assertThatThrownBy(
                        () ->
                                new CheckAndMutateRowRequest(
                                        null, null, Mutation.create().deleteRow(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("rowKey");
    }

    /** The wire form of the {@code index}-th conditional mutation the client received. */
    private static com.google.bigtable.v2.CheckAndMutateRowRequest sent(
            FakeSingleRowClient client, int index) {
        return client.conditionalMutations.get(index).toProto(CONTEXT);
    }
}
