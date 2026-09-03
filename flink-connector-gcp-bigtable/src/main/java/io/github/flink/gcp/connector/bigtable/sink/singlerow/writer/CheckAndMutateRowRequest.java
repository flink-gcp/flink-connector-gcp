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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.RowOperation;

import javax.annotation.Nullable;

/**
 * A {@code CheckAndMutateRow}: a predicate filter over one row, the mutations to apply when it
 * matches at least one cell, and the mutations to apply when it matches none. The answer is whether
 * it matched.
 *
 * <p>The condition and the branches are the client's own model types: this request is the runtime's
 * internal carrier, and the connector-owned request model a user builds arrives with the
 * conditional sink. At least one branch is required at construction, ahead of the client's own
 * check at start time, so a serializer that produced an empty request is told at the record it came
 * from.
 */
@Internal
public final class CheckAndMutateRowRequest implements RowRequest<Boolean> {

    private final ByteString rowKey;
    @Nullable private final Filters.Filter condition;
    @Nullable private final Mutation thenMutation;
    @Nullable private final Mutation otherwiseMutation;

    /**
     * Creates the request.
     *
     * @param rowKey the row to check and mutate
     * @param condition the predicate filter, or {@code null} for none — with the predicate unset
     *     the service checks that the row contains any value at all
     * @param thenMutation the mutations applied when the predicate matches, or {@code null} for
     *     none
     * @param otherwiseMutation the mutations applied when it does not, or {@code null} for none
     */
    public CheckAndMutateRowRequest(
            ByteString rowKey,
            @Nullable Filters.Filter condition,
            @Nullable Mutation thenMutation,
            @Nullable Mutation otherwiseMutation) {
        this.rowKey = Preconditions.checkNotNull(rowKey, "rowKey must not be null");
        Preconditions.checkArgument(
                thenMutation != null || otherwiseMutation != null,
                "A CheckAndMutateRow request needs a then or an otherwise mutation");
        this.condition = condition;
        this.thenMutation = thenMutation;
        this.otherwiseMutation = otherwiseMutation;
    }

    @Override
    public RowOperation operation() {
        return RowOperation.CHECK_AND_MUTATE_ROW;
    }

    @Override
    public ByteString rowKey() {
        return rowKey;
    }

    @Override
    public ApiFuture<Boolean> start(SingleRowClient client, TableDestination destination) {
        // The TargetId overload: the String one is deprecated, and TableId is the TargetId a table
        // has. Built per start, since the client's object is a mutable builder.
        ConditionalRowMutation mutation =
                ConditionalRowMutation.create(TableId.of(destination.getTable()), rowKey);
        if (condition != null) {
            mutation.condition(condition);
        }
        if (thenMutation != null) {
            mutation.then(thenMutation);
        }
        if (otherwiseMutation != null) {
            mutation.otherwise(otherwiseMutation);
        }
        return client.checkAndMutateRow(mutation);
    }
}
