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

import org.apache.flink.annotation.Internal;

import com.google.api.core.ApiFuture;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.TimestampRange;
import com.google.bigtable.v2.Value;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.RowOperation;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.CheckAndMutateRowRequest;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.RowRequest;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowClient;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowRequestMetrics;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Adapts an immutable public request at invocation time and interprets its successful answer. */
@Internal
final class ConditionalRowRequest implements RowRequest<Boolean> {
    final ConditionalRequest request;
    private final EmptyBranchPolicy policy;

    ConditionalRowRequest(ConditionalRequest request, EmptyBranchPolicy policy) {
        this.request = request;
        this.policy = policy;
    }

    @Override
    public RowOperation operation() {
        return RowOperation.CHECK_AND_MUTATE_ROW;
    }

    @Override
    public ByteString rowKey() {
        return request.getRowKey();
    }

    @Override
    public ApiFuture<Boolean> start(SingleRowClient client, TableDestination destination) {
        return new CheckAndMutateRowRequest(
                        rowKey(),
                        request.getPredicate().kind == ConditionalFilter.Kind.ROW_EXISTS
                                ? null
                                : filter(request.getPredicate()),
                        mutations(request.getThenMutations()),
                        mutations(request.getOtherwiseMutations()))
                .start(client, destination);
    }

    @Override
    public void onSuccess(Object answer, SingleRowRequestMetrics metrics) throws IOException {
        boolean matched = (Boolean) answer;
        boolean nonempty = selectedBranchHasMutations(matched);
        metrics.conditionalOutcome(matched, nonempty);
        if (!nonempty && policy == EmptyBranchPolicy.FAIL) {
            throw new IOException(
                    "CheckAndMutateRow succeeded but selected an empty mutation branch"
                            + " (predicateMatched="
                            + matched
                            + "). EmptyBranchPolicy.FAIL fails the job."
                            + " A replay can select this branch after an earlier attempt already applied.");
        }
    }

    boolean selectedBranchHasMutations(boolean matched) {
        return !(matched ? request.getThenMutations() : request.getOtherwiseMutations()).isEmpty();
    }

    static Filters.Filter filter(ConditionalFilter filter) {
        switch (filter.kind) {
            case ROW_EXISTS:
                return Filters.FILTERS.pass();
            case FAMILY:
                return Filters.FILTERS.family().exactMatch(filter.family);
            case QUALIFIER:
                return Filters.FILTERS.qualifier().exactMatch(filter.bytes);
            case VALUE:
                return Filters.FILTERS.value().exactMatch(filter.bytes);
            case TIMESTAMP:
                return Filters.FILTERS
                        .timestamp()
                        .range()
                        .startClosed(filter.start)
                        .endOpen(filter.end);
            case COLUMN_LIMIT:
                return Filters.FILTERS.limit().cellsPerColumn(filter.count);
            case ROW_LIMIT:
                return Filters.FILTERS.limit().cellsPerRow(filter.count);
            case CHAIN:
                Filters.ChainFilter chain = Filters.FILTERS.chain();
                for (ConditionalFilter child : filter.children) {
                    chain.filter(filter(child));
                }
                return chain;
            case INTERLEAVE:
                Filters.InterleaveFilter interleave = Filters.FILTERS.interleave();
                for (ConditionalFilter child : filter.children) {
                    interleave.filter(filter(child));
                }
                return interleave;
            default:
                throw new IllegalStateException("Unknown conditional filter " + filter.kind);
        }
    }

    @Nullable
    static com.google.cloud.bigtable.data.v2.models.Mutation mutations(
            List<ConditionalMutation> branch) {
        if (branch.isEmpty()) {
            return null;
        }
        // Build protos because SDK 2.82.0's Value wrapper cannot represent bytes_value. The
        // connector model validates timestamps and branch counts before reaching this adapter.
        List<Mutation> result = new ArrayList<>(branch.size());
        for (ConditionalMutation mutation : branch) {
            Mutation.Builder proto = Mutation.newBuilder();
            switch (mutation.kind) {
                case SET_CELL:
                    proto.setSetCell(
                            Mutation.SetCell.newBuilder()
                                    .setFamilyName(mutation.family)
                                    .setColumnQualifier(mutation.qualifier)
                                    .setTimestampMicros(mutation.timestamp)
                                    .setValue(mutation.value));
                    break;
                case DELETE_CELLS:
                    Mutation.DeleteFromColumn.Builder delete =
                            Mutation.DeleteFromColumn.newBuilder()
                                    .setFamilyName(mutation.family)
                                    .setColumnQualifier(mutation.qualifier);
                    if (mutation.start != null || mutation.end != null) {
                        TimestampRange.Builder range = TimestampRange.newBuilder();
                        if (mutation.start != null) {
                            range.setStartTimestampMicros(mutation.start);
                        }
                        if (mutation.end != null) {
                            range.setEndTimestampMicros(mutation.end);
                        }
                        delete.setTimeRange(range);
                    }
                    proto.setDeleteFromColumn(delete);
                    break;
                case DELETE_FAMILY:
                    proto.setDeleteFromFamily(
                            Mutation.DeleteFromFamily.newBuilder().setFamilyName(mutation.family));
                    break;
                case DELETE_ROW:
                    proto.setDeleteFromRow(Mutation.DeleteFromRow.getDefaultInstance());
                    break;
                case ADD_TO_CELL:
                    proto.setAddToCell(
                            Mutation.AddToCell.newBuilder()
                                    .setFamilyName(mutation.family)
                                    .setColumnQualifier(raw(mutation.qualifier))
                                    .setTimestamp(timestamp(mutation.timestamp))
                                    .setInput(value(mutation.aggregate)));
                    break;
                case MERGE_TO_CELL:
                    proto.setMergeToCell(
                            Mutation.MergeToCell.newBuilder()
                                    .setFamilyName(mutation.family)
                                    .setColumnQualifier(raw(mutation.qualifier))
                                    .setTimestamp(timestamp(mutation.timestamp))
                                    .setInput(value(mutation.aggregate)));
                    break;
                default:
                    throw new IllegalStateException(
                            "Unknown conditional mutation " + mutation.kind);
            }
            result.add(proto.build());
        }
        // Only SetCell may carry the explicit -1 server-time sentinel.
        return com.google.cloud.bigtable.data.v2.models.Mutation.fromProtoUnsafe(result);
    }

    private static Value raw(ByteString bytes) {
        return Value.newBuilder().setRawValue(bytes).build();
    }

    private static Value timestamp(long micros) {
        return Value.newBuilder().setRawTimestampMicros(micros).build();
    }

    private static Value value(AggregateValue value) {
        switch (value.kind) {
            case RAW:
                return raw(value.bytes);
            case BYTES:
                return Value.newBuilder().setBytesValue(value.bytes).build();
            case INT64:
                return Value.newBuilder().setIntValue(value.integer).build();
            default:
                throw new IllegalStateException("Unknown aggregate value " + value.kind);
        }
    }
}
