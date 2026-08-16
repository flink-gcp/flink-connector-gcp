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

package io.github.flink.gcp.connector.bigtable.source.changestream.reader;

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigtable.data.v2.models.AddToCell;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.bigtable.data.v2.models.MergeToCell;
import com.google.cloud.bigtable.data.v2.models.Range;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.cloud.bigtable.data.v2.models.Value;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutationFilter;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamMutation;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Converts the pinned SDK mutation model to the connector-owned public model. */
@Internal
final class ChangeStreamMutationConverter {

    private static final int FILTERED_ENTRY_INITIAL_CAPACITY = 16;

    private ChangeStreamMutationConverter() {}

    static ChangeStreamMutation convert(
            com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation mutation)
            throws IOException {
        List<ChangeStreamMutation.Entry> entries = new ArrayList<>(mutation.getEntries().size());
        for (Entry entry : mutation.getEntries()) {
            entries.add(convertEntry(entry));
        }
        return new ChangeStreamMutation(
                mutation.getRowKey(),
                convertMutationType(mutation.getType()),
                mutation.getSourceClusterId(),
                mutation.getCommitTime(),
                mutation.getTieBreaker(),
                mutation.getToken(),
                mutation.getEstimatedLowWatermarkTime(),
                entries);
    }

    static Result convertFiltered(
            com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation mutation,
            BigtableChangeStreamMutationFilter filter)
            throws IOException {
        ChangeStreamMutation.MutationType mutationType = convertMutationType(mutation.getType());
        List<ChangeStreamMutation.Entry> entries = null;
        long removedEntries = 0;
        for (Entry entry : mutation.getEntries()) {
            validateEntry(entry);
            if (included(entry, filter)) {
                if (entries == null) {
                    entries =
                            new ArrayList<>(
                                    Math.min(
                                            mutation.getEntries().size(),
                                            FILTERED_ENTRY_INITIAL_CAPACITY));
                }
                entries.add(convertEntry(entry));
            } else {
                removedEntries++;
            }
        }
        if (entries == null && removedEntries > 0 && filter.skipsMessagesWithoutChange()) {
            return Result.skipped(removedEntries);
        }
        return Result.deliver(
                new ChangeStreamMutation(
                        mutation.getRowKey(),
                        mutationType,
                        mutation.getSourceClusterId(),
                        mutation.getCommitTime(),
                        mutation.getTieBreaker(),
                        mutation.getToken(),
                        mutation.getEstimatedLowWatermarkTime(),
                        entries == null ? java.util.Collections.emptyList() : entries),
                removedEntries);
    }

    private static boolean included(Entry entry, BigtableChangeStreamMutationFilter filter)
            throws IOException {
        String familyName = familyName(entry);
        if (!filter.includesFamily(familyName)) {
            return false;
        }
        if (entry instanceof DeleteFamily || !filter.hasQualifierFilters()) {
            return true;
        }
        return filter.includesQualifiedColumn(familyName, qualifier(entry));
    }

    private static String familyName(Entry entry) throws IOException {
        if (entry instanceof SetCell) {
            return ((SetCell) entry).getFamilyName();
        }
        if (entry instanceof DeleteCells) {
            return ((DeleteCells) entry).getFamilyName();
        }
        if (entry instanceof DeleteFamily) {
            return ((DeleteFamily) entry).getFamilyName();
        }
        if (entry instanceof AddToCell) {
            return ((AddToCell) entry).getFamily();
        }
        if (entry instanceof MergeToCell) {
            return ((MergeToCell) entry).getFamily();
        }
        throw unsupportedEntry(entry);
    }

    private static com.google.protobuf.ByteString qualifier(Entry entry) throws IOException {
        if (entry instanceof SetCell) {
            return ((SetCell) entry).getQualifier();
        }
        if (entry instanceof DeleteCells) {
            return ((DeleteCells) entry).getQualifier();
        }
        Value aggregateQualifier;
        if (entry instanceof AddToCell) {
            aggregateQualifier = ((AddToCell) entry).getQualifier();
        } else if (entry instanceof MergeToCell) {
            aggregateQualifier = ((MergeToCell) entry).getQualifier();
        } else {
            throw unsupportedEntry(entry);
        }
        if (aggregateQualifier instanceof Value.RawValue) {
            return ((Value.RawValue) aggregateQualifier).getValue();
        }
        throw new IOException(
                "Bigtable Change Streams "
                        + entry.getClass().getSimpleName()
                        + " returned a non-RAW_VALUE qualifier while qualifier filtering requires"
                        + " the service's documented RAW_VALUE qualifier.");
    }

    private static void validateEntry(Entry entry) throws IOException {
        if (entry instanceof SetCell
                || entry instanceof DeleteCells
                || entry instanceof DeleteFamily) {
            return;
        }
        if (entry instanceof AddToCell) {
            AddToCell add = (AddToCell) entry;
            validateValue(add.getQualifier());
            validateValue(add.getTimestamp());
            validateValue(add.getInput());
            return;
        }
        if (entry instanceof MergeToCell) {
            MergeToCell merge = (MergeToCell) entry;
            validateValue(merge.getQualifier());
            validateValue(merge.getTimestamp());
            validateValue(merge.getInput());
            return;
        }
        throw unsupportedEntry(entry);
    }

    private static void validateValue(Value value) throws IOException {
        if (!(value instanceof Value.RawValue)
                && !(value instanceof Value.RawTimestamp)
                && !(value instanceof Value.IntValue)) {
            throw unsupportedValue(value);
        }
    }

    private static ChangeStreamMutation.MutationType convertMutationType(
            com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation.MutationType type)
            throws IOException {
        switch (type) {
            case USER:
                return ChangeStreamMutation.MutationType.USER;
            case GARBAGE_COLLECTION:
                return ChangeStreamMutation.MutationType.GARBAGE_COLLECTION;
            default:
                throw new IOException(
                        "Unsupported Bigtable Change Streams mutation type: "
                                + type
                                + ". Upgrade the connector-owned mutation converter before accepting"
                                + " this SDK type.");
        }
    }

    private static ChangeStreamMutation.Entry convertEntry(Entry entry) throws IOException {
        if (entry instanceof SetCell) {
            SetCell set = (SetCell) entry;
            return new ChangeStreamMutation.SetCellEntry(
                    set.getFamilyName(), set.getQualifier(), set.getTimestamp(), set.getValue());
        }
        if (entry instanceof DeleteCells) {
            DeleteCells delete = (DeleteCells) entry;
            return new ChangeStreamMutation.DeleteCellsEntry(
                    delete.getFamilyName(),
                    delete.getQualifier(),
                    convertRange(delete.getTimestampRange()));
        }
        if (entry instanceof DeleteFamily) {
            return new ChangeStreamMutation.DeleteFamilyEntry(
                    ((DeleteFamily) entry).getFamilyName());
        }
        if (entry instanceof AddToCell) {
            AddToCell add = (AddToCell) entry;
            return new ChangeStreamMutation.AddToCellEntry(
                    add.getFamily(),
                    convertValue(add.getQualifier()),
                    convertValue(add.getTimestamp()),
                    convertValue(add.getInput()));
        }
        if (entry instanceof MergeToCell) {
            MergeToCell merge = (MergeToCell) entry;
            return new ChangeStreamMutation.MergeToCellEntry(
                    merge.getFamily(),
                    convertValue(merge.getQualifier()),
                    convertValue(merge.getTimestamp()),
                    convertValue(merge.getInput()));
        }
        throw unsupportedEntry(entry);
    }

    private static ChangeStreamMutation.Value convertValue(Value value) throws IOException {
        if (value instanceof Value.RawValue) {
            return new ChangeStreamMutation.RawValue(((Value.RawValue) value).getValue());
        }
        if (value instanceof Value.RawTimestamp) {
            return new ChangeStreamMutation.RawTimestamp(((Value.RawTimestamp) value).getValue());
        }
        if (value instanceof Value.IntValue) {
            return new ChangeStreamMutation.Int64Value(((Value.IntValue) value).getValue());
        }
        throw unsupportedValue(value);
    }

    private static ChangeStreamMutation.TimestampRange convertRange(Range.TimestampRange range) {
        return new ChangeStreamMutation.TimestampRange(
                range.getStartBound() == Range.BoundType.UNBOUNDED
                        ? ChangeStreamMutation.TimestampBound.unbounded()
                        : convertBound(range.getStartBound(), range.getStart()),
                range.getEndBound() == Range.BoundType.UNBOUNDED
                        ? ChangeStreamMutation.TimestampBound.unbounded()
                        : convertBound(range.getEndBound(), range.getEnd()));
    }

    private static ChangeStreamMutation.TimestampBound convertBound(
            Range.BoundType type, long timestampMicros) {
        switch (type) {
            case OPEN:
                return ChangeStreamMutation.TimestampBound.open(timestampMicros);
            case CLOSED:
                return ChangeStreamMutation.TimestampBound.closed(timestampMicros);
            case UNBOUNDED:
                return ChangeStreamMutation.TimestampBound.unbounded();
            default:
                throw new IllegalArgumentException("Unsupported Bigtable timestamp bound " + type);
        }
    }

    private static IOException unsupportedEntry(Entry entry) {
        return new IOException(
                "Unsupported Bigtable Change Streams entry type: "
                        + entry.getClass().getName()
                        + ". Upgrade the connector-owned mutation converter before accepting this"
                        + " SDK type.");
    }

    private static IOException unsupportedValue(Value value) {
        return new IOException(
                "Unsupported Bigtable Change Streams value type: "
                        + value.getClass().getName()
                        + ". Upgrade the connector-owned mutation converter before accepting this"
                        + " SDK type.");
    }

    static final class Result {
        @Nullable private final ChangeStreamMutation mutation;
        private final long removedEntries;

        private Result(@Nullable ChangeStreamMutation mutation, long removedEntries) {
            this.mutation = mutation;
            this.removedEntries = removedEntries;
        }

        private static Result deliver(ChangeStreamMutation mutation, long removedEntries) {
            return new Result(mutation, removedEntries);
        }

        private static Result skipped(long removedEntries) {
            return new Result(null, removedEntries);
        }

        boolean isSkipped() {
            return mutation == null;
        }

        ChangeStreamMutation getMutation() {
            if (mutation == null) {
                throw new IllegalStateException("A skipped result has no mutation");
            }
            return mutation;
        }

        long getRemovedEntries() {
            return removedEntries;
        }
    }
}
