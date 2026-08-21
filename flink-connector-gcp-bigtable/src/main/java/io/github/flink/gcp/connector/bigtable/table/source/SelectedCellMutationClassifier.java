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

package io.github.flink.gcp.connector.bigtable.table.source;

import org.apache.flink.annotation.Internal;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutation;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

/** Recognizes the deliberately narrow producer protocol for one selected Bigtable cell. */
@Internal
final class SelectedCellMutationClassifier implements Serializable {

    private static final long serialVersionUID = 1L;

    enum Kind {
        UNRELATED,
        UPSERT,
        DELETE
    }

    static final class Result {
        private static final Result UNRELATED = new Result(Kind.UNRELATED, null);
        private static final Result DELETE = new Result(Kind.DELETE, null);

        private final Kind kind;
        @Nullable private final ByteString value;

        private Result(Kind kind, @Nullable ByteString value) {
            this.kind = kind;
            this.value = value;
        }

        static Result upsert(ByteString value) {
            return new Result(Kind.UPSERT, value);
        }

        Kind getKind() {
            return kind;
        }

        @Nullable
        ByteString getValue() {
            return value;
        }
    }

    private final String family;
    private final ByteString qualifier;
    private final String sourceClusterId;

    SelectedCellMutationClassifier(String family, ByteString qualifier, String sourceClusterId) {
        this.family = Objects.requireNonNull(family);
        this.qualifier = Objects.requireNonNull(qualifier);
        this.sourceClusterId = Objects.requireNonNull(sourceClusterId);
    }

    Result classify(BigtableChangeStreamMutation mutation) throws IOException {
        boolean deleted = false;
        @Nullable ByteString value = null;

        for (BigtableChangeStreamMutation.Entry entry : mutation.getEntries()) {
            if (entry instanceof BigtableChangeStreamMutation.DeleteCellsEntry) {
                BigtableChangeStreamMutation.DeleteCellsEntry delete =
                        (BigtableChangeStreamMutation.DeleteCellsEntry) entry;
                if (!isSelected(delete.getFamilyName(), delete.getQualifier())) {
                    continue;
                }
                validateHeader(mutation);
                if (!isUnbounded(delete.getTimestampRange())) {
                    throw protocolFailure(
                            "a timestamp-bounded delete cannot represent the selected logical"
                                    + " row");
                }
                if (deleted || value != null) {
                    throw protocolFailure(
                            "the selected cell is deleted more than once or after it is set");
                }
                deleted = true;
                continue;
            }
            if (entry instanceof BigtableChangeStreamMutation.DeleteFamilyEntry) {
                BigtableChangeStreamMutation.DeleteFamilyEntry delete =
                        (BigtableChangeStreamMutation.DeleteFamilyEntry) entry;
                if (!family.equals(delete.getFamilyName())) {
                    continue;
                }
                validateHeader(mutation);
                if (deleted || value != null) {
                    throw protocolFailure(
                            "the selected family is deleted more than once or after the cell is"
                                    + " set");
                }
                deleted = true;
                continue;
            }
            if (entry instanceof BigtableChangeStreamMutation.SetCellEntry) {
                BigtableChangeStreamMutation.SetCellEntry set =
                        (BigtableChangeStreamMutation.SetCellEntry) entry;
                if (!isSelected(set.getFamilyName(), set.getQualifier())) {
                    continue;
                }
                validateHeader(mutation);
                if (!deleted || value != null) {
                    throw protocolFailure(
                            "a selected SetCell must follow exactly one full selected-column or"
                                    + " selected-family delete");
                }
                value = set.getValue();
                continue;
            }
            if (entry instanceof BigtableChangeStreamMutation.AddToCellEntry) {
                BigtableChangeStreamMutation.AddToCellEntry add =
                        (BigtableChangeStreamMutation.AddToCellEntry) entry;
                if (couldSelect(add.getFamilyName(), add.getQualifier())) {
                    validateHeader(mutation);
                    throw protocolFailure("AddToCell cannot encode a complete logical row");
                }
                continue;
            }
            if (entry instanceof BigtableChangeStreamMutation.MergeToCellEntry) {
                BigtableChangeStreamMutation.MergeToCellEntry merge =
                        (BigtableChangeStreamMutation.MergeToCellEntry) entry;
                if (couldSelect(merge.getFamilyName(), merge.getQualifier())) {
                    validateHeader(mutation);
                    throw protocolFailure("MergeToCell cannot encode a complete logical row");
                }
                continue;
            }
            throw protocolFailure(
                    "the connector returned an unknown mutation entry type "
                            + entry.getClass().getName());
        }

        if (value != null) {
            return Result.upsert(value);
        }
        return deleted ? Result.DELETE : Result.UNRELATED;
    }

    private boolean isSelected(String entryFamily, ByteString entryQualifier) {
        return family.equals(entryFamily) && qualifier.equals(entryQualifier);
    }

    private boolean couldSelect(
            String entryFamily, BigtableChangeStreamMutation.Value entryQualifier) {
        if (!family.equals(entryFamily)) {
            return false;
        }
        return !(entryQualifier instanceof BigtableChangeStreamMutation.RawValue)
                || qualifier.equals(
                        ((BigtableChangeStreamMutation.RawValue) entryQualifier).getValue());
    }

    private static boolean isUnbounded(BigtableChangeStreamMutation.TimestampRange range) {
        return range.getStart().getType() == BigtableChangeStreamMutation.BoundType.UNBOUNDED
                && range.getEnd().getType() == BigtableChangeStreamMutation.BoundType.UNBOUNDED;
    }

    private void validateHeader(BigtableChangeStreamMutation mutation) throws IOException {
        if (mutation.getType() != BigtableChangeStreamMutation.MutationType.USER) {
            throw protocolFailure(
                    "a garbage-collection mutation affects the selected cell or family");
        }
        if (!sourceClusterId.equals(mutation.getSourceClusterId())) {
            throw protocolFailure(
                    "the mutation came from source cluster '"
                            + mutation.getSourceClusterId()
                            + "', not configured cluster '"
                            + sourceClusterId
                            + "'");
        }
    }

    private static IOException protocolFailure(String detail) {
        return new IOException("Invalid Bigtable selected-cell producer protocol: " + detail + ".");
    }
}
