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
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutation;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamMutationDispatcher;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;

/** Recognizes the deliberately narrow producer protocol for one selected Bigtable cell. */
@Internal
final class SelectedCellMutationClassifier implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final QualifierMatcher QUALIFIER_MATCHER = new QualifierMatcher();

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
        this.family = Preconditions.checkNotNull(family, "family must not be null");
        this.qualifier = Preconditions.checkNotNull(qualifier, "qualifier must not be null");
        this.sourceClusterId =
                Preconditions.checkNotNull(sourceClusterId, "sourceClusterId must not be null");
    }

    Result classify(BigtableChangeStreamMutation mutation) throws IOException {
        ProtocolFold fold = new ProtocolFold();
        for (BigtableChangeStreamMutation.Entry entry : mutation.getEntries()) {
            ChangeStreamMutationDispatcher.dispatchEntry(entry, fold, mutation);
        }
        return fold.result();
    }

    /**
     * Folds one mutation's entries into a verdict.
     *
     * <p>The running state is held here rather than returned per entry because the arms read it —
     * whether the selected cell is already deleted decides both what a later {@code SetCell} means
     * and which of two differently worded failures a second delete raises. One instance per
     * mutation, so the arms can throw where they detect the violation.
     */
    private final class ProtocolFold
            implements ChangeStreamMutationDispatcher.EntryVisitor<
                    Void, BigtableChangeStreamMutation> {

        private boolean deleted;
        @Nullable private ByteString value;

        @Override
        public Void visit(
                BigtableChangeStreamMutation.DeleteCellsEntry entry,
                BigtableChangeStreamMutation mutation)
                throws IOException {
            if (!isSelected(entry.getFamilyName(), entry.getQualifier())) {
                return null;
            }
            validateHeader(mutation);
            if (!isUnbounded(entry.getTimestampRange())) {
                throw protocolFailure(
                        "a timestamp-bounded delete cannot represent the selected logical row");
            }
            if (deleted || value != null) {
                throw protocolFailure(
                        "the selected cell is deleted more than once or after it is set");
            }
            deleted = true;
            return null;
        }

        @Override
        public Void visit(
                BigtableChangeStreamMutation.DeleteFamilyEntry entry,
                BigtableChangeStreamMutation mutation)
                throws IOException {
            if (!family.equals(entry.getFamilyName())) {
                return null;
            }
            validateHeader(mutation);
            if (deleted || value != null) {
                throw protocolFailure(
                        "the selected family is deleted more than once or after the cell is set");
            }
            deleted = true;
            return null;
        }

        @Override
        public Void visit(
                BigtableChangeStreamMutation.SetCellEntry entry,
                BigtableChangeStreamMutation mutation)
                throws IOException {
            if (!isSelected(entry.getFamilyName(), entry.getQualifier())) {
                return null;
            }
            validateHeader(mutation);
            if (!deleted || value != null) {
                throw protocolFailure(
                        "a selected SetCell must follow exactly one full selected-column or"
                                + " selected-family delete");
            }
            value = entry.getValue();
            return null;
        }

        @Override
        public Void visit(
                BigtableChangeStreamMutation.AddToCellEntry entry,
                BigtableChangeStreamMutation mutation)
                throws IOException {
            if (couldSelect(entry.getFamilyName(), entry.getQualifier())) {
                validateHeader(mutation);
                throw protocolFailure("AddToCell cannot encode a complete logical row");
            }
            return null;
        }

        @Override
        public Void visit(
                BigtableChangeStreamMutation.MergeToCellEntry entry,
                BigtableChangeStreamMutation mutation)
                throws IOException {
            if (couldSelect(entry.getFamilyName(), entry.getQualifier())) {
                validateHeader(mutation);
                throw protocolFailure("MergeToCell cannot encode a complete logical row");
            }
            return null;
        }

        Result result() {
            if (value != null) {
                return Result.upsert(value);
            }
            return deleted ? Result.DELETE : Result.UNRELATED;
        }
    }

    private boolean isSelected(String entryFamily, ByteString entryQualifier) {
        return family.equals(entryFamily) && qualifier.equals(entryQualifier);
    }

    private boolean couldSelect(
            String entryFamily, BigtableChangeStreamMutation.Value entryQualifier)
            throws IOException {
        if (!family.equals(entryFamily)) {
            return false;
        }
        return ChangeStreamMutationDispatcher.dispatchValue(
                entryQualifier, QUALIFIER_MATCHER, qualifier);
    }

    /**
     * Decides whether an aggregate entry's qualifier could be the selected one.
     *
     * <p>Only a raw qualifier can be compared; a computed one is treated as possibly selected, so
     * the caller reports the protocol violation rather than silently passing the mutation through.
     */
    private static final class QualifierMatcher
            implements ChangeStreamMutationDispatcher.ValueVisitor<Boolean, ByteString> {

        @Override
        public Boolean visit(BigtableChangeStreamMutation.RawValue value, ByteString selected) {
            return selected.equals(value.getValue());
        }

        @Override
        public Boolean visit(BigtableChangeStreamMutation.RawTimestamp value, ByteString selected) {
            return true;
        }

        @Override
        public Boolean visit(BigtableChangeStreamMutation.Int64Value value, ByteString selected) {
            return true;
        }
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
