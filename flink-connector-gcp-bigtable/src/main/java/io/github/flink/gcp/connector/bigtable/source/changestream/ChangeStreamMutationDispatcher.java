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

package io.github.flink.gcp.connector.bigtable.source.changestream;

import org.apache.flink.annotation.Internal;

import java.io.IOException;

/**
 * Dispatches a change-stream mutation entry or aggregate value to the connector code that handles
 * it.
 *
 * <p>{@link BigtableChangeStreamMutation.Entry} and {@link BigtableChangeStreamMutation.Value}
 * declare their visitor methods package-private, so this class is how the connector's other
 * packages reach them. Both hierarchies have private constructors, which makes the visitor
 * contracts below the complete set of cases: adding a subtype adds a visitor method, and every
 * handler then fails to compile until it states what the new case does.
 *
 * <p>The argument travels through {@code accept} rather than being captured in a visitor field, so
 * a handler can be a stateless singleton and cost no allocation per record. {@link
 * BigtableChangeStreamMutationSerializer} needs that: its {@code duplicate()} returns {@code this},
 * so one instance is shared across task threads.
 */
@Internal
public final class ChangeStreamMutationDispatcher {

    private ChangeStreamMutationDispatcher() {}

    /**
     * Handles every mutation entry type the connector models.
     *
     * @param <R> what handling an entry produces
     * @param <A> what the caller passes through to the handler
     */
    @Internal
    public interface EntryVisitor<R, A> {

        /** Handles one written cell version. */
        R visit(BigtableChangeStreamMutation.SetCellEntry entry, A argument) throws IOException;

        /** Handles one deletion of cell versions in a timestamp range. */
        R visit(BigtableChangeStreamMutation.DeleteCellsEntry entry, A argument) throws IOException;

        /** Handles one deletion of every cell in a family. */
        R visit(BigtableChangeStreamMutation.DeleteFamilyEntry entry, A argument)
                throws IOException;

        /** Handles one aggregate input added to a cell. */
        R visit(BigtableChangeStreamMutation.AddToCellEntry entry, A argument) throws IOException;

        /** Handles one aggregate input merged into a cell. */
        R visit(BigtableChangeStreamMutation.MergeToCellEntry entry, A argument) throws IOException;
    }

    /**
     * Handles every aggregate value type the connector models.
     *
     * @param <R> what handling a value produces
     * @param <A> what the caller passes through to the handler
     */
    @Internal
    public interface ValueVisitor<R, A> {

        /** Handles arbitrary bytes. */
        R visit(BigtableChangeStreamMutation.RawValue value, A argument) throws IOException;

        /** Handles a raw microsecond timestamp. */
        R visit(BigtableChangeStreamMutation.RawTimestamp value, A argument) throws IOException;

        /** Handles a signed 64-bit integer. */
        R visit(BigtableChangeStreamMutation.Int64Value value, A argument) throws IOException;
    }

    /** Dispatches one entry without allocating a per-record callback. */
    public static <R, A> R dispatchEntry(
            BigtableChangeStreamMutation.Entry entry, EntryVisitor<R, A> visitor, A argument)
            throws IOException {
        return entry.accept(visitor, argument);
    }

    /** Dispatches one aggregate value without allocating a per-record callback. */
    public static <R, A> R dispatchValue(
            BigtableChangeStreamMutation.Value value, ValueVisitor<R, A> visitor, A argument)
            throws IOException {
        return value.accept(visitor, argument);
    }
}
