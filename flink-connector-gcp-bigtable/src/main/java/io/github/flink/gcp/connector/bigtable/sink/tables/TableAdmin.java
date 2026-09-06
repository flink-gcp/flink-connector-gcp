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

package io.github.flink.gcp.connector.bigtable.sink.tables;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.ColumnFamilyType;
import io.github.flink.gcp.connector.bigtable.sink.TableCreateOptions;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Table administration operations used by the sink, abstracting the Bigtable admin client so writer
 * logic can be unit-tested without one.
 *
 * <p>Instances are created on the task manager inside {@code createWriter} and are never shipped in
 * the job graph, so the interface is not {@link java.io.Serializable}. It is {@link AutoCloseable}
 * so implementations holding a gRPC client (for example one injected for tests) can shut down its
 * channel and threads with the writer.
 */
@Internal
public interface TableAdmin extends AutoCloseable {

    /**
     * Ensures the given table exists with every column family the options declare. Idempotent: when
     * the table is absent it is created with all the declared families and their garbage-collection
     * rules; when it exists — including because a parallel subtask won the creation race — only the
     * declared families it lacks are added, and existing families are left exactly as they are,
     * their garbage-collection rules neither compared nor updated.
     *
     * <p>Options containing any aggregate type require every declared existing type to match,
     * including raw declarations. Raw-only options reconcile existing families by name only.
     *
     * @param destination the table to ensure
     * @param options the families, value types and rules to create it with
     * @return what the call actually did
     * @throws IOException if table creation or family reconciliation fails for a reason other than
     *     a type mismatch
     * @throws IllegalStateException if aggregate declarations contain an existing family whose type
     *     does not match; retrying does not repair a type mismatch
     */
    EnsureResult ensureTable(TableDestination destination, TableCreateOptions options)
            throws IOException;

    /**
     * Checks declared existing family types without changing the table.
     *
     * @param destination the table to inspect
     * @param expected the declared family types
     * @param allowMissing whether absent tables and families may be repaired later
     * @throws IOException if metadata cannot be read or a required table is missing
     * @throws IllegalStateException if a declared family's type does not match, or if a required
     *     family is missing and {@code allowMissing} is false
     */
    void validateFamilies(
            TableDestination destination,
            Map<String, ColumnFamilyType> expected,
            boolean allowMissing)
            throws IOException;

    @Override
    void close() throws Exception;

    /**
     * What an {@link #ensureTable} call actually did, so the caller can report creation and
     * family-addition separately without this SPI knowing about metrics.
     */
    @Internal
    final class EnsureResult {

        private final boolean tableCreated;
        private final int columnFamiliesAdded;
        private final Set<String> existingColumnFamilies;

        private EnsureResult(
                boolean tableCreated, int columnFamiliesAdded, Set<String> existingColumnFamilies) {
            this.tableCreated = tableCreated;
            this.columnFamiliesAdded = columnFamiliesAdded;
            this.existingColumnFamilies =
                    Collections.unmodifiableSet(new LinkedHashSet<>(existingColumnFamilies));
        }

        /**
         * The table was created, with every declared family. The families are part of the creation,
         * never also "added", which is what the two factories keeping the pair apart makes
         * unrepresentable.
         *
         * @param columnFamilies the families created with the table
         * @return the result
         */
        public static EnsureResult created(Set<String> columnFamilies) {
            return new EnsureResult(true, 0, columnFamilies);
        }

        /**
         * The table already existed and the given number of declared families it lacked were added.
         *
         * @param count how many families were added; zero when everything already existed
         * @param existingColumnFamilies the families known to exist when the ensure completed
         * @return the result
         */
        public static EnsureResult familiesAdded(int count, Set<String> existingColumnFamilies) {
            return new EnsureResult(false, count, existingColumnFamilies);
        }

        /** Returns whether the table was created (with every declared family). */
        public boolean tableCreated() {
            return tableCreated;
        }

        /** Returns how many declared families were added to an already-existing table. */
        public int columnFamiliesAdded() {
            return columnFamiliesAdded;
        }

        /**
         * Returns the snapshot of families known to exist when the ensure completed.
         *
         * <p>For a created table these are the declared families sent with the creation. For an
         * existing table they are the families read from the table, plus any this call added.
         */
        public Set<String> existingColumnFamilies() {
            return existingColumnFamilies;
        }

        @Override
        public String toString() {
            return "EnsureResult{tableCreated="
                    + tableCreated
                    + ", columnFamiliesAdded="
                    + columnFamiliesAdded
                    + ", existingColumnFamilies="
                    + existingColumnFamilies
                    + "}";
        }
    }
}
