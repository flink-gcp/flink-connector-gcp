/*
 * Copyright 2026 laughingman7743
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
import io.github.flink.gcp.connector.bigtable.sink.TableCreateOptions;

import java.io.IOException;

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
     * @param destination the table to ensure
     * @param options the families (and rules) to create it with
     * @return what the call actually did
     * @throws IOException if the table or a missing family could not be created for any reason
     *     other than already existing
     */
    EnsureResult ensureTable(TableDestination destination, TableCreateOptions options)
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

        private EnsureResult(boolean tableCreated, int columnFamiliesAdded) {
            this.tableCreated = tableCreated;
            this.columnFamiliesAdded = columnFamiliesAdded;
        }

        /**
         * The table was created, with every declared family. The families are part of the creation,
         * never also "added", which is what the two factories keeping the pair apart makes
         * unrepresentable.
         *
         * @return the result
         */
        public static EnsureResult created() {
            return new EnsureResult(true, 0);
        }

        /**
         * The table already existed and the given number of declared families it lacked were added.
         *
         * @param count how many families were added; zero when everything already existed
         * @return the result
         */
        public static EnsureResult familiesAdded(int count) {
            return new EnsureResult(false, count);
        }

        /** Returns whether the table was created (with every declared family). */
        public boolean tableCreated() {
            return tableCreated;
        }

        /** Returns how many declared families were added to an already-existing table. */
        public int columnFamiliesAdded() {
            return columnFamiliesAdded;
        }

        @Override
        public String toString() {
            return "EnsureResult{tableCreated="
                    + tableCreated
                    + ", columnFamiliesAdded="
                    + columnFamiliesAdded
                    + "}";
        }
    }
}
