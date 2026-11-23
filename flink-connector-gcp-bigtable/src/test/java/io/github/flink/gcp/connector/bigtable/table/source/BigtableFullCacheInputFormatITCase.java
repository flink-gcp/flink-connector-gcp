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

import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableEmulatorITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;
import io.github.flink.gcp.connector.bigtable.table.TrailingBytes;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** The full lookup cache's input format against the emulator. */
class BigtableFullCacheInputFormatITCase extends AbstractBigtableEmulatorITCase {

    /** Rows of 64 KiB, enough that the read cannot finish inside gRPC's flow-control window. */
    private static final int ROWS = 128;

    private static final String VALUE = "x".repeat(64 * 1024);

    private static final BigtableTableSchema SCHEMA =
            BigtableTableSchema.of(
                    (RowType)
                            DataTypes.ROW(
                                            DataTypes.FIELD("rowkey", DataTypes.STRING()),
                                            DataTypes.FIELD(
                                                    FAMILY,
                                                    DataTypes.ROW(
                                                            DataTypes.FIELD(
                                                                    "q", DataTypes.STRING()))))
                                    .getLogicalType());

    /**
     * A load stopped part-way cancels its read: the data client's close waits up to six minutes for
     * a classic call still in flight and cancels none, so an abandoned stream, which the server
     * cannot finish while nothing reads it, would hold the close that long (#2264).
     */
    @Test
    void closingALoadStoppedPartWayCancelsItsReadAndReturnsPromptly() throws Exception {
        TableDestination destination = createTable("abandoned-load");
        for (int i = 0; i < ROWS; i++) {
            writeCell(destination, String.format("k%04d", i), FAMILY, "q", VALUE);
        }
        BigtableFullCacheInputFormat format =
                new BigtableFullCacheInputFormat(
                        destination,
                        SCHEMA,
                        null,
                        "null",
                        TrailingBytes.IGNORE,
                        Filters.FILTERS.pass(),
                        Collections.singletonList(ByteStringRange.unbounded()),
                        null,
                        null,
                        emulatorEndpoint(),
                        null,
                        false);

        Duration took;
        // A lease of the test's own keeps the group up across the timed close, so the time is the
        // client's close alone and not the last release's wait for the group.
        try (GrpcEventLoops.Lease held = GrpcEventLoops.acquire()) {
            format.open(new GenericInputSplit(0, 1));
            try {
                assertThat(format.reachedEnd()).isFalse();
                assertThat(format.nextRecord(null)).isNotNull();
            } finally {
                long started = System.nanoTime();
                format.close();
                took = Duration.ofNanos(System.nanoTime() - started);
            }
        }

        assertThat(took).as("closing the stopped load").isLessThan(Duration.ofSeconds(10));
        assertThat(GrpcEventLoops.isRunning()).as("the load's lease was kept").isFalse();
    }
}
