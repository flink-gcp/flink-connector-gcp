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

package io.github.flink.gcp.connector.bigtable;

import org.apache.flink.annotation.Internal;

/**
 * Every metric name this connector registers itself, in one place so that this file is the
 * connector's inventory: what it reports can be read here without opening the writer.
 *
 * <p>Each connector has one of these, and comparing them is how the repository's metric naming
 * convention is held across connectors — a name that means the same thing in two connectors should
 * be spelled the same way, and a diff of these files is what shows it. The convention itself (a
 * counter names the event, a gauge names the state, and neither takes Flink's {@code num} prefix)
 * is recorded in the base module's {@code CLAUDE.md}.
 *
 * <p>What is <em>not</em> here: Flink's standard sink names, which come from {@code
 * SinkWriterMetricGroup} accessors rather than from a name, and the subgroup leaves {@code
 * base.metrics} registers on this connector's behalf ({@code errorClass.CODE.errors}). The
 * user-facing meaning of each name is on the connector's documentation page, not duplicated here.
 */
@Internal
public final class BigtableMetricNames {

    // Registered by the sink writer (BigtableWriterMetrics).
    public static final String IN_FLIGHT_MUTATIONS = "inFlightMutations";
    public static final String IN_FLIGHT_BYTES = "inFlightBytes";
    public static final String PARKED_MUTATIONS = "parkedMutations";

    /** Registered by the sink writer and by the scan source's reader, on their own groups. */
    public static final String RECORDS_SKIPPED = "recordsSkipped";

    public static final String TABLES_CREATED = "tablesCreated";
    public static final String COLUMN_FAMILIES_ADDED = "columnFamiliesAdded";

    // Registered by the scan source's reader (BigtableSourceReaderMetrics).
    public static final String ROWS_READ = "rowsRead";

    // Registered by the scan source's split enumerator (BigtableScanSplitEnumerator).
    public static final String SPLITS_ASSIGNED = "splitsAssigned";
    public static final String SPLITS_RETURNED = "splitsReturned";
    public static final String ROW_KEY_SAMPLES_TAKEN = "rowKeySamplesTaken";

    private BigtableMetricNames() {}
}
