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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.Schema;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

/** Every planned job and cleanup target for one destination in a commit. */
@Internal
final class DestinationCommitPlan {

    private static final Logger LOG = LoggerFactory.getLogger(DestinationCommitPlan.class);

    final TableDestination destination;

    /**
     * Reassigned only by {@link #layOutTempTables(TableLayout, TempTables)}, in the commit's
     * reconciliation phase.
     */
    List<PlannedLoad> loads;

    /**
     * Reassigned only by {@link #layOutTempTables(TableLayout, TempTables)}, in the commit's
     * reconciliation phase.
     */
    @Nullable DestinationCopy copy;

    @Nullable Schema reconciledSchema;

    DestinationCommitPlan(
            TableDestination destination, List<PlannedLoad> loads, @Nullable DestinationCopy copy) {
        this.destination = destination;
        this.loads = loads;
        this.copy = copy;
    }

    /** What laying out a destination's temporary tables needs from the service. */
    interface TempTables {

        /**
         * Returns whether the copy {@code copy} describes already succeeded, under its id or its
         * equivalent id (see {@link LoadJobRunner#copySucceeded(String, CopyJobSpec)}).
         */
        boolean copySucceeded(PlannedCopy copy) throws IOException;

        /**
         * Creates the laid-out temporary table, or extends the one that exists, and returns its
         * creation time (see {@code TableAdmin.prepareTemporaryTable}).
         */
        long prepare(TableDestination table) throws IOException;
    }

    /**
     * Gives this destination's temporary tables its live layout where the copy into it needs one
     * (ADR-0183), since a column- or range-partitioned or clustered destination refuses an
     * unpartitioned, unclustered source; and drops the loads and copies into temporary tables of
     * any destination whose final copy already succeeded.
     *
     * <p>Every temporary table is renamed with a salt of the layout, and every job that fills one
     * takes the salt in its id. A retried commit therefore neither re-attaches to jobs an earlier
     * version or an earlier layout ran, nor loads into a table they left, whose layout a load
     * cannot change. The final copy takes a fixed marker rather than the salt: its id differs from
     * the one an earlier version used, whose every attempt was refused, but stays the same across
     * layouts, and names its unsalted id as equivalent, so a final copy that already succeeded,
     * laid out or not, is re-attached rather than repeated.
     *
     * <p>Each renamed table is then prepared, created with an expiration or extended, before any
     * job writes it, and every job that fills one also names that table's incarnation, its creation
     * time: within one incarnation every retry derives the same ids and re-attaches, and a table
     * created anew after it expired is filled again rather than copied empty.
     *
     * <p>When the final copy, under its id or its equivalent one, already succeeded in an earlier
     * attempt, nothing is prepared, loaded or copied into a temporary table again, whether the
     * destination needs a layout now or not: the copy re-attaches to that job, which needs none of
     * them, and reloading would need staged files a lifecycle rule may have removed.
     *
     * <p>Ids and names stay unchanged for a destination without a copy hierarchy, one whose layout
     * a copy does not require (no layout, or ingestion-time partitioning alone), and under {@code
     * WRITE_TRUNCATE_DATA}, whose terminal query accepts temporary tables without one; the second
     * still drops its loads and intermediate copies once its final copy succeeded, and the first
     * and third are left entirely alone. Every load of a destination with a copy hierarchy fills a
     * temporary table, since the planner routes a destination through temporary tables as a whole.
     *
     * @param layout the destination's layout as reconciliation read it
     * @param tempTables the service the layout consults and prepares tables through
     * @throws IOException if the lookup or a preparation fails
     */
    void layOutTempTables(TableLayout layout, TempTables tempTables) throws IOException {
        if (copy == null || copy.terminalQuery != null) {
            return;
        }
        if (!layout.requiresLaidOutSources()) {
            // Asked here too: a destination that lost its only clustering since an attempt whose
            // laid-out final copy succeeded would otherwise load its files again, which fails if a
            // lifecycle rule removed them, before that copy is re-attached to.
            if (tempTables.copySucceeded(copy.finalCopy)) {
                logFinalCopySucceeded();
                loads = List.of();
                copy = copy.finalCopyOnly(copy.finalCopy, UnaryOperator.identity());
            }
            return;
        }
        String salt = CommitPlanner.layoutSalt(layout);
        Set<TableDestination> plannedTables = new HashSet<>(copy.cleanupTables);
        UnaryOperator<TableDestination> renameTemporary =
                table -> {
                    Preconditions.checkState(
                            plannedTables.contains(table),
                            "%s is not one of %s's temporary tables",
                            table,
                            destination);
                    return CommitPlanner.laidOutTable(table, salt);
                };
        UnaryOperator<TableDestination> rename =
                table -> table.equals(destination) ? table : renameTemporary.apply(table);
        PlannedCopy finalCopy = copy.laidOutFinalCopy(rename);
        if (tempTables.copySucceeded(finalCopy)) {
            logFinalCopySucceeded();
            loads = List.of();
            copy = copy.finalCopyOnly(finalCopy, rename);
            return;
        }
        LOG.info("Laying out the temporary tables of {} with its layout {}", destination, layout);
        Map<TableDestination, Long> creationTimes = new HashMap<>();
        for (TableDestination table : copy.cleanupTables) {
            TableDestination renamed = renameTemporary.apply(table);
            creationTimes.put(renamed, tempTables.prepare(renamed));
        }
        List<PlannedLoad> laidOutLoads = new ArrayList<>(loads.size());
        for (PlannedLoad load : loads) {
            // Every load fills a temporary table; a direct load into the destination is refused.
            TableDestination table = renameTemporary.apply(load.jobDestination);
            laidOutLoads.add(
                    load.laidOut(
                            table,
                            CommitPlanner.laidOutIncarnationJobId(
                                    load.jobId, salt, creationTimes, table),
                            layout));
        }
        loads = laidOutLoads;
        copy = copy.laidOut(rename, salt, creationTimes);
    }

    private void logFinalCopySucceeded() {
        LOG.info(
                "The final copy into {} already succeeded; re-attaching to it without loading or"
                        + " copying into its temporary tables again",
                destination);
    }
}
