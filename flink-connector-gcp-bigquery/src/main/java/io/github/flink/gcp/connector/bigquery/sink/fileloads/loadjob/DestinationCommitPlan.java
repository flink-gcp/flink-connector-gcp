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
import java.util.Collections;
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
     * Reassigned only by {@link #prepareTempTables(TableLayout, TempTables)}, in the commit's
     * reconciliation phase.
     */
    List<PlannedLoad> loads;

    /**
     * Reassigned only by {@link #prepareTempTables(TableLayout, TempTables)}, in the commit's
     * reconciliation phase.
     */
    @Nullable DestinationCopy copy;

    @Nullable Schema reconciledSchema;

    /**
     * The Cloud KMS key that encrypts the destination, or {@code null} for none of its own; set
     * with {@code reconciledSchema}.
     */
    @Nullable String kmsKeyName;

    DestinationCommitPlan(
            TableDestination destination, List<PlannedLoad> loads, @Nullable DestinationCopy copy) {
        this.destination = destination;
        this.loads = loads;
        this.copy = copy;
    }

    /** What preparing a destination's temporary tables needs from the service. */
    interface TempTables {

        /**
         * Returns whether the copy {@code copy} describes already succeeded, under its id or its
         * equivalent id, once a job an earlier attempt left pending or running under them has
         * finished (see {@link LoadJobRunner#copySucceeded(String, CopyJobSpec)}).
         */
        boolean copySucceeded(PlannedCopy copy) throws IOException;

        /**
         * Returns whether the terminal query {@code query} describes already succeeded (see {@link
         * LoadJobRunner#querySucceeded(String, QueryJobSpec)}).
         */
        boolean querySucceeded(PlannedQuery query) throws IOException;

        /**
         * Creates the temporary table with {@code layout}, or extends the one that exists, and
         * returns its creation time (see {@code TableAdmin.prepareTemporaryTable}).
         */
        long prepare(TableDestination table, TableLayout layout) throws IOException;

        /**
         * Extends the temporary table if it exists, creating nothing, and returns whether it
         * existed (see {@code TableAdmin.extendTemporaryTable}).
         */
        boolean extend(TableDestination table) throws IOException;
    }

    /**
     * Creates this destination's temporary tables ahead of the jobs that fill them, with an
     * expiration, and gives them its live layout where the copy into it needs one (ADR-0183); and
     * drops the loads and copies into temporary tables of any destination whose final copy or
     * terminal query already succeeded, and those below an intermediate or aggregate copy that
     * already succeeded.
     *
     * <p>Every temporary table is prepared, created with an expiration or extended, before any job
     * writes it, and every job that fills one names that table's incarnation, its creation time:
     * within one incarnation every retry derives the same ids and re-attaches, and a table created
     * anew after it expired is filled again from the staged files rather than copied empty or not
     * found (#2290). Those jobs also take a salt of the reconciled schema, so a schema that changed
     * between attempts refills every table rather than leaving the copy sources whose schemas
     * differ. The final copy into the destination and the terminal query keep their ids, so an
     * earlier attempt's success under them still stands.
     *
     * <p>Where the copy requires a layout (a column- or range-partitioned or clustered destination,
     * which refuses an unpartitioned, unclustered source), every temporary table is also renamed
     * with a salt of the layout, and every job that fills one takes the salt in its id. A retried
     * commit therefore neither re-attaches to jobs an earlier version or an earlier layout ran, nor
     * loads into a table they left, whose layout a load cannot change. The final copy takes a fixed
     * marker rather than the salt: its id differs from the one an earlier version used, whose every
     * attempt was refused, but stays the same across layouts, and names its unsalted id as
     * equivalent. Every other destination keeps its tables' names and prepares them without a
     * layout: one whose copy accepts that (no layout, or ingestion-time partitioning alone), and
     * {@code WRITE_TRUNCATE_DATA}, whose terminal query does.
     *
     * <p>When the final copy, under its id or its equivalent one, or under {@code
     * WRITE_TRUNCATE_DATA} the terminal query, already succeeded in an earlier attempt, nothing is
     * prepared, loaded or copied into a temporary table again: the final job re-attaches to that
     * job, which needs none of them. One still pending or running is waited for before anything is
     * prepared, so it cannot read a table this attempt recreated empty; so is each intermediate or
     * aggregate copy, before any table it reads is prepared, and one that succeeded leaves the
     * tables below it unfilled. Preparing and filling anything after the final job succeeded would
     * recreate the tables a successful commit deleted and load its staged files, which it deleted
     * too, or which a lifecycle rule may have removed.
     *
     * <p>A destination without a copy hierarchy, whose loads go directly into it, is left alone.
     * Every load of a destination with one fills a temporary table, since the planner routes a
     * destination through temporary tables as a whole.
     *
     * @param layout the destination's layout as reconciliation read it
     * @param tempTables the service the preparation consults and prepares tables through
     * @throws IOException if a lookup or a preparation fails
     */
    void prepareTempTables(TableLayout layout, TempTables tempTables) throws IOException {
        if (copy == null) {
            return;
        }
        if (copy.terminalQuery != null) {
            if (tempTables.querySucceeded(copy.terminalQuery)) {
                LOG.info(
                        "The terminal query into {} already succeeded; re-attaching to it without"
                                + " loading or copying into its temporary tables again",
                        destination);
                loads = List.of();
                copy = copy.terminalQueryOnly();
                return;
            }
            prepareUnderPlannedNames(tempTables);
            return;
        }
        PlannedCopy unsaltedFinalCopy = Preconditions.checkNotNull(copy.finalCopy);
        if (!layout.requiresLaidOutSources()) {
            // Asked here too: a destination that lost its only clustering since an attempt whose
            // laid-out final copy succeeded would otherwise load its files again, which fails if a
            // lifecycle rule removed them, before that copy is re-attached to.
            if (tempTables.copySucceeded(unsaltedFinalCopy)) {
                logFinalCopySucceeded();
                loads = List.of();
                copy = copy.finalCopyOnly(unsaltedFinalCopy, UnaryOperator.identity());
                return;
            }
            prepareUnderPlannedNames(tempTables);
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
        UnaryOperator<String> baseJobId =
                withSchemaSalt(jobId -> CommitPlanner.laidOutJobId(jobId, salt));
        Map<TableDestination, Long> creationTimes =
                prepareTablesAndStampLoads(tempTables, renameTemporary, layout, layout, baseJobId);
        copy = Preconditions.checkNotNull(copy).laidOut(rename, baseJobId, creationTimes);
    }

    /**
     * Prepares the temporary tables of a destination whose copy or terminal query accepts them
     * without a layout, under their planned names, and has every job that fills one name its
     * incarnation.
     */
    private void prepareUnderPlannedNames(TempTables tempTables) throws IOException {
        LOG.info("Preparing the temporary tables of {} without a layout", destination);
        UnaryOperator<String> baseJobId = withSchemaSalt(UnaryOperator.identity());
        Map<TableDestination, Long> creationTimes =
                prepareTablesAndStampLoads(
                        tempTables, UnaryOperator.identity(), TableLayout.NONE, null, baseJobId);
        copy = Preconditions.checkNotNull(copy).incarnated(baseJobId, creationTimes);
    }

    /**
     * Returns {@code baseJobId} followed by the salt of the reconciled schema. A temporary table
     * that survived an attempt whose reconciled schema differed holds that schema, and a copy
     * refuses sources whose schemas differ, so every job that fills one under another schema runs
     * again rather than re-attaching. Every attempt extends the tables it prepares, so without the
     * salt a partial expiry and a schema change together would refuse the copy on every restart.
     */
    private UnaryOperator<String> withSchemaSalt(UnaryOperator<String> baseJobId) {
        String schemaSalt =
                CommitPlanner.schemaSalt(
                        Preconditions.checkNotNull(
                                reconciledSchema,
                                "%s is prepared before its schema is reconciled",
                                destination));
        return jobId -> CommitPlanner.schemaJobId(baseJobId.apply(jobId), schemaSalt);
    }

    /**
     * Prepares the temporary tables under the names {@code renameTemporary} gives them, with {@code
     * tableLayout}, has every remaining load fill its prepared table under {@code baseJobId} of its
     * planned id, naming the table's incarnation, and leaves this plan's hierarchy without the
     * copies into the tables it leaves out.
     *
     * <p>The tables copies fill are prepared first, from those the last copy reads down, and the
     * leaves last. Once such a table is prepared, the copy that fills it, under the id the copy
     * stage will give it, is waited for if an earlier attempt left it unfinished, before any table
     * it reads is prepared: that id names only the table it fills, so an unfinished copy could read
     * a source created anew empty here and then be re-attached to. When that copy succeeded, the
     * tables it read, and everything below them, are only extended where they survive: not created,
     * filled or copied from again, since the copy stage re-attaches to the copy that already holds
     * their rows.
     *
     * @param tempTables the service the tables are prepared through, and the copies waited for
     * @param renameTemporary renames a planned temporary table, or is the identity
     * @param tableLayout the layout each table is created with
     * @param loadLayout the layout each load states, or {@code null} for none
     * @param baseJobId derives a job's id, before its incarnation, from its planned id
     * @return the creation time of every prepared table
     */
    private Map<TableDestination, Long> prepareTablesAndStampLoads(
            TempTables tempTables,
            UnaryOperator<TableDestination> renameTemporary,
            TableLayout tableLayout,
            @Nullable TableLayout loadLayout,
            UnaryOperator<String> baseJobId)
            throws IOException {
        DestinationCopy hierarchy = Preconditions.checkNotNull(copy);
        Set<TableDestination> unneeded = new HashSet<>();
        int extended = 0;
        Map<TableDestination, PlannedCopy> fillingCopies = hierarchy.copiesIntoTemporaryTables();
        Map<TableDestination, Long> creationTimes = new HashMap<>();
        // The tables copies fill come first, from those the last copy reads down, and the leaves
        // last: a table's sources are prepared only once the copy that fills it, under the
        // incarnation it now has, is not pending or running. Such a copy left by an earlier
        // attempt may not have read its sources yet, and one of them created anew after it
        // expired would be copied empty, under an id this attempt then re-attaches to. A table is
        // created after its sources in the plan, so the reverse order reaches it first.
        List<TableDestination> filledByCopies = new ArrayList<>(hierarchy.cleanupTables);
        filledByCopies.retainAll(fillingCopies.keySet());
        Collections.reverse(filledByCopies);
        // A table whose copy already succeeded under the incarnation it now has holds that copy's
        // rows, which the copy stage re-attaches to, so the tables it read are not filled or
        // copied from again: refilling one that expired would need staged files a lifecycle rule
        // may have removed, for rows the table above already holds. One that survives is still
        // extended, so it keeps its rows as long as the table above for a later attempt that
        // finds that table expired; one that expired is not created anew, since an empty table
        // there could only be read by an earlier attempt's copy still running under another id.
        for (TableDestination table : filledByCopies) {
            PlannedCopy filling = fillingCopies.get(table);
            TableDestination prepared = renameTemporary.apply(table);
            if (unneeded.contains(table)) {
                extended += tempTables.extend(prepared) ? 1 : 0;
                unneeded.addAll(filling.spec.getSourceTables());
                continue;
            }
            creationTimes.put(prepared, tempTables.prepare(prepared, tableLayout));
            if (tempTables.copySucceeded(
                    DestinationCopy.stamped(filling, renameTemporary, baseJobId, creationTimes))) {
                unneeded.addAll(filling.spec.getSourceTables());
            }
        }
        for (TableDestination table : hierarchy.cleanupTables) {
            if (fillingCopies.containsKey(table)) {
                continue;
            }
            TableDestination prepared = renameTemporary.apply(table);
            if (unneeded.contains(table)) {
                extended += tempTables.extend(prepared) ? 1 : 0;
            } else {
                creationTimes.put(prepared, tempTables.prepare(prepared, tableLayout));
            }
        }
        if (!unneeded.isEmpty()) {
            LOG.info(
                    "{} of the temporary tables of {} feed a copy that already succeeded; not"
                            + " filling them again, and extending the {} that survive",
                    unneeded.size(),
                    destination,
                    extended);
        }
        List<PlannedLoad> preparedLoads = new ArrayList<>(loads.size());
        for (PlannedLoad load : loads) {
            if (unneeded.contains(load.jobDestination)) {
                continue;
            }
            // Every load fills a temporary table; a direct load into the destination is refused.
            TableDestination table = renameTemporary.apply(load.jobDestination);
            preparedLoads.add(
                    load.intoPreparedTable(
                            table,
                            CommitPlanner.incarnationJobId(
                                    baseJobId.apply(load.jobId), creationTimes, table),
                            loadLayout));
        }
        loads = preparedLoads;
        copy = hierarchy.withoutCopiesInto(unneeded);
        return creationTimes;
    }

    private void logFinalCopySucceeded() {
        LOG.info(
                "The final copy into {} already succeeded; re-attaching to it without loading or"
                        + " copying into its temporary tables again",
                destination);
    }
}
