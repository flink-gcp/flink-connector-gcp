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

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableLayout;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

/** One destination table's combined temporary-table hierarchy across all staging formats. */
@Internal
final class DestinationCopy {

    final List<List<PlannedCopy>> intermediateLevels;

    /**
     * The last copy: into the destination, or into the aggregate temporary table the terminal query
     * reads; {@code null} only in {@link #terminalQueryOnly()}, once that query already succeeded.
     */
    @Nullable final PlannedCopy finalCopy;

    @Nullable final PlannedQuery terminalQuery;
    final List<TableDestination> cleanupTables;

    DestinationCopy(
            List<List<PlannedCopy>> intermediateLevels,
            @Nullable PlannedCopy finalCopy,
            @Nullable PlannedQuery terminalQuery,
            List<TableDestination> cleanupTables) {
        this.intermediateLevels = intermediateLevels;
        this.finalCopy = finalCopy;
        this.terminalQuery = terminalQuery;
        this.cleanupTables = cleanupTables;
    }

    /**
     * Returns this hierarchy over the renamed temporary tables of a laid-out destination (see
     * {@link DestinationCommitPlan#prepareTempTables(TableLayout,
     * DestinationCommitPlan.TempTables)}). Every intermediate copy fills a renamed table under an
     * id salted with the layout and the schema and naming the incarnation of that table. The final
     * copy, which writes the destination, takes a fixed marker instead, so its id is the same
     * whatever the layout, and names its unsalted id as equivalent, under which it may have run
     * before the destination needed a layout.
     *
     * @param rename renames a temporary table of this hierarchy, and leaves the destination
     * @param baseJobId derives a filling copy's id, before its incarnation, from its planned id:
     *     the layout's and the schema's salts
     * @param creationTimes the creation time of every renamed temporary table
     */
    DestinationCopy laidOut(
            UnaryOperator<TableDestination> rename,
            UnaryOperator<String> baseJobId,
            Map<TableDestination, Long> creationTimes) {
        Preconditions.checkState(
                terminalQuery == null, "A terminal query's temporary tables are not laid out");
        return withPreparedLevels(rename, baseJobId, creationTimes, laidOutFinalCopy(rename));
    }

    /**
     * Returns this hierarchy over temporary tables that keep their names and were prepared ahead of
     * their jobs (see {@link DestinationCommitPlan#prepareTempTables(TableLayout,
     * DestinationCommitPlan.TempTables)}). Every copy that fills a temporary table, the copy into
     * the aggregate table of a terminal query included, takes the schema's salt, names the
     * incarnation of that table and creates nothing. A final copy into the destination and the
     * terminal query keep their ids.
     *
     * @param baseJobId derives a filling copy's id, before its incarnation, from its planned id:
     *     the schema's salt
     * @param creationTimes the creation time of every temporary table
     */
    DestinationCopy incarnated(
            UnaryOperator<String> baseJobId, Map<TableDestination, Long> creationTimes) {
        PlannedCopy lastCopy = Preconditions.checkNotNull(finalCopy);
        return withPreparedLevels(
                UnaryOperator.identity(),
                baseJobId,
                creationTimes,
                // The last copy fills the aggregate table a terminal query reads, and names its
                // incarnation like every other copy into a temporary table.
                terminalQuery == null
                        ? lastCopy
                        : stamped(lastCopy, UnaryOperator.identity(), baseJobId, creationTimes));
    }

    /**
     * Returns this hierarchy with every intermediate copy passed through {@link
     * #stamped(PlannedCopy, UnaryOperator, UnaryOperator, Map)}, ending in {@code lastCopy}, over
     * the tables {@code rename} gives.
     */
    private DestinationCopy withPreparedLevels(
            UnaryOperator<TableDestination> rename,
            UnaryOperator<String> baseJobId,
            Map<TableDestination, Long> creationTimes,
            PlannedCopy lastCopy) {
        List<List<PlannedCopy>> levels = new ArrayList<>(intermediateLevels.size());
        for (List<PlannedCopy> level : intermediateLevels) {
            List<PlannedCopy> copies = new ArrayList<>(level.size());
            for (PlannedCopy copy : level) {
                copies.add(stamped(copy, rename, baseJobId, creationTimes));
            }
            levels.add(copies);
        }
        return new DestinationCopy(levels, lastCopy, terminalQuery, renamed(rename));
    }

    /**
     * Returns {@code copy}, which fills a temporary table, over the tables {@code rename} gives,
     * under {@code baseJobId} of its planned id, naming the incarnation of the table it fills.
     */
    static PlannedCopy stamped(
            PlannedCopy copy,
            UnaryOperator<TableDestination> rename,
            UnaryOperator<String> baseJobId,
            Map<TableDestination, Long> creationTimes) {
        return copy.reissued(
                rename,
                CommitPlanner.incarnationJobId(
                        baseJobId.apply(copy.jobId),
                        creationTimes,
                        rename.apply(copy.spec.getDestination())),
                null);
    }

    /**
     * Returns this hierarchy without the intermediate copies into {@code tables}, which a copy that
     * already succeeded read, so they are not filled again.
     *
     * @param tables planned names of temporary tables
     */
    DestinationCopy withoutCopiesInto(Set<TableDestination> tables) {
        List<List<PlannedCopy>> levels = new ArrayList<>(intermediateLevels.size());
        for (List<PlannedCopy> level : intermediateLevels) {
            List<PlannedCopy> copies = new ArrayList<>(level.size());
            for (PlannedCopy copy : level) {
                if (!tables.contains(copy.spec.getDestination())) {
                    copies.add(copy);
                }
            }
            levels.add(copies);
        }
        return new DestinationCopy(levels, finalCopy, terminalQuery, cleanupTables);
    }

    /**
     * Returns every planned copy that fills a temporary table, by the table it fills: the
     * intermediate copies, and the copy into the aggregate table a terminal query reads.
     */
    Map<TableDestination, PlannedCopy> copiesIntoTemporaryTables() {
        Map<TableDestination, PlannedCopy> copies = new HashMap<>();
        for (List<PlannedCopy> level : intermediateLevels) {
            for (PlannedCopy copy : level) {
                copies.put(copy.spec.getDestination(), copy);
            }
        }
        if (terminalQuery != null) {
            PlannedCopy aggregateCopy = Preconditions.checkNotNull(finalCopy);
            copies.put(aggregateCopy.spec.getDestination(), aggregateCopy);
        }
        return copies;
    }

    private List<TableDestination> renamed(UnaryOperator<TableDestination> rename) {
        List<TableDestination> tables = new ArrayList<>(cleanupTables.size());
        for (TableDestination table : cleanupTables) {
            tables.add(rename.apply(table));
        }
        return tables;
    }

    /**
     * The final copy of {@link #laidOut(UnaryOperator, UnaryOperator, Map)}, which reads the
     * renamed tables into the destination.
     */
    PlannedCopy laidOutFinalCopy(UnaryOperator<TableDestination> rename) {
        PlannedCopy unchanged =
                Preconditions.checkNotNull(finalCopy, "The final copy was already dropped");
        return unchanged.reissued(
                rename, CommitPlanner.laidOutFinalCopyId(unchanged.jobId), unchanged.jobId);
    }

    /**
     * Returns only {@code reattachedCopy}, the final copy the caller re-attaches to, laid out or
     * not, for a destination whose final copy an earlier attempt already succeeded: nothing is
     * loaded or copied into a temporary table again. The tables, passed through {@code rename},
     * stay cleanup targets.
     *
     * @param reattachedCopy this hierarchy's final copy, or its laid-out form
     * @param rename the identity, or the renaming of a laid-out destination's temporary tables
     */
    DestinationCopy finalCopyOnly(
            PlannedCopy reattachedCopy, UnaryOperator<TableDestination> rename) {
        return new DestinationCopy(List.of(), reattachedCopy, null, renamed(rename));
    }

    /**
     * Returns only the terminal query, which the caller re-attaches to, for a {@code
     * WRITE_TRUNCATE_DATA} destination whose terminal query an earlier attempt already succeeded:
     * nothing is loaded or copied into a temporary table again, the aggregate table included. The
     * tables stay cleanup targets.
     */
    DestinationCopy terminalQueryOnly() {
        Preconditions.checkState(terminalQuery != null, "There is no terminal query");
        return new DestinationCopy(List.of(), null, terminalQuery, cleanupTables);
    }

    long jobCount() {
        long count = finalCopy == null ? 0 : 1;
        for (List<PlannedCopy> level : intermediateLevels) {
            count += level.size();
        }
        return count;
    }
}
