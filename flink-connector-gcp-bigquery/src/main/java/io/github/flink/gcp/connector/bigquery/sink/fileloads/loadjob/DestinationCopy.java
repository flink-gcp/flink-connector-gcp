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
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

/** One destination table's combined temporary-table hierarchy across all staging formats. */
@Internal
final class DestinationCopy {

    final List<List<PlannedCopy>> intermediateLevels;
    final PlannedCopy finalCopy;
    @Nullable final PlannedQuery terminalQuery;
    final List<TableDestination> cleanupTables;

    DestinationCopy(
            List<List<PlannedCopy>> intermediateLevels,
            PlannedCopy finalCopy,
            @Nullable PlannedQuery terminalQuery,
            List<TableDestination> cleanupTables) {
        this.intermediateLevels = intermediateLevels;
        this.finalCopy = finalCopy;
        this.terminalQuery = terminalQuery;
        this.cleanupTables = cleanupTables;
    }

    /**
     * Returns this hierarchy over the renamed temporary tables of a laid-out destination (see
     * {@link DestinationCommitPlan#layOutTempTables(TableLayout,
     * DestinationCommitPlan.TempTables)}). Every intermediate copy fills a renamed table under an
     * id salted with the layout and naming the incarnation of that table. The final copy, which
     * writes the destination, takes a fixed marker instead, so its id is the same whatever the
     * layout, and names its unsalted id as equivalent, under which it may have run before the
     * destination needed a layout.
     *
     * @param rename renames a temporary table of this hierarchy, and leaves the destination
     * @param salt the layout's salt
     * @param creationTimes the creation time of every renamed temporary table
     */
    DestinationCopy laidOut(
            UnaryOperator<TableDestination> rename,
            String salt,
            Map<TableDestination, Long> creationTimes) {
        Preconditions.checkState(
                terminalQuery == null, "A terminal query's temporary tables are not laid out");
        List<List<PlannedCopy>> levels = new ArrayList<>(intermediateLevels.size());
        for (List<PlannedCopy> level : intermediateLevels) {
            List<PlannedCopy> copies = new ArrayList<>(level.size());
            for (PlannedCopy copy : level) {
                copies.add(
                        copy.laidOut(
                                rename,
                                CommitPlanner.laidOutIncarnationJobId(
                                        copy.jobId,
                                        salt,
                                        creationTimes,
                                        rename.apply(copy.spec.getDestination())),
                                null));
            }
            levels.add(copies);
        }
        return new DestinationCopy(levels, laidOutFinalCopy(rename), null, renamed(rename));
    }

    private List<TableDestination> renamed(UnaryOperator<TableDestination> rename) {
        List<TableDestination> tables = new ArrayList<>(cleanupTables.size());
        for (TableDestination table : cleanupTables) {
            tables.add(rename.apply(table));
        }
        return tables;
    }

    /**
     * The final copy of {@link #laidOut(UnaryOperator, String, Map)}, which reads the renamed
     * tables into the destination.
     */
    PlannedCopy laidOutFinalCopy(UnaryOperator<TableDestination> rename) {
        return finalCopy.laidOut(
                rename, CommitPlanner.laidOutFinalCopyId(finalCopy.jobId), finalCopy.jobId);
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

    long jobCount() {
        long count = 1;
        for (List<PlannedCopy> level : intermediateLevels) {
            count += level.size();
        }
        return count;
    }
}
