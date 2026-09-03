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

package io.github.flink.gcp.connector.bigtable.source;

import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableEmulatorITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * What the <em>emulator</em> does on the read path, asserted so that an image bump has to declare a
 * change rather than quietly move the ground the source's emulator suite stands on.
 *
 * <p>Nothing here asserts what the connector does, and nothing here is evidence about Bigtable.
 * Where the two are known to differ, the service's behaviour sits beside the emulator's as a
 * {@code @Disabled} twin carrying its measurement — to be enabled if the emulator ever catches up.
 *
 * <p>The deviation that shapes this connector's testing is the first one: the emulator samples row
 * keys by returning the table's final key plus others with roughly one-in-a-hundred probability
 * (read out of the {@code bttest} emulator's source on 2026-08-09), so a plan built against it is
 * effectively one split whatever the table holds. That is why split planning is a unit test and a
 * gated real-GCP test, and never an emulator test.
 *
 * <p>Since {@code 583.0.0-emulators} every {@code SampleRowKeys} response also <em>trails</em> an
 * end-of-table marker — an empty key at the table's total offset — sent outside the row loop and so
 * unconditional. Measured 2026-09-03: a three-row table answers {@code ['c'@2, ''@3]} where {@code
 * 441.0.0-emulators} answered {@code ['c'@2]}, and an empty table answers {@code ['']} where it
 * answered nothing. The planner drops empty-key samples, so no plan changed; what changed is that
 * the last sample is now always the marker, which is why the assertions below pin the table's own
 * last key by name rather than by position.
 */
class BigtableEmulatorReadDeviationITCase extends AbstractBigtableEmulatorITCase {

    @Test
    void samplingAPopulatedTableReturnsFarFewerBoundariesThanItHasRows() {
        TableDestination table = createTable("deviation-sample");
        String[] keys = new String[200];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = String.format("row-%03d", i);
        }
        seedRows(table, keys);

        List<KeyOffset> samples = sampleRowKeys(table);

        // Deliberately loose about the *count*: the emulator picks its extra boundaries at random,
        // so the only non-flaky assertion is far less than a boundary per row. A service answer
        // would be one boundary per tablet. Not loose about emptiness, though — since
        // 583.0.0-emulators the trailing end-of-table marker makes isNotEmpty() unfailable, so the
        // real row boundaries are what gets pinned: the table's own last key must be among them.
        assertThat(samples).hasSizeLessThan(keys.length);
        assertThat(samples)
                .extracting(KeyOffset::getKey)
                .filteredOn(key -> !key.isEmpty())
                .contains(ByteString.copyFromUtf8(String.format("row-%03d", keys.length - 1)));
    }

    @Test
    void samplingAnEmptyTableReturnsOnlyTheEndOfTableMarker() {
        // Measured 2026-09-03 against 583.0.0-emulators: one sample, the end-of-table marker, an
        // empty key at offset 0. Under 441.0.0-emulators it was no samples at all, so the image
        // bump moved the emulator onto the answer its own upstream source already described. The
        // planner was already written to treat "no boundaries" and "only the end-of-table marker"
        // the same way — both have to plan to one split covering the configured range — which is
        // why this change moves the measurement here and nothing behind it.
        TableDestination table = createTable("deviation-sample-empty");

        List<KeyOffset> samples = sampleRowKeys(table);

        assertThat(samples).hasSize(1);
        assertThat(samples.get(0).getKey()).isEqualTo(ByteString.EMPTY);
        assertThat(samples.get(0).getOffsetBytes()).isZero();
    }

    @Test
    void samplingAlwaysReportsTheLastRowKeyOfTheTable() {
        TableDestination table = createTable("deviation-sample-last");
        seedRows(table, "a", "b", "c");

        List<KeyOffset> samples = sampleRowKeys(table);

        // The emulator's one deterministic boundary. Pinned by name rather than by position: since
        // 583.0.0-emulators the *last* response is always the end-of-table marker, so asserting on
        // samples.get(size - 1) would be satisfied by the marker alone and could no longer observe
        // the property this test is named for. Measured 2026-09-03: ['c'@2, ''@3].
        //
        // contains, not containsExactly: "a" and "b" are candidates for the random extra
        // boundaries this class documents, so pinning the filtered list exactly would fail a
        // correct run whenever one of them is picked. Containment is the whole deterministic
        // property there is, and it still fails if "c" stops being sampled.
        assertThat(samples)
                .extracting(KeyOffset::getKey)
                .filteredOn(key -> !key.isEmpty())
                .contains(ByteString.copyFromUtf8("c"));
        assertThat(samples.get(samples.size() - 1).getKey()).isEqualTo(ByteString.EMPTY);
    }

    @Test
    void aReadModifyWriteRowStillPlantsAnEmptyKeyThatBreaksTheReadStateMachine() {
        // The empty-key deviation narrowed with 583.0.0-emulators; it did not close. Measured
        // 2026-09-03 against the pinned image, all three write paths:
        //
        //   MutateRows (what the sink uses)  rejected, INTERNAL wrapping "Row keys must be
        //                                    non-empty" — BigtableEmulatorDeviationITCase pins it
        //   MutateRow  (the seeding helpers) rejected, INVALID_ARGUMENT, the service's own status
        //   ReadModifyWriteRow               ACCEPTED — this test
        //
        // Up to 441.0.0-emulators every one of the three accepted it. The row the last one stores
        // still breaks the client's own read state machine, a state real Bigtable cannot reach, so
        // this is where that measurement lives now — the write-path suite can no longer produce
        // it. That state is what the range algebra's inclusive-successor start is written for
        // (RowRanges#truncateStartOpen), so it stays asserted rather than merely remembered.
        TableDestination table = createTable("deviation-rmw-empty-row-key");

        appendCell(table, "", "q", "v");

        assertThat(catchThrowable(() -> readRows(table))).hasMessageContaining("rowKey missing");
    }

    @Test
    void readsRowsInAscendingKeyOrder() {
        // Three lines, and the entire resume design rests on it: a split is resumed at the last
        // key seen, which only means anything if the order is the key order.
        TableDestination table = createTable("deviation-order");
        seedRows(table, "c", "a", "b");

        assertThat(readRows(table))
                .extracting(row -> row.getKey().toStringUtf8())
                .containsExactly("a", "b", "c");
    }

    @Test
    void answersARangeExclusiveAtItsOwnEndKeyWithNoRows() {
        // The state a split's range reaches after its last row was emitted. The emulator answers
        // it empty, which is exactly the answer the service refuses to give — see the disabled
        // twin — and why neither refusal in #481 could have been found here.
        TableDestination table = createTable("deviation-inverted-range");
        seedRows(table, "a", "b");

        assertThat(
                        readRange(
                                table,
                                ByteStringRange.unbounded()
                                        .startOpen(ByteString.copyFromUtf8("b"))
                                        .endClosed(ByteString.copyFromUtf8("b"))))
                .isEmpty();
    }

    @Test
    void answersAFilterNamingAnAbsentColumnFamilyWithNoRows() {
        // The emulator evaluates the family filter against the rows and finds nothing, where the
        // service checks the family against the table's schema first — see the disabled twin.
        TableDestination table = createTable("deviation-absent-family-filter");
        seedRows(table, "a");

        assertThat(readRows(table, Filters.FILTERS.family().exactMatch("absent"))).isEmpty();
    }

    @Test
    @Disabled(
            "Real Bigtable returns one boundary per tablet, so a pre-split table samples"
                    + " deterministically. Enable when the emulator models tablets; until then the"
                    + " gated real-GCP suite is the only coverage of multi-split planning.")
    void samplesOneBoundaryPerTabletAsBigtableDoes() {
        throw new UnsupportedOperationException("Recorded for the service, not run here.");
    }

    @Test
    @Disabled(
            "Real Bigtable refuses a range whose start is exclusive at its own end key with"
                    + " INVALID_ARGUMENT (\"start_key must be less than end_key\", measured"
                    + " 2026-08-10, #481). The emulator answers it empty, so the reader's"
                    + " finish-without-a-stream short-circuit is verifiable only against the"
                    + " service.")
    void refusesARangeExclusiveAtItsOwnEndKeyAsBigtableDoes() {
        throw new UnsupportedOperationException("Recorded for the service, not run here.");
    }

    @Test
    @Disabled(
            "Real Bigtable refuses a read whose filter names a column family the table does not"
                    + " have with NOT_FOUND (\"Requested column family not found\", measured"
                    + " 2026-08-10, #481). The emulator answers it empty, so the loud failure a"
                    + " misconfigured filter earns is covered only by the gated real-GCP suite.")
    void refusesAFilterNamingAnAbsentColumnFamilyAsBigtableDoes() {
        throw new UnsupportedOperationException("Recorded for the service, not run here.");
    }

    @Test
    @Disabled(
            "Real Bigtable rejects an empty row key on every write path. The emulator rejects it"
                    + " on the mutate paths only, so the deviation narrowed rather than closed and"
                    + " this stays disabled. Enable when ReadModifyWriteRow rejects one too.")
    void rejectsAnEmptyRowKeyOnEveryWritePathAsBigtableDoes() {
        throw new UnsupportedOperationException("Recorded for the service, not run here.");
    }

    @Test
    @Disabled(
            "Real Bigtable honours the application profile a request names. The emulator ignores"
                    + " it entirely, which is why a configured appProfileId reaching the wire is"
                    + " covered only by the gated real-GCP suite.")
    void routesThroughTheApplicationProfileAsBigtableDoes() {
        throw new UnsupportedOperationException("Recorded for the service, not run here.");
    }
}
