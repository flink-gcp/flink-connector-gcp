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

package io.github.flink.gcp.connector.bigtable.source;

import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableEmulatorITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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

        // Deliberately loose: the emulator picks its extra boundaries at random, so the only
        // assertion that is not flaky is that it answers with something and with far less than a
        // boundary per row. A service answer would be one boundary per tablet.
        assertThat(samples).isNotEmpty();
        assertThat(samples.size()).isLessThan(keys.length);
    }

    @Test
    void samplingAnEmptyTableReturnsNothingAtAll() {
        // Measured against this suite's pinned image on 2026-08-09: no samples at all, not the
        // single end-of-table marker the emulator's own upstream source now describes. Both answers
        // have to plan to one split covering the configured range, which is why the planner treats
        // "no boundaries" and "only the end-of-table marker" the same way.
        TableDestination table = createTable("deviation-sample-empty");

        List<KeyOffset> samples = sampleRowKeys(table);

        assertThat(samples).isEmpty();
    }

    @Test
    void samplingAlwaysReportsTheLastRowKeyOfTheTable() {
        TableDestination table = createTable("deviation-sample-last");
        seedRows(table, "a", "b", "c");

        List<KeyOffset> samples = sampleRowKeys(table);

        // The emulator's one deterministic boundary. The last response is the end-of-table marker
        // when the table is non-empty, so the last *key* answered is the table's own last key.
        assertThat(samples).isNotEmpty();
        assertThat(samples.get(samples.size() - 1).getKey())
                .isIn(ByteString.copyFromUtf8("c"), ByteString.EMPTY);
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
    @Disabled(
            "Real Bigtable returns one boundary per tablet, so a pre-split table samples"
                    + " deterministically. Enable when the emulator models tablets; until then the"
                    + " gated real-GCP suite is the only coverage of multi-split planning.")
    void samplesOneBoundaryPerTabletAsBigtableDoes() {
        throw new UnsupportedOperationException("Recorded for the service, not run here.");
    }

    @Test
    @Disabled(
            "Real Bigtable rejects an empty row key. The emulator accepts one, which is why the"
                    + " range algebra expresses progress past an empty key rather than assuming it"
                    + " cannot occur.")
    void rejectsAnEmptyRowKeyAsBigtableDoes() {
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
