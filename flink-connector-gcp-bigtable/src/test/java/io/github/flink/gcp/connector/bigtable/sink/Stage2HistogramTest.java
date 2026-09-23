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

package io.github.flink.gcp.connector.bigtable.sink;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class Stage2HistogramTest {
    @Test
    void quantilesAreBinUpperEdgesThatNeverUnderstateTheValue() {
        Stage2Histogram histogram = new Stage2Histogram();
        for (int i = 0; i < 90; i++) {
            histogram.record(7_000_000);
        }
        for (int i = 0; i < 10; i++) {
            histogram.record(27_000_000);
        }
        assertThat(histogram.count()).isEqualTo(100);
        assertThat(histogram.quantileUpperBound(.50)).isBetween(7_000_000L, 7_110_000L);
        assertThat(histogram.quantileUpperBound(.95)).isBetween(27_000_000L, 27_430_000L);
        assertThat(histogram.json())
                .contains("\"count\":100")
                .contains("\"sumNanos\":" + (90L * 7_000_000 + 10L * 27_000_000))
                .contains("\"maxNanos\":27000000")
                .contains("\"p50UpperNanos\":" + histogram.quantileUpperBound(.50))
                .contains("\"p95UpperNanos\":" + histogram.quantileUpperBound(.95))
                .contains("\"p99UpperNanos\":" + histogram.quantileUpperBound(.99));
        assertThat(histogram.quantileUpperBound(.50)).isLessThan(histogram.quantileUpperBound(.95));
    }

    @Test
    void jsonReportsEachQuantileFromItsOwnBin() {
        Stage2Histogram histogram = new Stage2Histogram();
        for (int i = 0; i < 90; i++) {
            histogram.record(7_000_000);
        }
        for (int i = 0; i < 6; i++) {
            histogram.record(27_000_000);
        }
        for (int i = 0; i < 4; i++) {
            histogram.record(100_000_000);
        }
        long p50 = histogram.quantileUpperBound(.50);
        long p95 = histogram.quantileUpperBound(.95);
        long p99 = histogram.quantileUpperBound(.99);
        assertThat(p50).isLessThan(p95);
        assertThat(p95).isLessThan(p99);
        assertThat(histogram.json())
                .contains("\"p50UpperNanos\":" + p50 + ",")
                .contains("\"p95UpperNanos\":" + p95 + ",")
                .contains("\"p99UpperNanos\":" + p99 + "}");
    }

    @Test
    void emptyAndNonPositiveValuesAreDefined() {
        Stage2Histogram histogram = new Stage2Histogram();
        assertThat(histogram.quantileUpperBound(.5)).isZero();
        histogram.record(0);
        assertThat(histogram.count()).isEqualTo(1);
        assertThat(histogram.quantileUpperBound(.5)).isEqualTo(2);
    }
}
