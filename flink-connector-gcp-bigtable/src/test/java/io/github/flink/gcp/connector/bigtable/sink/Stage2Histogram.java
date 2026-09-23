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

/**
 * A nanosecond histogram with 64 bins per power of two, about 1.6% wide. A quantile is reported as
 * the upper edge of the bin that holds it, so it never understates the recorded value.
 */
final class Stage2Histogram {
    private final long[] bins = new long[4096];
    private long count;
    private long sum;
    private long max;

    synchronized void record(long nanos) {
        if (nanos <= 0) {
            nanos = 1;
        }
        int exponent = 63 - Long.numberOfLeadingZeros(nanos);
        long base = 1L << exponent;
        int fraction = (int) (((double) nanos / base - 1) * 64);
        bins[Math.min(4095, exponent * 64 + fraction)]++;
        count++;
        sum += nanos;
        max = Math.max(max, nanos);
    }

    synchronized long count() {
        return count;
    }

    synchronized long quantileUpperBound(double fraction) {
        long target = (long) Math.ceil(count * fraction);
        long seen = 0;
        for (int i = 0; i < bins.length; i++) {
            seen += bins[i];
            if (seen >= target && seen > 0) {
                return (long) Math.ceil(Math.scalb(1.0 + (i % 64 + 1) / 64.0, i / 64));
            }
        }
        return 0;
    }

    /** Count, sum and maximum are exact; the three quantiles are bin upper edges. */
    synchronized String json() {
        return "{\"count\":"
                + count
                + ",\"sumNanos\":"
                + sum
                + ",\"maxNanos\":"
                + max
                + ",\"p50UpperNanos\":"
                + quantileUpperBound(.50)
                + ",\"p95UpperNanos\":"
                + quantileUpperBound(.95)
                + ",\"p99UpperNanos\":"
                + quantileUpperBound(.99)
                + "}";
    }
}
