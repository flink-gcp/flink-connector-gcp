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

package io.github.flink.gcp.connector.spanner;

import org.apache.flink.annotation.Internal;

/** Shared boundary handling for Spanner metric values. */
@Internal
public final class SpannerMetricValues {

    /** Returns elapsed milliseconds, clamped for future values and saturated on overflow. */
    public static long elapsedMillis(long now, long earlier) {
        if (now <= earlier) {
            return 0;
        }
        try {
            return Math.subtractExact(now, earlier);
        } catch (ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
    }

    private SpannerMetricValues() {}
}
