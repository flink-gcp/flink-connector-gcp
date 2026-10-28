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

package io.github.flink.gcp.connector.spanner;

import org.apache.flink.metrics.Counter;

import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;

/**
 * Tells whether a {@link ClientMetrics} still accepts recordings, which is what its close changes:
 * a closed one keeps its values and drops what it is given.
 */
public final class ClientMetricsProbe {

    private static final String INSTRUMENT = "probe";
    private static final String METHOD = "Spanner.Probe";

    private ClientMetricsProbe() {}

    /** Creates client metrics keyed by the method attribute Spanner's client records. */
    public static ClientMetrics create() {
        return ClientMetrics.create("method");
    }

    /**
     * Records one value through the client metrics' own OpenTelemetry and reports whether it
     * arrived.
     *
     * @param metrics the client metrics to probe
     * @return {@code true} if they are open
     */
    public static boolean isOpen(ClientMetrics metrics) {
        Counter counter = metrics.counterSum(INSTRUMENT, METHOD);
        long before = counter.getCount();
        metrics.openTelemetry()
                .getMeter(INSTRUMENT)
                .counterBuilder(INSTRUMENT)
                .build()
                .add(1, Attributes.of(AttributeKey.stringKey("method"), METHOD));
        return counter.getCount() == before + 1;
    }
}
