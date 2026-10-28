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

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.metrics.ClientMetrics;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * The client metrics of a source's enumerators: registered on the first enumerator's metric group
 * and shared by every enumerator the same source object builds after it.
 *
 * <p>Flink keeps one enumerator metric group per source for the job's life. Under the default
 * scheduler, a coordinator reset after a global failover builds the next enumerator from the same
 * source object on that same group, and a second registration of a name there is dropped with a
 * "Name collision" warning while the first stays reported (Flink 2.2.1, {@code
 * SourceCoordinatorContext#metricGroup} and {@code AbstractMetricGroup#addMetric}). Client metrics
 * registered per enumerator would therefore keep reporting the first enumerator's totals, which
 * nothing records into any more. Shared, they keep counting across resets, as a counter should.
 * Bigtable's {@code EnumeratorClientMetrics} is the same holder over its own adapter.
 *
 * <p>The adaptive scheduler deserializes a new source object at each restart, onto the same group,
 * so there this holder registers again and the series freeze after the first restart, as every
 * other enumerator metric does (issue #1908).
 *
 * <p>They are never closed: the source object and its group outlive every enumerator, and a client
 * an enumerator is still closing may record into them, which is no error.
 */
@Internal
public final class SpannerEnumeratorClientMetrics implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String[] methods;

    /** Created on the JobManager by the first enumerator; never serialized. */
    @Nullable private transient ClientMetrics clientMetrics;

    /**
     * Creates the holder.
     *
     * @param methods the RPC methods the enumerators' clients call, without the {@code Spanner.}
     *     prefix
     */
    public SpannerEnumeratorClientMetrics(String... methods) {
        this.methods = Preconditions.checkNotNull(methods, "methods").clone();
    }

    /**
     * Returns the shared client metrics, registering them on the given group the first time.
     *
     * @param group the enumerator's metric group, or {@code null} where the context offers none
     * @return the client metrics, or {@code null} if none were registered and no group is given
     */
    @Nullable
    public synchronized ClientMetrics get(@Nullable MetricGroup group) {
        if (clientMetrics == null && group != null) {
            clientMetrics = SpannerClientMetrics.register(group, methods);
        }
        return clientMetrics;
    }
}
