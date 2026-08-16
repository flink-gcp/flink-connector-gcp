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

package io.github.flink.gcp.connector.base.metrics;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import com.google.api.gax.rpc.StatusCode;

import javax.annotation.Nullable;

import java.util.EnumMap;
import java.util.Map;

/**
 * Counts a sink's failures by the status code that caused them, as {@code errorClass.CODE.errors}
 * on the writer's metric group.
 *
 * <p>{@code CODE} is a gax {@link StatusCode.Code} name, or {@value #UNCLASSIFIED} for a failure
 * carrying no status the client libraries expose — a serialization error, or an exception chain
 * {@code StatusCodes.codeOf} finds nothing in. The set is therefore bounded by the enum plus one,
 * which is what makes an unconditional per-code subgroup safe where a per-destination one is not
 * (Flink cannot unregister a metric).
 *
 * <p>Child counters are created on first use rather than up front: registering all ~17 codes for
 * every writer subtask would put a permanent row in the reporter for statuses the job never sees.
 *
 * <p><b>Which throwable in a chain a failure is classified by is the caller's decision</b>, exactly
 * as {@code StatusCodes.codeOf} leaves cause-chain traversal to the call site — the connectors
 * disagree on it deliberately (Pub/Sub matches any element, Cloud Tasks takes the first
 * classifiable one).
 *
 * <p><b>Task thread only.</b> The counters are plain {@code SimpleCounter}s taken from the metric
 * group, because every sink increment site in this repository runs on the task thread. A connector
 * that ever counts from an SDK callback thread needs a thread-safe counter and must not reuse this
 * type as it stands.
 */
@Internal
public final class ErrorClassCounters {

    /** Group name carrying the status code, so the code is a metric variable and not a name. */
    public static final String ERROR_CLASS_GROUP = "errorClass";

    /** Counter name inside the per-code group. */
    public static final String ERRORS = "errors";

    /** The {@code CODE} used when the failure carries no gax status code at all. */
    public static final String UNCLASSIFIED = "UNCLASSIFIED";

    private final MetricGroup metricGroup;
    private final Map<StatusCode.Code, Counter> byCode = new EnumMap<>(StatusCode.Code.class);

    @Nullable private Counter unclassified;

    /**
     * Creates the counters against the group they register on.
     *
     * @param metricGroup the sink writer's metric group
     */
    public ErrorClassCounters(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
    }

    /**
     * Counts one failure classified by {@code code}, or one unclassified failure when {@code code}
     * is {@code null}.
     *
     * @param code the status code the caller classified the failure by, or {@code null}
     */
    public void count(@Nullable StatusCode.Code code) {
        if (code == null) {
            if (unclassified == null) {
                unclassified = counterFor(UNCLASSIFIED);
            }
            unclassified.inc();
            return;
        }
        byCode.computeIfAbsent(code, c -> counterFor(c.name())).inc();
    }

    private Counter counterFor(String errorClass) {
        return metricGroup.addGroup(ERROR_CLASS_GROUP, errorClass).counter(ERRORS);
    }
}
