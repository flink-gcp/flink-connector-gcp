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

package io.github.flink.gcp.connector.testutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import javax.annotation.Nullable;

/**
 * A {@link SinkCommitterMetricGroup} whose metrics can be read back by the names they registered
 * under — {@link TestSinkCommitterMetricGroup} is to a committer what {@link
 * TestSinkWriterMetricGroup} is to a writer, and for the same reason: {@code
 * UnregisteredMetricsGroup.createSinkCommitterMetricGroup()} hands out a fresh {@code
 * SimpleCounter} per call, so a counter the committer registered is unreachable afterwards.
 *
 * <p>The framework's own committer counters are registered here under the names a reporter sees
 * ({@value #TOTAL_COMMITTABLES} and friends, which are <em>not</em> the accessor names on the
 * interface), so a test asserting a connector's custom counter cannot accidentally collide with one
 * of them. The registered metrics are read back through the inherited {@link
 * #counterValue(String...)}, {@link #gaugeValue(String...)} and {@link #hasMetric(String...)}
 * accessors.
 */
@Internal
public final class TestSinkCommitterMetricGroup extends ListenerReadableMetricGroup
        implements SinkCommitterMetricGroup {

    /** Name of the framework's arrived-committables counter, as a reporter sees it. */
    public static final String TOTAL_COMMITTABLES = "totalCommittables";

    /** Name of the framework's successful-committables counter. */
    public static final String SUCCESSFUL_COMMITTABLES = "successfulCommittables";

    /** Name of the framework's already-committed-committables counter. */
    public static final String ALREADY_COMMITTED_COMMITTABLES = "alreadyCommittedCommittables";

    /** Name of the framework's failed-committables counter. */
    public static final String FAILED_COMMITTABLES = "failedCommittables";

    /** Name of the framework's retried-committables counter. */
    public static final String RETRIED_COMMITTABLES = "retriedCommittables";

    private final Counter totalCommittables;
    private final Counter successfulCommittables;
    private final Counter alreadyCommittedCommittables;
    private final Counter failedCommittables;
    private final Counter retriedCommittables;

    @Nullable private Gauge<Integer> currentPendingCommittablesGauge;

    private TestSinkCommitterMetricGroup() {
        this.totalCommittables = counter(TOTAL_COMMITTABLES);
        this.successfulCommittables = counter(SUCCESSFUL_COMMITTABLES);
        this.alreadyCommittedCommittables = counter(ALREADY_COMMITTED_COMMITTABLES);
        this.failedCommittables = counter(FAILED_COMMITTABLES);
        this.retriedCommittables = counter(RETRIED_COMMITTABLES);
    }

    /** Creates a group over a fresh listener. */
    public static TestSinkCommitterMetricGroup create() {
        return new TestSinkCommitterMetricGroup();
    }

    /** The gauge a committer passed to {@link #setCurrentPendingCommittablesGauge}, or null. */
    @Nullable
    public Gauge<Integer> getCurrentPendingCommittablesGauge() {
        return currentPendingCommittablesGauge;
    }

    @Override
    public Counter getNumCommittablesTotalCounter() {
        return totalCommittables;
    }

    @Override
    public Counter getNumCommittablesFailureCounter() {
        return failedCommittables;
    }

    @Override
    public Counter getNumCommittablesRetryCounter() {
        return retriedCommittables;
    }

    @Override
    public Counter getNumCommittablesSuccessCounter() {
        return successfulCommittables;
    }

    @Override
    public Counter getNumCommittablesAlreadyCommittedCounter() {
        return alreadyCommittedCommittables;
    }

    @Override
    public void setCurrentPendingCommittablesGauge(Gauge<Integer> currentPendingCommittablesGauge) {
        // Captured rather than registered, as the writer harness captures currentSendTime: the
        // framework sets this one, and capturing is what lets a test say a committer did not.
        this.currentPendingCommittablesGauge = currentPendingCommittablesGauge;
    }

    @Override
    public OperatorIOMetricGroup getIOMetricGroup() {
        return UnregisteredMetricsGroup.createOperatorIOMetricGroup();
    }
}
