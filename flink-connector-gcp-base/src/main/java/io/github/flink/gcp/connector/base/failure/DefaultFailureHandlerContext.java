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

package io.github.flink.gcp.connector.base.failure;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Preconditions;

/** The {@link FailureHandlerContext} the sinks build from their {@link WriterInitContext}. */
@Internal
public final class DefaultFailureHandlerContext implements FailureHandlerContext {

    private final int subtaskIndex;
    private final MetricGroup metricGroup;

    /**
     * Creates a context.
     *
     * @param subtaskIndex the sink writer's subtask index
     * @param metricGroup the sink writer's metric group
     */
    public DefaultFailureHandlerContext(int subtaskIndex, MetricGroup metricGroup) {
        this.subtaskIndex = subtaskIndex;
        this.metricGroup = Preconditions.checkNotNull(metricGroup, "metricGroup must not be null");
    }

    /**
     * Creates the context a sink passes to {@link FailureHandler#open} when creating its writer.
     *
     * @param context the writer init context
     * @return the failure-handler context
     */
    public static DefaultFailureHandlerContext of(WriterInitContext context) {
        return new DefaultFailureHandlerContext(
                context.getTaskInfo().getIndexOfThisSubtask(), context.metricGroup());
    }

    @Override
    public int getSubtaskIndex() {
        return subtaskIndex;
    }

    @Override
    public MetricGroup getMetricGroup() {
        return metricGroup;
    }
}
