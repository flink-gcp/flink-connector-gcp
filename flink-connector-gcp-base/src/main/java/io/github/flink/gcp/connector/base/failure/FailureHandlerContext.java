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

package io.github.flink.gcp.connector.base.failure;

import org.apache.flink.annotation.Public;
import org.apache.flink.metrics.MetricGroup;

/**
 * What a {@link FailureHandler} or {@link DeadLetterQueue} learns about its surroundings when it is
 * opened: enough to stamp per-subtask output and to register its own metrics, and nothing more.
 */
@Public
public interface FailureHandlerContext {

    /** Returns the index of the sink writer subtask driving the handler. */
    int getSubtaskIndex();

    /** Returns the sink writer's metric group, for handler-registered counters and gauges. */
    MetricGroup getMetricGroup();
}
