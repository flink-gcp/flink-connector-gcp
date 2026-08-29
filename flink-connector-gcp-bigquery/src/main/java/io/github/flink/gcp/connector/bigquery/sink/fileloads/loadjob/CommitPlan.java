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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob;

import org.apache.flink.annotation.Internal;

import java.util.List;

/**
 * Every job and cleanup target for one commit, validated before execution: what {@link
 * CommitPlanner} produces and {@link LoadJobOrchestrator} then performs.
 *
 * <p><b>The fields are package-private and read directly</b>, here and on the destination and
 * planned-job types beside it, with no accessors and no defensive copies. Both would be the usual
 * choice for value types and both are deliberately absent: {@code DestinationCommitPlan.loads}
 * reaches 100,000 elements by design, so copying on each read is a cost the plan cannot carry, and
 * accessors over types only this package can name would be ceremony. The destination plan's
 * reconciled schema is the deliberate mutable hand-off between execution phases; the executor's
 * completed-task boundary publishes it before the next phase. Nothing outside {@code loadjob} can
 * see any of it.
 */
@Internal
final class CommitPlan {

    final List<DestinationCommitPlan> destinations;

    CommitPlan(List<DestinationCommitPlan> destinations) {
        this.destinations = destinations;
    }
}
