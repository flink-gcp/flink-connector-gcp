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

package io.github.flink.gcp.connector.pubsub.source;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Whether the source preserves Pub/Sub ordering-key delivery order.
 *
 * <p>Ordered delivery is only meaningful for subscriptions created with {@code
 * enableMessageOrdering}; see the module README for the end-to-end guarantee and its cost.
 */
@PublicEvolving
public enum OrderingMode {

    /**
     * No ordering guarantee (default). A subscription may be consumed by several reader subtasks
     * concurrently, which maximizes throughput but lets Pub/Sub move an ordering key between
     * subtasks when streaming-pull affinity shifts.
     */
    NONE,

    /**
     * Preserves per-ordering-key delivery order within each subscription. Each subscription is
     * assigned to exactly one reader subtask and its subscriber uses a single streaming-pull
     * connection, so every message for a key is emitted by one subtask in delivery order.
     *
     * <p>Two consequences: source parallelism is effectively capped at the number of subscriptions
     * (surplus subtasks receive no splits), and because Pub/Sub keeps only one batch outstanding
     * per ordering key while this source defers acknowledgements to checkpoint completion, per-key
     * throughput is bounded by roughly one batch per checkpoint interval.
     *
     * <p>Order is preserved <em>up to the source's output</em>. Preserving it further requires
     * partitioning the stream by the ordering key, for example {@code keyBy(orderingKey)}.
     */
    PER_KEY
}
