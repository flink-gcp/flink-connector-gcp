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
     * No ordering guarantee (default), tuned for throughput. A subscription may be consumed by
     * several reader subtasks concurrently — the split plan opens one subscriber client per subtask
     * even when that means several on the same subscription — and Pub/Sub balances messages across
     * them. Nothing waits on anything, and an ordering key may move between subtasks whenever
     * streaming-pull affinity shifts.
     */
    NONE,

    /**
     * Preserves per-ordering-key delivery order within each subscription. Each subscription is
     * assigned to exactly one reader subtask and its subscriber uses a single streaming-pull
     * connection, so every message for a key is emitted by one subtask in delivery order.
     *
     * <p>Ordering costs throughput, and most of that cost is Pub/Sub's rather than this source's:
     * ordered delivery raises end-to-end latency, publish throughput is capped at 1 MB/s per
     * ordering key, only one batch may be outstanding per key at a time, and unacknowledged
     * messages for one key can delay delivery for other keys. Prefer the most granular ordering
     * keys the data allows.
     *
     * <p>This source adds two costs of its own: parallelism is effectively capped at the number of
     * subscriptions (surplus subtasks receive no splits), and because acknowledgement waits for a
     * checkpoint while only one batch may be outstanding per key, per-key throughput is bounded by
     * roughly one batch per checkpoint interval.
     *
     * <p>Order is preserved <em>up to the source's output</em>. Preserving it further requires
     * partitioning the stream by the ordering key, for example {@code keyBy(orderingKey)}.
     */
    PER_KEY
}
