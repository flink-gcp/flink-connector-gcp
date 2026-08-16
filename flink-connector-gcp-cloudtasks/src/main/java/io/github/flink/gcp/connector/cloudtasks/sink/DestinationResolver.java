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

package io.github.flink.gcp.connector.cloudtasks.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.Serializable;

/**
 * Resolves the destination queue for each record, enabling one sink instance to write to many
 * queues (dynamic destinations).
 *
 * <p>Unlike the Pub/Sub and BigQuery sinks' dynamic destinations this costs the sink nothing: Cloud
 * Tasks has no per-destination connection or stream, so one client serves every queue. Sharding
 * across queues is also the way to raise aggregate throughput past a single queue's limits.
 *
 * <p>The resolver is invoked once per record on the hot write path. Implementations must be
 * deterministic and cheap; when destinations repeat, return cached {@link QueueDestination}
 * instances instead of re-creating them per record (for example via a small {@code
 * Map#computeIfAbsent} keyed on the varying component).
 *
 * <p>The {@link SinkWriter.Context} exposes the record's event timestamp for time-based routing.
 *
 * @param <T> type of the records written by the sink
 */
@PublicEvolving
@FunctionalInterface
public interface DestinationResolver<T> extends Serializable {

    /**
     * Returns the destination queue for the given record.
     *
     * @param element the record
     * @param context writer context exposing the record's event timestamp and current watermark
     * @return the destination queue; never {@code null}
     */
    QueueDestination resolve(T element, SinkWriter.Context context);
}
