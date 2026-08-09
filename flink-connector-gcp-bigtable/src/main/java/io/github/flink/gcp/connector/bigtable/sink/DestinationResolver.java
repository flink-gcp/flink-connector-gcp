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

package io.github.flink.gcp.connector.bigtable.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.SinkWriter;

import io.github.flink.gcp.connector.bigtable.TableDestination;

import java.io.Serializable;

/**
 * Resolves the destination table for each record, enabling one sink instance to write to many
 * tables (dynamic destinations).
 *
 * <p>The resolver is invoked once per record on the hot write path, and before the serializer runs
 * — a record the serializer then rejects is reported against the table it was headed for.
 * Implementations must be deterministic and cheap; when destinations repeat, return cached {@link
 * TableDestination} instances instead of re-creating them per record (for example via a small
 * {@code Map#computeIfAbsent} keyed on the varying component).
 *
 * <p>Each distinct table costs the writer a bulk mutation batcher of its own, held until the
 * destination goes idle, so a resolver's <em>cardinality</em> is a sink resource decision and not
 * only a routing one. {@code BigtableWriterOptions.destinationIdleTimeout} is what bounds it.
 *
 * <p>The {@link SinkWriter.Context} exposes the record's event timestamp for time-based routing.
 *
 * @param <T> type of the records written by the sink
 */
@PublicEvolving
@FunctionalInterface
public interface DestinationResolver<T> extends Serializable {

    /**
     * Returns the destination table for the given record.
     *
     * @param element the record
     * @param context writer context exposing the record's event timestamp and current watermark
     * @return the destination table; never {@code null}
     */
    TableDestination resolve(T element, SinkWriter.Context context);
}
