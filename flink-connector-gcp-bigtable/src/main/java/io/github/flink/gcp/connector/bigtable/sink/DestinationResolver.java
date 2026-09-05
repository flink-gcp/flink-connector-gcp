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

package io.github.flink.gcp.connector.bigtable.sink;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.connector.sink2.SinkWriter;

import io.github.flink.gcp.connector.bigtable.TableDestination;

import javax.annotation.Nullable;

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
 * <p>In the bulk sink, each distinct table costs the writer a mutation batcher of its own, held
 * until the destination goes idle; {@code BigtableWriterOptions.destinationIdleTimeout} bounds that
 * state. The conditional surfaces instead retain per-table request state and share a client per
 * instance, governed by {@code BigtableRequestOptions}. Resolver <em>cardinality</em> is a resource
 * decision on both surfaces.
 *
 * <p>Sinks supply a {@link SinkWriter.Context} exposing the record's event timestamp for time-based
 * routing. The conditional async helper supplies {@code null}; resolvers used there must obtain
 * routing information from the element itself.
 *
 * @param <T> type of the records written by the sink
 */
@Public
@FunctionalInterface
public interface DestinationResolver<T> extends Serializable {

    /**
     * Returns the destination table for the given record.
     *
     * @param element the record
     * @param context writer context exposing the record's event timestamp and current watermark, or
     *     {@code null} when invoked by the conditional async helper
     * @return the destination table; never {@code null}
     */
    TableDestination resolve(T element, @Nullable SinkWriter.Context context);
}
