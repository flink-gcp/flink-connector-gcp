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

package io.github.flink.gcp.connector.bigquery.sink;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.Serializable;

/**
 * Resolves the destination table for each record, enabling one sink instance to write to many
 * tables (dynamic destinations).
 *
 * <p>The resolver is invoked once per record on the hot write path. Implementations must be
 * deterministic and cheap; when destinations repeat, return cached {@link TableDestination}
 * instances instead of re-creating them per record (for example via a small {@code
 * Map#computeIfAbsent} keyed on the varying component).
 *
 * <p>The {@link SinkWriter.Context} exposes the record's event timestamp for time-based routing
 * (for example daily tables).
 *
 * <p>Return {@link UnroutableRecord} only for a deterministic, record-specific routing failure that
 * the configured failure policy may safely fail, drop, or dead-letter. Returning {@code null} or
 * throwing an unexpected exception is a resolver bug or configuration failure and always fails the
 * write.
 *
 * @param <T> type of the records written by the sink
 */
@Public
@FunctionalInterface
public interface DestinationResolver<T> extends Serializable {

    /**
     * Resolves the destination for the given record.
     *
     * @param element the record
     * @param context writer context exposing the record's event timestamp and current watermark
     * @return a destination table, or an explicit record-specific routing failure; never {@code
     *     null}
     */
    DestinationResolution resolve(T element, SinkWriter.Context context);
}
