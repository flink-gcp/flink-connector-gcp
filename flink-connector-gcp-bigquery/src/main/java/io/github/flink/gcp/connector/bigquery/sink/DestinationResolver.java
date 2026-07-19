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

package io.github.flink.gcp.connector.bigquery.sink;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * Resolves the destination table for each record, enabling one sink instance to write to many
 * tables (dynamic destinations).
 *
 * <p>Implementations must be deterministic and cheap: the resolver is invoked once per record on
 * the hot write path.
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
     * @return the destination table
     */
    TableDestination resolve(T element);
}
