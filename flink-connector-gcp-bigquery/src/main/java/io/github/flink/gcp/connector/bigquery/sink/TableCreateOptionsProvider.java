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

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * Supplies per-destination {@link TableCreateOptions} for tables created under {@link
 * CreateDisposition#CREATE_IF_NEEDED}.
 *
 * <p>This is the creation-metadata hook referenced by {@link TableDestination}: destinations stay
 * pure table identity, while partitioning and clustering are resolved here. It is invoked only when
 * the sink finds a destination missing and {@link CreateDisposition#CREATE_IF_NEEDED} permits
 * creation, not for an existing table or per record.
 */
@PublicEvolving
@FunctionalInterface
public interface TableCreateOptionsProvider extends Serializable {

    /**
     * Returns the creation options for the given destination.
     *
     * @param destination the destination table to create
     * @return the creation options, never {@code null}
     */
    TableCreateOptions optionsFor(TableDestination destination);
}
