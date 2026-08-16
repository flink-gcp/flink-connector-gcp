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

import java.io.Serializable;

/**
 * Supplies the desired CDC table contract for each destination.
 *
 * <p>The default-stream CDC path invokes it when a destination becomes active so the table can be
 * created, verified, or reconciled before the appender opens. It is not invoked per record while
 * that destination stays active.
 */
@FunctionalInterface
@Public
public interface CdcTableOptionsProvider extends Serializable {

    /**
     * Returns the CDC table options for the destination.
     *
     * @param destination the destination table to create, verify, or reconcile
     * @return the desired CDC table contract, never {@code null}
     */
    CdcTableOptions optionsFor(TableDestination destination);
}
