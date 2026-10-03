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

package io.github.flink.gcp.connector.datastore.source.batch.enumerator;

import org.apache.flink.annotation.Internal;

import java.io.IOException;
import java.io.Serializable;

/**
 * Mints the {@link QueryPlanner} one enumerator plans with.
 *
 * <p>Serializable because the source carries it; the planner it mints is not, which is the whole
 * reason for the indirection ({@code docs/adr/0128}). An implementation acquires nothing when it
 * mints: the planner builds its client on first use.
 */
@Internal
public interface QueryPlannerFactory extends Serializable {

    /**
     * Mints one planner.
     *
     * @return the planner, owned by the caller, which closes it exactly once
     * @throws IOException if the planner cannot be created
     */
    QueryPlanner create() throws IOException;
}
