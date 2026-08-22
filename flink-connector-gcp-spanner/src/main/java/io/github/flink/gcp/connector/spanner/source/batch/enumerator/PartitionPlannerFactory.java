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

package io.github.flink.gcp.connector.spanner.source.batch.enumerator;

import org.apache.flink.annotation.Internal;

import java.io.IOException;
import java.io.Serializable;

/**
 * Mints the {@link PartitionPlanner} one enumerator plans through.
 *
 * <p>Serializable because the source carries it there; the planner it mints is not, which is the
 * whole reason for the indirection. The JobManager holds one source object for a job's whole life,
 * so a planner on the source configuration would be shared by every enumerator a coordinator reset
 * builds and the first teardown would refuse every later one ({@code docs/adr/0128}).
 *
 * <p>An implementation holds only values that may travel in a job graph. It does not open a client:
 * the planner it answers with builds one on first use, so a restore whose checkpoint already
 * records a plan mints a planner and opens nothing.
 */
@Internal
public interface PartitionPlannerFactory extends Serializable {

    /**
     * Mints one planner.
     *
     * <p>Acquires nothing: an implementation that took a resource and then threw would leak it,
     * because the caller has nothing to release yet. The declared {@code IOException} is for an
     * implementation that must read something to decide what to mint, not for opening a client.
     *
     * @return the planner, owned by the caller, which closes it exactly once
     * @throws IOException if the planner cannot be created
     */
    PartitionPlanner create() throws IOException;
}
