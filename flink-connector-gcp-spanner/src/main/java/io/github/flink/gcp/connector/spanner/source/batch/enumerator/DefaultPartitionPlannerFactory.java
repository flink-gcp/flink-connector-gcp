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

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;

import javax.annotation.Nullable;

/**
 * Mints {@link BatchClientPartitionPlanner}s.
 *
 * <p>A named class rather than a lambda, because it travels in the job graph and {@code
 * docs/adr/0125} keeps connector-minted serializable lambdas out of it.
 *
 * <p>It holds exactly what the planner's own immutable fields hold, so what a job graph carries is
 * unchanged by the indirection: the database and, for a test or a local run, the emulator endpoint.
 */
@Internal
public final class DefaultPartitionPlannerFactory implements PartitionPlannerFactory {

    private static final long serialVersionUID = 1L;

    private final SpannerDatabase database;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;

    /**
     * Creates the factory.
     *
     * @param database the database to read
     * @param emulatorEndpoint the emulator endpoint (plaintext, no credentials), or {@code null}
     *     for the real service
     */
    public DefaultPartitionPlannerFactory(
            SpannerDatabase database, @Nullable EmulatorEndpoint emulatorEndpoint) {
        this.database = database;
        this.emulatorEndpoint = emulatorEndpoint;
    }

    @Override
    public PartitionPlanner create() {
        return new BatchClientPartitionPlanner(database, emulatorEndpoint);
    }
}
