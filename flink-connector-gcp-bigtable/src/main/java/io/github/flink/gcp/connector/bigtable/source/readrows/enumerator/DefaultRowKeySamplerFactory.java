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

package io.github.flink.gcp.connector.bigtable.source.readrows.enumerator;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;

import javax.annotation.Nullable;

/**
 * Mints {@link DataClientRowKeySampler}s.
 *
 * <p>A named class rather than a lambda, because it travels in the job graph and {@code
 * docs/adr/0125} keeps connector-minted serializable lambdas out of it.
 *
 * <p>It holds exactly what the sampler's own constructor takes, so what a job graph carries is
 * unchanged by the indirection.
 */
@Internal
public final class DefaultRowKeySamplerFactory implements RowKeySamplerFactory {

    private static final long serialVersionUID = 1L;

    @Nullable private final String appProfileId;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;

    /**
     * Creates the factory.
     *
     * @param appProfileId the application profile to route through, or {@code null} for the
     *     instance's default
     * @param emulatorEndpoint the emulator endpoint (plaintext, no credentials), or {@code null}
     *     for production Bigtable
     */
    public DefaultRowKeySamplerFactory(
            @Nullable String appProfileId, @Nullable EmulatorEndpoint emulatorEndpoint) {
        this.appProfileId = appProfileId;
        this.emulatorEndpoint = emulatorEndpoint;
    }

    @Override
    public RowKeySampler create() {
        return new DataClientRowKeySampler(appProfileId, emulatorEndpoint);
    }
}
