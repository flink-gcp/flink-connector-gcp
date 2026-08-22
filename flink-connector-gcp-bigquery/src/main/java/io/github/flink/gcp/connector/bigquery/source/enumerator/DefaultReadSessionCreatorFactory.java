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

package io.github.flink.gcp.connector.bigquery.source.enumerator;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;

import javax.annotation.Nullable;

/**
 * Mints {@link ReadClientSessionCreator}s.
 *
 * <p>A named class rather than a lambda, because it travels in the job graph and {@code
 * docs/adr/0125} keeps connector-minted serializable lambdas out of it.
 *
 * <p>It holds exactly what the creator's own constructor takes, so what a job graph carries is
 * unchanged by the indirection — including the key-file path, which this seam reads for itself
 * rather than being handed a loaded credential.
 */
@Internal
public final class DefaultReadSessionCreatorFactory implements ReadSessionCreatorFactory {

    private static final long serialVersionUID = 1L;

    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;

    /**
     * Creates the factory.
     *
     * @param serviceAccountKeyFile the service-account key-file path, or {@code null} for ADC
     * @param emulatorEndpoint the emulator's gRPC endpoint, or {@code null} for BigQuery itself
     */
    public DefaultReadSessionCreatorFactory(
            @Nullable String serviceAccountKeyFile, @Nullable EmulatorEndpoint emulatorEndpoint) {
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.emulatorEndpoint = emulatorEndpoint;
    }

    @Override
    public ReadSessionCreator create() {
        return new ReadClientSessionCreator(serviceAccountKeyFile, emulatorEndpoint);
    }
}
