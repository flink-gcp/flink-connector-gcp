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

package io.github.flink.gcp.connector.bigtable.source.changestream.enumerator;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.bigtable.BigtableCredentials;
import io.github.flink.gcp.connector.bigtable.TableDestination;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Mints {@link DefaultChangeStreamCoordinatorClient}s.
 *
 * <p>A named class rather than a lambda, because it travels in the job graph and {@code
 * docs/adr/0125} keeps connector-minted serializable lambdas out of it.
 *
 * <p>It carries the key-file path rather than a loaded provider, and loads once per {@link
 * #create()}: the coordinator owns three client families and one provider scopes all of them, which
 * is the boundary {@code docs/adr/0086} sets.
 */
@Internal
public final class DefaultChangeStreamCoordinatorClientFactory
        implements ChangeStreamCoordinatorClientFactory {

    private static final long serialVersionUID = 1L;

    private final TableDestination table;
    private final String appProfileId;
    @Nullable private final String serviceAccountKeyFile;

    /**
     * Creates the factory.
     *
     * @param table the table whose change stream is coordinated
     * @param appProfileId the single-cluster application profile to route through
     * @param serviceAccountKeyFile the service-account key-file path, or {@code null} for ADC
     */
    public DefaultChangeStreamCoordinatorClientFactory(
            TableDestination table, String appProfileId, @Nullable String serviceAccountKeyFile) {
        this.table = table;
        this.appProfileId = appProfileId;
        this.serviceAccountKeyFile = serviceAccountKeyFile;
    }

    @Override
    public ChangeStreamCoordinatorClient create() throws IOException {
        return new DefaultChangeStreamCoordinatorClient(
                table, appProfileId, BigtableCredentials.loadAll(serviceAccountKeyFile));
    }
}
