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

import java.io.IOException;
import java.io.Serializable;

/**
 * Mints the {@link ChangeStreamCoordinatorClient} one enumerator coordinates through.
 *
 * <p>Serializable because the source carries it there; the client it mints is not, which is the
 * whole reason for the indirection. The JobManager holds one source object for a job's whole life,
 * so a client on the source configuration would be shared by every enumerator a coordinator reset
 * builds ({@code docs/adr/0128}). This source already minted per enumerator on the path where no
 * client was configured; the factory is what removes the other path rather than leaving it to a
 * caller to avoid.
 *
 * <p>An implementation holds only values that may travel in a job graph. It opens no client: the
 * one it answers with builds its three client families on first use, so a restore that adopts a
 * checkpointed ledger mints a client and opens none of them. It may still <em>read</em> something
 * to decide what to mint — the shipped implementation loads the service-account key here, because
 * this coordinator's three client families share one provider — which is what the declared {@code
 * IOException} is for. It must not take a resource it then leaks on a throw, because the caller has
 * nothing to release yet.
 */
@Internal
public interface ChangeStreamCoordinatorClientFactory extends Serializable {

    /**
     * Mints one coordinator client.
     *
     * @return the client, owned by the caller, which closes it exactly once
     * @throws IOException if the client cannot be created
     */
    ChangeStreamCoordinatorClient create() throws IOException;
}
