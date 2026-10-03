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

package io.github.flink.gcp.connector.datastore;

import com.google.cloud.datastore.DatastoreOptions;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The source's lazily built client: its settings and its one-way close. */
class LazyDatastoreClientTest {

    private static final DatabaseDestination DATABASE = DatabaseDestination.of("p");

    @Test
    void buildsItsClientWithTheLibrarysRetries() {
        // The source's reads are idempotent at a fixed read time; a call timeout here would turn
        // every page into a single attempt.
        LazyDatastoreClient client =
                new LazyDatastoreClient("test", EmulatorEndpoint.parse("localhost:1", "e"));

        assertThat(client.settings(DATABASE).getRetrySettings())
                .isEqualTo(
                        DatastoreOptions.newBuilder().setProjectId("p").build().getRetrySettings());
    }

    @Test
    void refusesUseAfterItIsClosed() throws IOException {
        LazyDatastoreClient client =
                new LazyDatastoreClient("page reader", EmulatorEndpoint.parse("localhost:1", "e"));

        client.close();

        assertThatThrownBy(() -> client.rpc(DATABASE))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("page reader")
                .hasMessageContaining("closed before use");
        assertThatThrownBy(() -> client.useCredentials(null))
                .isInstanceOf(IllegalStateException.class);
    }
}
