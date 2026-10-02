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

package io.github.flink.gcp.connector.firestore;

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The holder's one-way close. The endpoint is never connected to. */
class LazyFirestoreClientTest {

    private static LazyFirestoreClient holder() {
        return new LazyFirestoreClient("test seam", EmulatorEndpoint.parse("localhost:1", "e"));
    }

    @Test
    void buildsOneClientAndHandsItOutAgain() throws Exception {
        LazyFirestoreClient holder = holder();

        assertThat(holder.get(DatabaseDestination.of("p")))
                .isSameAs(holder.get(DatabaseDestination.of("p")));
        holder.close();
    }

    @Test
    void refusesUseAfterCloseRatherThanBuildingAClientNothingWouldClose() throws Exception {
        LazyFirestoreClient holder = holder();
        holder.close();

        assertThatThrownBy(() -> holder.get(DatabaseDestination.of("p")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("test seam")
                .hasMessageContaining("closed before use");
        assertThatThrownBy(() -> holder.useCredentials(null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("test seam");
    }

    @Test
    void refusesUseAfterClosingABuiltClient() throws Exception {
        LazyFirestoreClient holder = holder();
        holder.get(DatabaseDestination.of("p"));
        holder.close();

        assertThatThrownBy(() -> holder.get(DatabaseDestination.of("p")))
                .isInstanceOf(IOException.class);
    }
}
