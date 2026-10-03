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

package io.github.flink.gcp.connector.datastore.sink;

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.PathElement;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

import static org.assertj.core.api.Assertions.assertThat;

class FailedMutationTest {

    private static final DatabaseDestination DATABASE = DatabaseDestination.of("p");

    @Test
    void thePayloadIsTheJavaSerializedWriteAndRoundTrips() throws Exception {
        DatastoreMutation write =
                DatastoreMutation.upsert(
                        Entity.newBuilder(Key.newBuilder("p", "Order", "o-1").build())
                                .set("v", 1L)
                                .build());
        FailedMutation failed = FailedMutation.of(DATABASE, write, "refused", null);

        try (ObjectInputStream in =
                new ObjectInputStream(
                        new ByteArrayInputStream(failed.getPayloadBytes().toByteArray()))) {
            assertThat(in.readObject()).isEqualTo(write);
        }
        assertThat(failed.getConnector()).isEqualTo("datastore");
        assertThat(failed.getErrorMessage()).isEqualTo("refused");
        assertThat(failed.getMutation()).isSameAs(write);
        assertThat(failed.getDatabase()).isEqualTo(DATABASE);
    }

    @Test
    void theDestinationIsTheDatabaseNamespaceAndKeyPath() {
        Key key =
                Key.newBuilder("p", "Order", "o-1")
                        .addAncestor(PathElement.of("Customer", 7L))
                        .setNamespace("tenant")
                        .build();
        Key plain = Key.newBuilder("p", "Order", 3L).build();

        assertThat(
                        FailedMutation.of(DATABASE, DatastoreMutation.delete(key), "m", null)
                                .describeDestination())
                .isEqualTo("projects/p/databases/(default)/namespaces/tenant/Customer/7/Order/o-1");
        assertThat(
                        FailedMutation.of(DATABASE, DatastoreMutation.delete(plain), "m", null)
                                .describeDestination())
                .isEqualTo("projects/p/databases/(default)/Order/3");
    }

    @Test
    void aKeyOfAnotherDatabaseIsDescribedAsTheDatabaseItAddresses() {
        Key elsewhere = Key.newBuilder("other", "Order", "o-1", "named").build();
        Key noDatabase = Key.newBuilder("p", "Order", "o-1", null).build();

        assertThat(
                        FailedMutation.of(DATABASE, DatastoreMutation.delete(elsewhere), "m", null)
                                .describeDestination())
                .isEqualTo("projects/other/databases/named/Order/o-1");
        assertThat(
                        FailedMutation.of(DATABASE, DatastoreMutation.delete(noDatabase), "m", null)
                                .describeDestination())
                .isEqualTo("projects/p/databases/null/Order/o-1");
    }

    @Test
    void aSerializationFailureCarriesNoWriteAndNamesOnlyTheDatabase() {
        FailedMutation failed =
                FailedMutation.of(DATABASE, null, "bad record", new IllegalStateException("x"));

        assertThat(failed.getMutation()).isNull();
        assertThat(failed.getPayloadBytes()).isNull();
        assertThat(failed.describeDestination()).isEqualTo("projects/p/databases/(default)");
        assertThat(failed.getCause()).hasMessage("x");
    }
}
