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

package io.github.flink.gcp.connector.firestore.sink;

import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class FailedWriteTest {

    private static final DatabaseDestination DATABASE = DatabaseDestination.of("p");

    @Test
    void thePayloadIsTheJavaSerializedWriteAndRoundTrips() throws Exception {
        FirestoreWrite write = FirestoreWrite.set("c/a", Map.of("v", 1L));
        FailedWrite failed = FailedWrite.of(DATABASE, write, "refused", null);

        try (ObjectInputStream in =
                new ObjectInputStream(
                        new ByteArrayInputStream(failed.getPayloadBytes().toByteArray()))) {
            assertThat(in.readObject()).isEqualTo(write);
        }
        assertThat(failed.getConnector()).isEqualTo("firestore");
        assertThat(failed.describeDestination())
                .isEqualTo("projects/p/databases/(default)/documents/c/a");
    }

    @Test
    void aRecordThatNeverBecameAWriteCarriesOnlyTheDatabase() {
        FailedWrite failed =
                FailedWrite.of(DATABASE, null, "not serialized", new RuntimeException());

        assertThat(failed.getPayloadBytes()).isNull();
        assertThat(failed.describeDestination()).isEqualTo("projects/p/databases/(default)");
        assertThat(failed.toString()).contains("not serialized");
    }
}
