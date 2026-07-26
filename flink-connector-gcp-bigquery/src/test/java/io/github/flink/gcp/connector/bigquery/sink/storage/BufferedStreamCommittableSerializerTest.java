/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.bigquery.sink.storage;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BufferedStreamCommittableSerializer}. */
class BufferedStreamCommittableSerializerTest {

    private final BufferedStreamCommittableSerializer serializer =
            new BufferedStreamCommittableSerializer();

    @Test
    void roundTrips() throws IOException {
        BufferedStreamCommittable committable =
                new BufferedStreamCommittable(
                        "projects/p/datasets/d/tables/t/streams/abc123", 41L, 3);

        BufferedStreamCommittable copy =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(committable));

        assertThat(copy).isEqualTo(committable);
        assertThat(copy.getStreamName()).isEqualTo("projects/p/datasets/d/tables/t/streams/abc123");
        assertThat(copy.getFlushOffset()).isEqualTo(41L);
        assertThat(copy.getSubtaskId()).isEqualTo(3);
    }

    @Test
    void rejectsUnknownVersion() throws IOException {
        byte[] bytes =
                serializer.serialize(new BufferedStreamCommittable("projects/p/streams/s", 0L, 0));

        assertThatThrownBy(() -> serializer.deserialize(99, bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("version");
    }
}
