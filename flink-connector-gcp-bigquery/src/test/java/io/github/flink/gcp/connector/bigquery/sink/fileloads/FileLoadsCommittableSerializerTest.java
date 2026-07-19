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

package io.github.flink.gcp.connector.bigquery.sink.fileloads;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FileLoadsCommittableSerializer}. */
class FileLoadsCommittableSerializerTest {

    private final FileLoadsCommittableSerializer serializer = new FileLoadsCommittableSerializer();

    @Test
    void roundTrips() throws IOException {
        FileLoadsCommittable committable =
                new FileLoadsCommittable(
                        TableDestination.of("my-project", "my_dataset", "my_table"),
                        "gs://bucket/prefix/jobid/my-project.my_dataset.my_table/0-0-abc-0.avro",
                        123_456L,
                        789L);

        FileLoadsCommittable copy =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(committable));

        assertThat(copy).isEqualTo(committable);
    }

    @Test
    void rejectsUnknownVersion() throws IOException {
        FileLoadsCommittable committable =
                new FileLoadsCommittable(
                        TableDestination.of("p", "d", "t"), "gs://bucket/o.avro", 1L, 1L);
        byte[] bytes = serializer.serialize(committable);

        assertThatThrownBy(() -> serializer.deserialize(99, bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("version");
    }
}
