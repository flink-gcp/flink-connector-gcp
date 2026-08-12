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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BufferedStreamWriterStateSerializer}. */
class BufferedStreamWriterStateSerializerTest {

    private static final TableDestination DESTINATION = TableDestination.of("p", "d", "t");

    private final BufferedStreamWriterStateSerializer serializer =
            new BufferedStreamWriterStateSerializer();

    @Test
    void roundTrips() throws IOException {
        BufferedStreamWriterState state =
                new BufferedStreamWriterState(
                        DESTINATION, "projects/p/datasets/d/tables/t/streams/abc123", 42L, 7L);

        BufferedStreamWriterState copy =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(state));

        assertThat(copy).isEqualTo(state);
    }

    @Test
    void roundTripsTheNoStreamMarker() throws IOException {
        BufferedStreamWriterState state =
                new BufferedStreamWriterState(
                        DESTINATION, BufferedStreamWriterState.NO_STREAM, 0L, 1L);

        BufferedStreamWriterState copy =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(state));

        assertThat(copy).isEqualTo(state);
        assertThat(copy.getStreamName()).isEqualTo(BufferedStreamWriterState.NO_STREAM);
    }

    @Test
    void rejectsUnknownVersion() throws IOException {
        byte[] bytes =
                serializer.serialize(
                        new BufferedStreamWriterState(DESTINATION, "projects/p/streams/s", 1L, 1L));

        assertThatThrownBy(() -> serializer.deserialize(99, bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("version");
    }

    @Test
    void migratesVersionOneWithTheConfiguredFixedDestination() throws IOException {
        BufferedStreamWriterStateSerializer migrating =
                new BufferedStreamWriterStateSerializer(DESTINATION);
        byte[] versionOne;
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeUTF("projects/p/datasets/d/tables/t/streams/legacy");
            out.writeLong(13L);
            out.writeLong(5L);
            versionOne = bytes.toByteArray();
        }

        assertThat(migrating.deserialize(1, versionOne))
                .isEqualTo(
                        new BufferedStreamWriterState(
                                DESTINATION,
                                "projects/p/datasets/d/tables/t/streams/legacy",
                                13L,
                                5L));
        assertThatThrownBy(() -> serializer.deserialize(1, versionOne))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("legacy fixed destination");
    }

    @Test
    void rejectsVersionOneStreamFromAnotherConfiguredDestination() throws IOException {
        BufferedStreamWriterStateSerializer migrating =
                new BufferedStreamWriterStateSerializer(
                        TableDestination.of("p", "d", "changed_table"));
        byte[] versionOne;
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeUTF("projects/p/datasets/d/tables/t/streams/legacy");
            out.writeLong(13L);
            out.writeLong(5L);
            versionOne = bytes.toByteArray();
        }

        assertThatThrownBy(() -> migrating.deserialize(1, versionOne))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("does not belong to the configured fixed destination");
    }
}
