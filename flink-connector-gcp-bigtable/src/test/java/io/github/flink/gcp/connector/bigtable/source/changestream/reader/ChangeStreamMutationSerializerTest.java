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

package io.github.flink.gcp.connector.bigtable.source.changestream.reader;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamMutation;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamMutationSerializer;
import io.github.flink.gcp.connector.bigtable.source.serializer.ChangeStreamMutationDeserializationSchema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChangeStreamMutationSerializerTest {

    @Test
    void theBuiltInSchemaRoundTripsEveryConnectorOwnedEntryAndValueKind() throws Exception {
        ChangeStreamMutation mutation =
                new ChangeStreamMutation(
                        ByteString.copyFromUtf8("row"),
                        ChangeStreamMutation.MutationType.GARBAGE_COLLECTION,
                        "cluster",
                        Instant.parse("2026-08-12T00:00:00.123456789Z"),
                        7,
                        "token",
                        Instant.parse("2026-08-11T23:59:00.987654321Z"),
                        Arrays.asList(
                                new ChangeStreamMutation.SetCellEntry(
                                        "set",
                                        ByteString.copyFromUtf8("q1"),
                                        11L,
                                        ByteString.copyFromUtf8("value")),
                                new ChangeStreamMutation.DeleteCellsEntry(
                                        "delete",
                                        ByteString.copyFromUtf8("q2"),
                                        new ChangeStreamMutation.TimestampRange(
                                                ChangeStreamMutation.TimestampBound.open(12L),
                                                ChangeStreamMutation.TimestampBound.closed(13L))),
                                new ChangeStreamMutation.DeleteCellsEntry(
                                        "delete-unbounded",
                                        ByteString.EMPTY,
                                        new ChangeStreamMutation.TimestampRange(
                                                ChangeStreamMutation.TimestampBound.unbounded(),
                                                ChangeStreamMutation.TimestampBound.unbounded())),
                                new ChangeStreamMutation.DeleteFamilyEntry("family"),
                                new ChangeStreamMutation.AddToCellEntry(
                                        "aggregate",
                                        new ChangeStreamMutation.RawValue(
                                                ByteString.copyFromUtf8("q3")),
                                        new ChangeStreamMutation.RawTimestamp(14L),
                                        new ChangeStreamMutation.Int64Value(15L)),
                                new ChangeStreamMutation.MergeToCellEntry(
                                        "aggregate",
                                        new ChangeStreamMutation.RawValue(
                                                ByteString.copyFromUtf8("q4")),
                                        new ChangeStreamMutation.RawTimestamp(16L),
                                        new ChangeStreamMutation.RawValue(
                                                ByteString.copyFromUtf8("input")))));
        TypeSerializer<ChangeStreamMutation> serializer =
                new ChangeStreamMutationDeserializationSchema()
                        .getProducedType()
                        .createSerializer(new SerializerConfigImpl());

        assertThat(serializer).isInstanceOf(ChangeStreamMutationSerializer.class);
        assertThat(serializer.copy(mutation)).isSameAs(mutation);

        DataOutputSerializer output = new DataOutputSerializer(256);
        serializer.serialize(mutation, output);
        ChangeStreamMutation restored =
                serializer.deserialize(new DataInputDeserializer(output.getCopyOfBuffer()));

        assertThat(restored.getRowKey()).isEqualTo(mutation.getRowKey());
        assertThat(restored.getType()).isEqualTo(mutation.getType());
        assertThat(restored.getSourceClusterId()).isEqualTo("cluster");
        assertThat(restored.getCommitTime()).isEqualTo(mutation.getCommitTime());
        assertThat(restored.getTieBreaker()).isEqualTo(7);
        assertThat(restored.getEstimatedLowWatermarkTime())
                .isEqualTo(mutation.getEstimatedLowWatermarkTime());
        assertThat(restored.getToken()).isEqualTo("token");
        assertThat(restored.getEntries()).containsExactlyElementsOf(mutation.getEntries());

        DataOutputSerializer copiedOutput = new DataOutputSerializer(256);
        serializer.copy(new DataInputDeserializer(output.getCopyOfBuffer()), copiedOutput);
        ChangeStreamMutation copied =
                serializer.deserialize(new DataInputDeserializer(copiedOutput.getCopyOfBuffer()));
        assertThat(copied.getEntries()).containsExactlyElementsOf(mutation.getEntries());
        assertThat(copied.getRowKey()).isEqualTo(mutation.getRowKey());
        assertThat(copied.getType()).isEqualTo(mutation.getType());
        assertThat(copied.getSourceClusterId()).isEqualTo(mutation.getSourceClusterId());
        assertThat(copied.getCommitTime()).isEqualTo(mutation.getCommitTime());
        assertThat(copied.getTieBreaker()).isEqualTo(mutation.getTieBreaker());
        assertThat(copied.getToken()).isEqualTo(mutation.getToken());
        assertThat(copied.getEstimatedLowWatermarkTime())
                .isEqualTo(mutation.getEstimatedLowWatermarkTime());
    }

    @Test
    void roundTripsAnEmptyProjectedMutation() throws Exception {
        ChangeStreamMutation mutation =
                new ChangeStreamMutation(
                        ByteString.copyFromUtf8("row"),
                        ChangeStreamMutation.MutationType.USER,
                        "cluster",
                        Instant.EPOCH,
                        0,
                        "token",
                        Instant.EPOCH,
                        Collections.emptyList());
        TypeSerializer<ChangeStreamMutation> serializer =
                TypeInformation.of(ChangeStreamMutation.class)
                        .createSerializer(new SerializerConfigImpl());
        DataOutputSerializer output = new DataOutputSerializer(128);

        serializer.serialize(mutation, output);
        ChangeStreamMutation restored =
                serializer.deserialize(new DataInputDeserializer(output.getCopyOfBuffer()));

        assertThat(restored.getEntries()).isEmpty();
        assertThat(restored.getRowKey()).isEqualTo(mutation.getRowKey());
        assertThat(restored.getToken()).isEqualTo(mutation.getToken());
    }

    @Test
    void mutationDefensivelyCopiesAndExposesAnUnmodifiableEntryList() {
        List<ChangeStreamMutation.Entry> entries = new ArrayList<>();
        entries.add(new ChangeStreamMutation.DeleteFamilyEntry("original"));
        ChangeStreamMutation mutation =
                new ChangeStreamMutation(
                        ByteString.copyFromUtf8("row"),
                        ChangeStreamMutation.MutationType.USER,
                        "cluster",
                        Instant.EPOCH,
                        0,
                        "token",
                        Instant.EPOCH,
                        entries);

        entries.add(new ChangeStreamMutation.DeleteFamilyEntry("later"));

        assertThat(mutation.getEntries())
                .extracting(ChangeStreamMutation.Entry::getFamilyName)
                .containsExactly("original");
        assertThatThrownBy(() -> mutation.getEntries().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void serializedCopyStreamsByteStringsLargerThanItsBuffer() throws Exception {
        byte[] rowKey = new byte[4 * 1024 + 17];
        byte[] qualifier = new byte[4 * 1024 + 31];
        byte[] value = new byte[2 * 4 * 1024 + 43];
        Arrays.fill(rowKey, (byte) 1);
        Arrays.fill(qualifier, (byte) 2);
        Arrays.fill(value, (byte) 3);
        ChangeStreamMutation mutation =
                new ChangeStreamMutation(
                        ByteString.copyFrom(rowKey),
                        ChangeStreamMutation.MutationType.USER,
                        "cluster",
                        Instant.EPOCH,
                        0,
                        "token",
                        Instant.EPOCH,
                        Collections.singletonList(
                                new ChangeStreamMutation.SetCellEntry(
                                        "family",
                                        ByteString.copyFrom(qualifier),
                                        1L,
                                        ByteString.copyFrom(value))));
        ChangeStreamMutationSerializer serializer = new ChangeStreamMutationSerializer();
        DataOutputSerializer serialized = new DataOutputSerializer(256);
        serializer.serialize(mutation, serialized);
        DataOutputSerializer copied = new DataOutputSerializer(256);

        serializer.copy(new DataInputDeserializer(serialized.getCopyOfBuffer()), copied);

        assertThat(copied.getCopyOfBuffer()).isEqualTo(serialized.getCopyOfBuffer());
        ChangeStreamMutation restored =
                serializer.deserialize(new DataInputDeserializer(copied.getCopyOfBuffer()));
        assertThat(restored.getRowKey()).isEqualTo(mutation.getRowKey());
        assertThat(restored.getEntries()).containsExactlyElementsOf(mutation.getEntries());
    }

    @Test
    void aNegativeByteStringLengthIsRejectedRatherThanSized() throws Exception {
        byte[] state = serializedEmptyMutation();
        // The stream opens with the row key's length, which is zero for this mutation. Asserting
        // that first keeps a reordered format from turning the corruption below into a no-op.
        assertThat(Arrays.copyOf(state, 4)).containsExactly(0, 0, 0, 0);
        state[0] = (byte) 0xFF;
        state[1] = (byte) 0xFF;
        state[2] = (byte) 0xFF;
        state[3] = (byte) 0xFF;

        assertThatThrownBy(
                        () ->
                                new ChangeStreamMutationSerializer()
                                        .deserialize(new DataInputDeserializer(state)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Negative byte string length");
        assertThatThrownBy(
                        () ->
                                new ChangeStreamMutationSerializer()
                                        .copy(
                                                new DataInputDeserializer(state),
                                                new DataOutputSerializer(64)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Negative byte string length");
    }

    @Test
    void anUnknownMutationTypeIsRejectedInsteadOfCopiedOn() throws Exception {
        byte[] state = serializedEmptyMutation();
        // An empty row key occupies its four length bytes alone, so the mutation type follows it.
        assertThat(state[4]).isEqualTo((byte) 1);
        state[4] = 9;

        assertThatThrownBy(
                        () ->
                                new ChangeStreamMutationSerializer()
                                        .copy(
                                                new DataInputDeserializer(state),
                                                new DataOutputSerializer(64)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unknown Bigtable Change Streams mutation type: 9");
    }

    @Test
    void anUnknownEntryTagIsRejectedInsteadOfCopiedOn() throws Exception {
        byte[] empty = serializedEmptyMutation();
        // The entry count closes an entryless stream, so one entry can be appended behind it.
        byte[] state = Arrays.copyOf(empty, empty.length + 1);
        assertThat(empty[empty.length - 1]).isZero();
        state[empty.length - 1] = 1;
        state[empty.length] = 9;

        assertThatThrownBy(
                        () ->
                                new ChangeStreamMutationSerializer()
                                        .copy(
                                                new DataInputDeserializer(state),
                                                new DataOutputSerializer(64)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unknown Bigtable Change Streams entry tag: 9");
    }

    @Test
    void theTypeAnnotationSelectsTheConnectorSerializerWithoutReflectiveKryo() {
        TypeInformation<ChangeStreamMutation> builtIn =
                new ChangeStreamMutationDeserializationSchema().getProducedType();
        TypeInformation<ChangeStreamMutation> annotated =
                TypeInformation.of(ChangeStreamMutation.class);

        assertThat(annotated).isEqualTo(builtIn);
        assertThat(annotated.createSerializer(new SerializerConfigImpl()))
                .isInstanceOf(ChangeStreamMutationSerializer.class);
    }

    private static byte[] serializedEmptyMutation() throws IOException {
        ChangeStreamMutation mutation =
                new ChangeStreamMutation(
                        ByteString.EMPTY,
                        ChangeStreamMutation.MutationType.USER,
                        "cluster",
                        Instant.EPOCH,
                        0,
                        "token",
                        Instant.EPOCH,
                        Collections.emptyList());
        DataOutputSerializer output = new DataOutputSerializer(128);
        new ChangeStreamMutationSerializer().serialize(mutation, output);
        return output.getCopyOfBuffer();
    }
}
