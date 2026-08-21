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
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutation;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutationSerializer;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableChangeStreamMutationDeserializationSchema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BigtableChangeStreamMutationSerializerTest {

    @Test
    void theBuiltInSchemaRoundTripsEveryConnectorOwnedEntryAndValueKind() throws Exception {
        BigtableChangeStreamMutation mutation =
                new BigtableChangeStreamMutation(
                        ByteString.copyFromUtf8("row"),
                        BigtableChangeStreamMutation.MutationType.GARBAGE_COLLECTION,
                        "cluster",
                        Instant.parse("2026-08-12T00:00:00.123456789Z"),
                        7,
                        "token",
                        Instant.parse("2026-08-11T23:59:00.987654321Z"),
                        Arrays.asList(
                                new BigtableChangeStreamMutation.SetCellEntry(
                                        "set",
                                        ByteString.copyFromUtf8("q1"),
                                        11L,
                                        ByteString.copyFromUtf8("value")),
                                new BigtableChangeStreamMutation.DeleteCellsEntry(
                                        "delete",
                                        ByteString.copyFromUtf8("q2"),
                                        new BigtableChangeStreamMutation.TimestampRange(
                                                BigtableChangeStreamMutation.TimestampBound.open(
                                                        12L),
                                                BigtableChangeStreamMutation.TimestampBound.closed(
                                                        13L))),
                                new BigtableChangeStreamMutation.DeleteCellsEntry(
                                        "delete-unbounded",
                                        ByteString.EMPTY,
                                        new BigtableChangeStreamMutation.TimestampRange(
                                                BigtableChangeStreamMutation.TimestampBound
                                                        .unbounded(),
                                                BigtableChangeStreamMutation.TimestampBound
                                                        .unbounded())),
                                new BigtableChangeStreamMutation.DeleteFamilyEntry("family"),
                                new BigtableChangeStreamMutation.AddToCellEntry(
                                        "aggregate",
                                        new BigtableChangeStreamMutation.RawValue(
                                                ByteString.copyFromUtf8("q3")),
                                        new BigtableChangeStreamMutation.RawTimestamp(14L),
                                        new BigtableChangeStreamMutation.Int64Value(15L)),
                                new BigtableChangeStreamMutation.MergeToCellEntry(
                                        "aggregate",
                                        new BigtableChangeStreamMutation.RawValue(
                                                ByteString.copyFromUtf8("q4")),
                                        new BigtableChangeStreamMutation.RawTimestamp(16L),
                                        new BigtableChangeStreamMutation.RawValue(
                                                ByteString.copyFromUtf8("input")))));
        TypeSerializer<BigtableChangeStreamMutation> serializer =
                new BigtableChangeStreamMutationDeserializationSchema()
                        .getProducedType()
                        .createSerializer(new SerializerConfigImpl());

        assertThat(serializer).isInstanceOf(BigtableChangeStreamMutationSerializer.class);
        assertThat(serializer.copy(mutation)).isSameAs(mutation);

        DataOutputSerializer output = new DataOutputSerializer(256);
        serializer.serialize(mutation, output);
        BigtableChangeStreamMutation restored =
                serializer.deserialize(new DataInputDeserializer(output.getCopyOfBuffer()));

        assertThat(restored).isEqualTo(mutation);
        // Anchored against literals rather than against `mutation`, so the round trip is pinned to
        // the values that were meant to go on the wire and not merely to whatever was constructed.
        assertThat(restored.getSourceClusterId()).isEqualTo("cluster");
        assertThat(restored.getTieBreaker()).isEqualTo(7);
        assertThat(restored.getToken()).isEqualTo("token");

        DataOutputSerializer copiedOutput = new DataOutputSerializer(256);
        serializer.copy(new DataInputDeserializer(output.getCopyOfBuffer()), copiedOutput);
        BigtableChangeStreamMutation copied =
                serializer.deserialize(new DataInputDeserializer(copiedOutput.getCopyOfBuffer()));
        assertThat(copied).isEqualTo(mutation);
    }

    @Test
    void roundTripsAnEmptyProjectedMutation() throws Exception {
        BigtableChangeStreamMutation mutation =
                new BigtableChangeStreamMutation(
                        ByteString.copyFromUtf8("row"),
                        BigtableChangeStreamMutation.MutationType.USER,
                        "cluster",
                        Instant.EPOCH,
                        0,
                        "token",
                        Instant.EPOCH,
                        Collections.emptyList());
        TypeSerializer<BigtableChangeStreamMutation> serializer =
                TypeInformation.of(BigtableChangeStreamMutation.class)
                        .createSerializer(new SerializerConfigImpl());
        DataOutputSerializer output = new DataOutputSerializer(128);

        serializer.serialize(mutation, output);
        BigtableChangeStreamMutation restored =
                serializer.deserialize(new DataInputDeserializer(output.getCopyOfBuffer()));

        assertThat(restored.getEntries()).isEmpty();
        assertThat(restored).isEqualTo(mutation);
    }

    @Test
    void mutationDefensivelyCopiesAndExposesAnUnmodifiableEntryList() {
        List<BigtableChangeStreamMutation.Entry> entries = new ArrayList<>();
        entries.add(new BigtableChangeStreamMutation.DeleteFamilyEntry("original"));
        BigtableChangeStreamMutation mutation =
                new BigtableChangeStreamMutation(
                        ByteString.copyFromUtf8("row"),
                        BigtableChangeStreamMutation.MutationType.USER,
                        "cluster",
                        Instant.EPOCH,
                        0,
                        "token",
                        Instant.EPOCH,
                        entries);

        entries.add(new BigtableChangeStreamMutation.DeleteFamilyEntry("later"));

        assertThat(mutation.getEntries())
                .extracting(BigtableChangeStreamMutation.Entry::getFamilyName)
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
        BigtableChangeStreamMutation mutation =
                new BigtableChangeStreamMutation(
                        ByteString.copyFrom(rowKey),
                        BigtableChangeStreamMutation.MutationType.USER,
                        "cluster",
                        Instant.EPOCH,
                        0,
                        "token",
                        Instant.EPOCH,
                        Collections.singletonList(
                                new BigtableChangeStreamMutation.SetCellEntry(
                                        "family",
                                        ByteString.copyFrom(qualifier),
                                        1L,
                                        ByteString.copyFrom(value))));
        BigtableChangeStreamMutationSerializer serializer =
                new BigtableChangeStreamMutationSerializer();
        DataOutputSerializer serialized = new DataOutputSerializer(256);
        serializer.serialize(mutation, serialized);
        DataOutputSerializer copied = new DataOutputSerializer(256);

        serializer.copy(new DataInputDeserializer(serialized.getCopyOfBuffer()), copied);

        assertThat(copied.getCopyOfBuffer()).isEqualTo(serialized.getCopyOfBuffer());
        BigtableChangeStreamMutation restored =
                serializer.deserialize(new DataInputDeserializer(copied.getCopyOfBuffer()));
        assertThat(restored).isEqualTo(mutation);
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
                                new BigtableChangeStreamMutationSerializer()
                                        .deserialize(new DataInputDeserializer(state)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Negative byte string length");
        assertThatThrownBy(
                        () ->
                                new BigtableChangeStreamMutationSerializer()
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
                                new BigtableChangeStreamMutationSerializer()
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
                                new BigtableChangeStreamMutationSerializer()
                                        .copy(
                                                new DataInputDeserializer(state),
                                                new DataOutputSerializer(64)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unknown Bigtable Change Streams entry tag: 9");
    }

    @Test
    void theTypeAnnotationSelectsTheConnectorSerializerWithoutReflectiveKryo() {
        TypeInformation<BigtableChangeStreamMutation> builtIn =
                new BigtableChangeStreamMutationDeserializationSchema().getProducedType();
        TypeInformation<BigtableChangeStreamMutation> annotated =
                TypeInformation.of(BigtableChangeStreamMutation.class);

        assertThat(annotated).isEqualTo(builtIn);
        assertThat(annotated.createSerializer(new SerializerConfigImpl()))
                .isInstanceOf(BigtableChangeStreamMutationSerializer.class);
    }

    private static byte[] serializedEmptyMutation() throws IOException {
        BigtableChangeStreamMutation mutation =
                new BigtableChangeStreamMutation(
                        ByteString.EMPTY,
                        BigtableChangeStreamMutation.MutationType.USER,
                        "cluster",
                        Instant.EPOCH,
                        0,
                        "token",
                        Instant.EPOCH,
                        Collections.emptyList());
        DataOutputSerializer output = new DataOutputSerializer(128);
        new BigtableChangeStreamMutationSerializer().serialize(mutation, output);
        return output.getCopyOfBuffer();
    }
}
