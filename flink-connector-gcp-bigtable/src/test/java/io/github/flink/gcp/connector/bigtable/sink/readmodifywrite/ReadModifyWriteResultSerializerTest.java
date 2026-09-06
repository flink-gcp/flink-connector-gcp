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

package io.github.flink.gcp.connector.bigtable.sink.readmodifywrite;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRow;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRowSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReadModifyWriteResultSerializerTest {
    @Test
    void typeExtractionUsesTheNestedVersionedFormatAndPreservesItsSnapshot() throws Exception {
        TypeSerializer<ReadModifyWriteResult> serializer =
                TypeInformation.of(ReadModifyWriteResult.class)
                        .createSerializer(new SerializerConfigImpl());
        assertThat(serializer).isInstanceOf(ReadModifyWriteResultSerializer.class);
        BigtableRow row =
                new BigtableRow(
                        ByteString.copyFrom(new byte[] {0, -1}),
                        Arrays.asList(
                                new BigtableRow.Cell(
                                        "cf",
                                        ByteString.EMPTY,
                                        123000,
                                        ByteString.copyFrom(new byte[] {-1, 0}),
                                        Arrays.asList("label")),
                                new BigtableRow.Cell(
                                        "other",
                                        ByteString.copyFromUtf8("q"),
                                        124000,
                                        ByteString.EMPTY,
                                        Arrays.asList())));
        ReadModifyWriteResult result =
                new ReadModifyWriteResult(TableDestination.of("p", "i", "t"), row);
        DataOutputSerializer encoded = new DataOutputSerializer(64);
        serializer.serialize(result, encoded);
        DataOutputSerializer golden = new DataOutputSerializer(64);
        for (char component : new char[] {'p', 'i', 't'}) {
            golden.writeInt(1);
            golden.writeByte(component);
        }
        new BigtableRowSerializer().serialize(row, golden);
        assertThat(encoded.getCopyOfBuffer()).isEqualTo(golden.getCopyOfBuffer());
        assertThat(serializer.deserialize(new DataInputDeserializer(golden.getCopyOfBuffer())))
                .isEqualTo(result);
        DataOutputSerializer copied = new DataOutputSerializer(64);
        serializer.copy(new DataInputDeserializer(encoded.getCopyOfBuffer()), copied);
        assertThat(copied.getCopyOfBuffer()).isEqualTo(encoded.getCopyOfBuffer());
        assertThat(serializer.copy(result)).isSameAs(result);
        assertThat(serializer.copy(result, result)).isSameAs(result);
        assertThat(serializer.duplicate()).isEqualTo(serializer);
        TypeSerializerSnapshot<ReadModifyWriteResult> original = serializer.snapshotConfiguration();
        DataOutputSerializer snapshotBytes = new DataOutputSerializer(64);
        TypeSerializerSnapshot.writeVersionedSnapshot(snapshotBytes, original);
        TypeSerializerSnapshot<ReadModifyWriteResult> restored =
                TypeSerializerSnapshot.readVersionedSnapshot(
                        new DataInputDeserializer(snapshotBytes.getCopyOfBuffer()),
                        getClass().getClassLoader());
        assertThat(restored.restoreSerializer()).isEqualTo(serializer);
        assertThat(restored.resolveSchemaCompatibility(original).isCompatibleAsIs()).isTrue();
        assertThat(
                        restored.restoreSerializer()
                                .deserialize(new DataInputDeserializer(encoded.getCopyOfBuffer())))
                .isEqualTo(result);
        assertThatThrownBy(
                        () ->
                                new ReadModifyWriteResultSerializer.Snapshot()
                                        .readSnapshot(
                                                2,
                                                new DataInputDeserializer(new byte[0]),
                                                getClass().getClassLoader()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("version 2");
    }

    @Test
    void malformedDestinationLengthsFailBeforeAllocation() throws Exception {
        DataOutputSerializer encoded = new DataOutputSerializer(4);
        encoded.writeInt(-1);
        ReadModifyWriteResultSerializer serializer = new ReadModifyWriteResultSerializer();
        assertThatThrownBy(
                        () ->
                                serializer.deserialize(
                                        new DataInputDeserializer(encoded.getCopyOfBuffer())))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(
                        () ->
                                serializer.copy(
                                        new DataInputDeserializer(encoded.getCopyOfBuffer()),
                                        new DataOutputSerializer(8)))
                .isInstanceOf(IOException.class);
    }
}
