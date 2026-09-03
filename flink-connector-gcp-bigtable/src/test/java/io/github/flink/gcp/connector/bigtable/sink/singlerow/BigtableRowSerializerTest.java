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

package io.github.flink.gcp.connector.bigtable.sink.singlerow;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BigtableRowSerializer} and the type information that selects it. */
class BigtableRowSerializerTest {

    private static final BigtableRow ROW =
            new BigtableRow(
                    ByteString.copyFromUtf8("row"),
                    Arrays.asList(
                            new BigtableRow.Cell(
                                    "cf",
                                    ByteString.copyFromUtf8("q1"),
                                    11L,
                                    ByteString.copyFromUtf8("value"),
                                    Arrays.asList("l1", "l2")),
                            new BigtableRow.Cell(
                                    "other",
                                    ByteString.EMPTY,
                                    -1L,
                                    ByteString.copyFrom(new byte[] {0, (byte) 0xff}),
                                    Collections.emptyList())));

    @Test
    void theTypeInformationOfARowSelectsTheConnectorOwnedSerializer() throws Exception {
        // Both routes a job's type extraction takes, since a reflective fallback would pick a
        // Kryo serializer for ByteString and silently work until a Flink upgrade changes it.
        TypeInformation<BigtableRow> declared = TypeInformation.of(BigtableRow.class);
        TypeInformation<BigtableRow> extracted = TypeExtractor.createTypeInfo(BigtableRow.class);

        assertThat(declared.getTypeClass()).isEqualTo(BigtableRow.class);
        assertThat(declared.toString()).isEqualTo("BigtableRow");
        assertThat(extracted).isEqualTo(declared).hasSameHashCodeAs(declared);
        assertThat(new BigtableRowTypeInfoFactory().createTypeInfo(BigtableRow.class, null))
                .isEqualTo(declared);
        TypeSerializer<BigtableRow> serializer =
                declared.createSerializer(new SerializerConfigImpl());
        assertThat(serializer).isInstanceOf(BigtableRowSerializer.class);
        assertThat(roundTrip(serializer, ROW)).isEqualTo(ROW);
        // The entry point Flink 1.20 calls, which 2.x's TypeInformation no longer declares: reached
        // reflectively so the 2.x build exercises the overload the 1.20 build resolves virtually.
        assertThat(createSerializer1x(declared)).isInstanceOf(BigtableRowSerializer.class);
    }

    private static Object createSerializer1x(TypeInformation<BigtableRow> typeInformation)
            throws Exception {
        Method method =
                typeInformation.getClass().getMethod("createSerializer", ExecutionConfig.class);
        method.setAccessible(true);
        return method.invoke(typeInformation, new ExecutionConfig());
    }

    @Test
    void roundTripsEveryFieldAndAnEmptyRow() throws Exception {
        BigtableRowSerializer serializer = new BigtableRowSerializer();

        BigtableRow restored = roundTrip(serializer, ROW);

        assertThat(restored).isEqualTo(ROW);
        // Anchored against literals, so the round trip is pinned to the values meant for the wire.
        assertThat(restored.getKey().toStringUtf8()).isEqualTo("row");
        assertThat(restored.getCells().get(0).getLabels()).containsExactly("l1", "l2");
        assertThat(restored.getCells().get(1).getTimestampMicros()).isEqualTo(-1L);
        assertThat(restored.getCells().get(1).getValue().toByteArray()).containsExactly(0, 0xff);
        BigtableRow empty = new BigtableRow(ByteString.EMPTY, Collections.emptyList());
        assertThat(roundTrip(serializer, empty)).isEqualTo(empty);
    }

    @Test
    void theWireFormatIsTheDocumentedOne() throws Exception {
        // Assembled by hand from the format the class documents, so a change to the serializer
        // that still round-trips — a reordered field, a varint — is caught as a savepoint break.
        BigtableRow row =
                new BigtableRow(
                        ByteString.copyFromUtf8("k"),
                        Collections.singletonList(
                                new BigtableRow.Cell(
                                        "cf",
                                        ByteString.copyFromUtf8("q"),
                                        7L,
                                        ByteString.copyFromUtf8("v"),
                                        Collections.singletonList("l"))));
        DataOutputSerializer expected = new DataOutputSerializer(64);
        writeBytes(expected, "k");
        expected.writeInt(1);
        writeBytes(expected, "cf");
        writeBytes(expected, "q");
        expected.writeLong(7L);
        writeBytes(expected, "v");
        expected.writeInt(1);
        writeBytes(expected, "l");

        DataOutputSerializer actual = new DataOutputSerializer(64);
        new BigtableRowSerializer().serialize(row, actual);

        assertThat(actual.getCopyOfBuffer()).isEqualTo(expected.getCopyOfBuffer());
        assertThat(
                        new BigtableRowSerializer()
                                .deserialize(new DataInputDeserializer(expected.getCopyOfBuffer())))
                .isEqualTo(row);
    }

    @Test
    void copiesTheWireFormWithoutMaterialisingIt() throws Exception {
        byte[] value = new byte[2 * 4 * 1024 + 43];
        Arrays.fill(value, (byte) 3);
        BigtableRow row =
                new BigtableRow(
                        ByteString.copyFromUtf8("row"),
                        Collections.singletonList(
                                new BigtableRow.Cell(
                                        "cf",
                                        ByteString.copyFromUtf8("q"),
                                        1L,
                                        ByteString.copyFrom(value),
                                        Collections.emptyList())));
        BigtableRowSerializer serializer = new BigtableRowSerializer();
        DataOutputSerializer serialized = new DataOutputSerializer(256);
        serializer.serialize(row, serialized);
        DataOutputSerializer copied = new DataOutputSerializer(256);

        serializer.copy(new DataInputDeserializer(serialized.getCopyOfBuffer()), copied);

        assertThat(copied.getCopyOfBuffer()).isEqualTo(serialized.getCopyOfBuffer());
        assertThat(serializer.copy(row)).isSameAs(row);
        assertThat(serializer.isImmutableType()).isTrue();
    }

    @Test
    void rejectsANegativeCountRatherThanAllocatingIt() {
        DataOutputSerializer corrupt = new DataOutputSerializer(16);
        BigtableRowSerializer serializer = new BigtableRowSerializer();

        assertThatThrownBy(
                        () -> {
                            writeBytes(corrupt, "k");
                            corrupt.writeInt(-1);
                            serializer.deserialize(
                                    new DataInputDeserializer(corrupt.getCopyOfBuffer()));
                        })
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Negative cell count");
    }

    @Test
    void theSnapshotRestoresTheSerializerAndRefusesAnotherVersion() throws Exception {
        BigtableRowSerializer.Snapshot written = new BigtableRowSerializer.Snapshot();
        DataOutputSerializer output = new DataOutputSerializer(16);
        written.writeSnapshot(output);
        BigtableRowSerializer.Snapshot restored = new BigtableRowSerializer.Snapshot();
        restored.readSnapshot(
                written.getCurrentVersion(),
                new DataInputDeserializer(output.getCopyOfBuffer()),
                getClass().getClassLoader());

        assertThat(restored.restoreSerializer()).isInstanceOf(BigtableRowSerializer.class);
        assertThat(restored.resolveSchemaCompatibility(new BigtableRowSerializer.Snapshot()))
                .matches(compatibility -> compatibility.isCompatibleAsIs());
        assertThat(new BigtableRowSerializer().snapshotConfiguration())
                .isInstanceOf(BigtableRowSerializer.Snapshot.class);
        assertThatThrownBy(
                        () ->
                                new BigtableRowSerializer.Snapshot()
                                        .readSnapshot(
                                                2,
                                                new DataInputDeserializer(new byte[0]),
                                                getClass().getClassLoader()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("2");
    }

    @Test
    void theRowCopiesItsCellsAndTheCellItsLabels() {
        List<BigtableRow.Cell> cells = new ArrayList<>();
        List<String> labels = new ArrayList<>();
        labels.add("original");
        cells.add(new BigtableRow.Cell("cf", ByteString.EMPTY, 0L, ByteString.EMPTY, labels));
        BigtableRow row = new BigtableRow(ByteString.EMPTY, cells);

        labels.add("later");
        cells.clear();

        assertThat(row.getCells()).hasSize(1);
        assertThat(row.getCells().get(0).getLabels()).containsExactly("original");
        assertThatThrownBy(() -> row.getCells().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(
                        () ->
                                new BigtableRow(
                                        ByteString.EMPTY, Arrays.asList((BigtableRow.Cell) null)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static BigtableRow roundTrip(TypeSerializer<BigtableRow> serializer, BigtableRow row)
            throws IOException {
        DataOutputSerializer output = new DataOutputSerializer(256);
        serializer.serialize(row, output);
        return serializer.deserialize(new DataInputDeserializer(output.getCopyOfBuffer()));
    }

    private static void writeBytes(DataOutputSerializer target, String utf8) throws IOException {
        byte[] bytes = utf8.getBytes(StandardCharsets.UTF_8);
        target.writeInt(bytes.length);
        target.write(bytes);
    }
}
