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

package io.github.flink.gcp.connector.bigtable.sink.conditional;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConditionalResultSerializerTest {
    @Test
    void typeExtractionChoosesTheVersionedFieldFormatAndCopiesIt() throws Exception {
        TypeSerializer<ConditionalResult> serializer =
                TypeInformation.of(ConditionalResult.class)
                        .createSerializer(new SerializerConfigImpl());
        assertThat(serializer).isInstanceOf(ConditionalResultSerializer.class);
        ConditionalResult result =
                new ConditionalResult(
                        TableDestination.of("p", "i", "t"),
                        ByteString.copyFrom(new byte[] {0, (byte) 255}),
                        false,
                        true);
        DataOutputSerializer encoded = new DataOutputSerializer(64);
        serializer.serialize(result, encoded);
        DataOutputSerializer golden = new DataOutputSerializer(64);
        for (char component : new char[] {'p', 'i', 't'}) {
            golden.writeInt(1);
            golden.writeByte(component);
        }
        golden.writeInt(2);
        golden.writeByte(0);
        golden.writeByte(255);
        golden.writeBoolean(false);
        golden.writeBoolean(true);
        assertThat(encoded.getCopyOfBuffer()).isEqualTo(golden.getCopyOfBuffer());
        assertThat(serializer.deserialize(new DataInputDeserializer(golden.getCopyOfBuffer())))
                .isEqualTo(result);
        DataOutputSerializer copied = new DataOutputSerializer(64);
        serializer.copy(new DataInputDeserializer(encoded.getCopyOfBuffer()), copied);
        assertThat(copied.getCopyOfBuffer()).isEqualTo(encoded.getCopyOfBuffer());
        assertThat(serializer.copy(result)).isSameAs(result);
        ConditionalResultSerializer.Snapshot snapshot = new ConditionalResultSerializer.Snapshot();
        snapshot.readSnapshot(
                1, new DataInputDeserializer(new byte[0]), getClass().getClassLoader());
        assertThat(snapshot.restoreSerializer()).isEqualTo(serializer);
        assertThat(
                        snapshot.resolveSchemaCompatibility(serializer.snapshotConfiguration())
                                .isCompatibleAsIs())
                .isTrue();
        assertThatThrownBy(
                        () ->
                                snapshot.readSnapshot(
                                        2,
                                        new DataInputDeserializer(new byte[0]),
                                        getClass().getClassLoader()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("version 2");
    }
}
