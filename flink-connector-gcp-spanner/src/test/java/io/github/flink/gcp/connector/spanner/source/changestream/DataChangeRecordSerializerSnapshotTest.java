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

package io.github.flink.gcp.connector.spanner.source.changestream;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DataChangeRecordSerializerSnapshotTest {

    @Test
    void restoresTheSerializerAndAcceptsTheSameFormat() throws Exception {
        DataChangeRecordSerializer.Snapshot written = new DataChangeRecordSerializer.Snapshot();
        DataOutputSerializer output = new DataOutputSerializer(8);
        written.writeSnapshot(output);
        DataChangeRecordSerializer.Snapshot restored = new DataChangeRecordSerializer.Snapshot();

        restored.readSnapshot(
                written.getCurrentVersion(),
                new DataInputDeserializer(output.getCopyOfBuffer()),
                getClass().getClassLoader());

        assertThat(restored.restoreSerializer()).isEqualTo(new DataChangeRecordSerializer());
        assertThat(restored.resolveSchemaCompatibility(new DataChangeRecordSerializer.Snapshot()))
                .matches(TypeSerializerSchemaCompatibility::isCompatibleAsIs);
        assertThat(restored.resolveSchemaCompatibility(new UnrelatedSnapshot()))
                .matches(TypeSerializerSchemaCompatibility::isIncompatible);
    }

    @Test
    void rejectsAnUnknownSnapshotFormat() {
        DataChangeRecordSerializer.Snapshot snapshot = new DataChangeRecordSerializer.Snapshot();

        assertThatThrownBy(
                        () ->
                                snapshot.readSnapshot(
                                        2,
                                        new DataInputDeserializer(new byte[0]),
                                        getClass().getClassLoader()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("snapshot version 2");
    }

    private static final class UnrelatedSnapshot
            implements TypeSerializerSnapshot<DataChangeRecord> {

        @Override
        public int getCurrentVersion() {
            return 1;
        }

        @Override
        public void writeSnapshot(DataOutputView out) {}

        @Override
        public void readSnapshot(
                int readVersion, DataInputView in, ClassLoader userCodeClassLoader) {}

        @Override
        public TypeSerializer<DataChangeRecord> restoreSerializer() {
            return new DataChangeRecordSerializer();
        }

        @Override
        public TypeSerializerSchemaCompatibility<DataChangeRecord> resolveSchemaCompatibility(
                TypeSerializerSnapshot<DataChangeRecord> oldSerializerSnapshot) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }
    }
}
