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

package io.github.flink.gcp.connector.bigtable.source.serializer;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChangeStreamMutationSerializerSnapshotTest {

    @Test
    void preservesTheBigtableSdkVersionAndAcceptsOnlyTheSameVersion() throws Exception {
        ChangeStreamMutationSerializer.Snapshot written =
                new ChangeStreamMutationSerializer.Snapshot("2.80.0");
        DataOutputSerializer output = new DataOutputSerializer(32);
        written.writeSnapshot(output);
        ChangeStreamMutationSerializer.Snapshot restored =
                new ChangeStreamMutationSerializer.Snapshot();

        restored.readSnapshot(
                written.getCurrentVersion(),
                new DataInputDeserializer(output.getCopyOfBuffer()),
                getClass().getClassLoader());

        assertThat(
                        restored.resolveSchemaCompatibility(
                                new ChangeStreamMutationSerializer.Snapshot("2.80.0")))
                .matches(compatibility -> compatibility.isCompatibleAsIs());
        assertThat(
                        restored.resolveSchemaCompatibility(
                                new ChangeStreamMutationSerializer.Snapshot("future-sdk")))
                .matches(compatibility -> compatibility.isIncompatible());
    }

    @Test
    void rejectsAnUnknownSnapshotFormat() {
        ChangeStreamMutationSerializer.Snapshot snapshot =
                new ChangeStreamMutationSerializer.Snapshot();

        assertThatThrownBy(
                        () ->
                                snapshot.readSnapshot(
                                        2,
                                        new DataInputDeserializer(new byte[0]),
                                        getClass().getClassLoader()))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("snapshot version 2");
    }
}
