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

package io.github.flink.gcp.connector.bigtable.source.changestream.reader;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import io.github.flink.gcp.connector.bigtable.source.serializer.ChangeStreamMutationDeserializationSchema;
import io.github.flink.gcp.connector.bigtable.source.serializer.ChangeStreamMutationSerializer;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class ChangeStreamMutationSerializerTest {

    @Test
    void theBuiltInSchemaCopiesAndRoundTripsSdkMutationsWithoutReflectiveKryo() throws Exception {
        ChangeStreamMutation mutation =
                TestChangeStreamRecords.mutation(
                        Instant.parse("2026-08-12T00:00:00Z"),
                        Instant.parse("2026-08-11T23:59:00Z"),
                        "token");
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
        assertThat(restored.getCommitTime()).isEqualTo(mutation.getCommitTime());
        assertThat(restored.getEstimatedLowWatermarkTime())
                .isEqualTo(mutation.getEstimatedLowWatermarkTime());
        assertThat(restored.getToken()).isEqualTo("token");
        assertThat(restored.getEntries())
                .singleElement()
                .isInstanceOfSatisfying(
                        DeleteFamily.class,
                        deleteFamily ->
                                assertThat(deleteFamily.getFamilyName()).isEqualTo("family"));
    }

    @Test
    void theBuiltInTypeDoesNotEqualReflectiveGenericTypeInformation() {
        TypeInformation<ChangeStreamMutation> builtIn =
                new ChangeStreamMutationDeserializationSchema().getProducedType();
        TypeInformation<ChangeStreamMutation> generic =
                TypeInformation.of(ChangeStreamMutation.class);

        assertThat(builtIn).isNotEqualTo(generic);
        assertThat(generic).isNotEqualTo(builtIn);
    }
}
