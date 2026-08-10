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

package io.github.flink.gcp.connector.bigquery.source.split;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BigQueryReadStreamSplitSerializerTest {

    private static final String STREAM = "projects/p/locations/l/sessions/s/streams/one";
    private static final String SCHEMA =
            "{\"type\":\"record\",\"name\":\"Row\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}";

    private static final Instant EXPIRE_TIME = Instant.parse("2026-08-09T18:00:00.123456789Z");

    private final BigQueryReadStreamSplitSerializer serializer =
            new BigQueryReadStreamSplitSerializer();

    @Test
    void roundTripsASplit() throws Exception {
        BigQueryReadStreamSplit split =
                new BigQueryReadStreamSplit(STREAM, 42, SCHEMA, EXPIRE_TIME);

        BigQueryReadStreamSplit restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(split));

        assertThat(restored).isEqualTo(split);
        assertThat(restored.getOffset()).isEqualTo(42);
        assertThat(restored.getAvroSchemaJson()).isEqualTo(SCHEMA);
        assertThat(restored.getSessionExpireTime()).isEqualTo(EXPIRE_TIME);
    }

    @Test
    void roundTripsASplitWithoutASessionExpiry() throws Exception {
        BigQueryReadStreamSplit split = new BigQueryReadStreamSplit(STREAM, 42, SCHEMA, null);

        BigQueryReadStreamSplit restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(split));

        assertThat(restored).isEqualTo(split);
        assertThat(restored.getSessionExpireTime()).isNull();
    }

    @Test
    void readsAVersionOneSplitAsOneWithoutASessionExpiry() throws Exception {
        // Version 1 is the same layout with nothing after the schema. Written by hand rather than
        // by keeping the old serializer, so what this asserts against is the format itself.
        DataOutputSerializer out = new DataOutputSerializer(1024);
        out.writeUTF(STREAM);
        out.writeLong(42);
        byte[] schema = SCHEMA.getBytes(StandardCharsets.UTF_8);
        out.writeInt(schema.length);
        out.write(schema);

        BigQueryReadStreamSplit restored =
                serializer.deserialize(
                        BigQueryReadStreamSplitSerializer.VERSION_WITHOUT_EXPIRY,
                        out.getCopyOfBuffer());

        assertThat(restored.getStreamName()).isEqualTo(STREAM);
        assertThat(restored.getOffset()).isEqualTo(42);
        assertThat(restored.getAvroSchemaJson()).isEqualTo(SCHEMA);
        assertThat(restored.getSessionExpireTime()).isNull();
    }

    @Test
    void writesTheFieldsInTheOrderTheLayoutDefines() throws Exception {
        // Pins the layout rather than the bytes: a field added in the middle would round-trip fine
        // and still break a state written by an older job. The expiry is appended, which is what
        // lets a version 1 payload be read by stopping where it stops.
        byte[] bytes =
                serializer.serialize(new BigQueryReadStreamSplit(STREAM, 42, SCHEMA, EXPIRE_TIME));

        DataInputDeserializer in = new DataInputDeserializer(bytes);
        assertThat(in.readUTF()).isEqualTo(STREAM);
        assertThat(in.readLong()).isEqualTo(42);
        int schemaLength = in.readInt();
        byte[] schema = new byte[schemaLength];
        in.readFully(schema);
        assertThat(new String(schema, StandardCharsets.UTF_8)).isEqualTo(SCHEMA);
        assertThat(in.readBoolean()).isTrue();
        assertThat(in.readLong()).isEqualTo(EXPIRE_TIME.getEpochSecond());
        assertThat(in.readInt()).isEqualTo(EXPIRE_TIME.getNano());
        assertThat(in.available()).isZero();
    }

    @Test
    void carriesASchemaLongerThanModifiedUtf8Allows() throws Exception {
        // A wide table's schema exceeds writeUTF's 65535-byte limit, and a job with such a table
        // would be the only thing to find out.
        StringBuilder fields = new StringBuilder();
        for (int i = 0; i < 4000; i++) {
            fields.append(i > 0 ? "," : "")
                    .append("{\"name\":\"column_")
                    .append(i)
                    .append("\",\"type\":\"long\"}");
        }
        String wide = "{\"type\":\"record\",\"name\":\"Row\",\"fields\":[" + fields + "]}";
        assertThat(wide.getBytes(StandardCharsets.UTF_8).length).isGreaterThan(65535);
        BigQueryReadStreamSplit split = new BigQueryReadStreamSplit(STREAM, 0, wide, EXPIRE_TIME);

        assertThat(serializer.deserialize(serializer.getVersion(), serializer.serialize(split)))
                .isEqualTo(split);
    }

    @Test
    void rejectsAnUnknownVersionNamingBoth() throws Exception {
        byte[] bytes = serializer.serialize(new BigQueryReadStreamSplit(STREAM, 0, SCHEMA, null));

        assertThatThrownBy(() -> serializer.deserialize(99, bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("99")
                .hasMessageContaining("version 2");
    }
}
