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

package io.github.flink.gcp.connector.cloudtasks.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32C;

/**
 * Bounded v1 framing for one envelope; the Flink collector owns batch counts and redistribution.
 */
@Internal
public final class CloudTasksCommittableSerializer
        implements SimpleVersionedSerializer<CloudTasksCommittable> {
    private static final int VERSION = 1;
    private static final int MAGIC = 0x43545345;
    private static final int FIXED_BYTES = 32;
    private static final int MAX_FRAME_BYTES =
            FIXED_BYTES + 2 * CloudTasksCommittable.MAX_TASK_BYTES;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(CloudTasksCommittable envelope) throws IOException {
        CloudTasksCommittable.validateQueuePath(envelope.getQueuePath());
        byte[] queue = envelope.getQueuePath().getBytes(StandardCharsets.UTF_8);
        if (queue.length > CloudTasksCommittable.MAX_TASK_BYTES) {
            throw new IOException("Cloud Tasks envelope queue encoding exceeds the state limit.");
        }
        ByteString task = envelope.getTaskBytes();
        ByteBuffer out = ByteBuffer.allocate(FIXED_BYTES + queue.length + task.size());
        out.putInt(MAGIC).putInt(queue.length).put(queue);
        out.putLong(envelope.getOriginEpochMillis())
                .putLong(envelope.getAuthorizationDeadlineMillis());
        out.putInt(task.size());
        task.copyTo(out);
        CRC32C checksum = new CRC32C();
        checksum.update(out.array(), 0, out.position());
        out.putInt((int) checksum.getValue());
        return out.array();
    }

    @Override
    public CloudTasksCommittable deserialize(int version, byte[] bytes) throws IOException {
        if (version != VERSION) {
            throw new IOException(
                    "Unsupported Cloud Tasks envelope version "
                            + version
                            + "; supported version is "
                            + VERSION
                            + ".");
        }
        if (bytes == null || bytes.length < FIXED_BYTES || bytes.length > MAX_FRAME_BYTES) {
            throw new IOException(
                    "Invalid Cloud Tasks envelope frame size; maximum is "
                            + MAX_FRAME_BYTES
                            + " bytes.");
        }
        ByteBuffer in = ByteBuffer.wrap(bytes);
        if (in.getInt() != MAGIC) {
            throw new IOException("Invalid Cloud Tasks envelope magic.");
        }
        CRC32C checksum = new CRC32C();
        checksum.update(bytes, 0, bytes.length - Integer.BYTES);
        if ((int) checksum.getValue() != in.getInt(bytes.length - Integer.BYTES)) {
            throw new IOException("Invalid Cloud Tasks envelope checksum.");
        }
        int queueLength = in.getInt();
        if (queueLength <= 0
                || queueLength > CloudTasksCommittable.MAX_TASK_BYTES
                || queueLength > in.remaining() - 24) {
            throw new IOException("Invalid Cloud Tasks envelope queue length.");
        }
        final String queue;
        try {
            queue =
                    StandardCharsets.UTF_8
                            .newDecoder()
                            .onMalformedInput(CodingErrorAction.REPORT)
                            .onUnmappableCharacter(CodingErrorAction.REPORT)
                            .decode(ByteBuffer.wrap(bytes, in.position(), queueLength))
                            .toString();
        } catch (CharacterCodingException e) {
            throw new IOException("Invalid Cloud Tasks envelope queue encoding.");
        }
        in.position(in.position() + queueLength);
        long origin = in.getLong();
        long deadline = in.getLong();
        int taskLength = in.getInt();
        if (taskLength <= 0
                || taskLength > CloudTasksCommittable.MAX_TASK_BYTES
                || taskLength != in.remaining() - Integer.BYTES) {
            throw new IOException("Invalid Cloud Tasks envelope task length or trailing bytes.");
        }
        return new CloudTasksCommittable(
                queue, origin, deadline, ByteString.copyFrom(bytes, in.position(), taskLength));
    }
}
