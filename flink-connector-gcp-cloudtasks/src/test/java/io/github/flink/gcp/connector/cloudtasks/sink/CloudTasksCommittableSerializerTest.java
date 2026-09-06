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

import com.google.cloud.tasks.v2.HttpMethod;
import com.google.cloud.tasks.v2.HttpRequest;
import com.google.cloud.tasks.v2.OAuthToken;
import com.google.cloud.tasks.v2.OidcToken;
import com.google.cloud.tasks.v2.Task;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.CRC32C;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CloudTasksCommittableSerializerTest {
    static final String QUEUE = "projects/p/locations/l/queues/q";
    private final CloudTasksCommittableSerializer serializer =
            new CloudTasksCommittableSerializer();

    static Task task(String body) {
        return Task.newBuilder()
                .setName(QUEUE + "/tasks/" + "a".repeat(32))
                .setHttpRequest(
                        HttpRequest.newBuilder()
                                .setUrl("https://example.com/task")
                                .setHttpMethod(HttpMethod.POST)
                                .setBody(ByteString.copyFromUtf8(body))
                                .putHeaders("Authorization", "secret-header"))
                .build();
    }

    static CloudTasksCommittable envelope() throws IOException {
        return CloudTasksCommittable.fromTask(QUEUE, 1234, 9876, task("secret-payload"));
    }

    @Test
    void preservesEveryFieldIncludingUnknownTaskFields() throws Exception {
        Task original =
                task("body").toBuilder()
                        .setUnknownFields(
                                UnknownFieldSet.newBuilder()
                                        .addField(
                                                123,
                                                UnknownFieldSet.Field.newBuilder()
                                                        .addVarint(77)
                                                        .build())
                                        .build())
                        .build();
        var envelope = CloudTasksCommittable.fromTask(QUEUE, 10, 20, original);
        byte[] encoded = serializer.serialize(envelope);
        var restored = serializer.deserialize(1, encoded);
        assertThat(restored).isEqualTo(envelope).hasSameHashCodeAs(envelope);
        assertThat(restored.getQueuePath()).isEqualTo(QUEUE);
        assertThat(restored.getOriginEpochMillis()).isEqualTo(10);
        assertThat(restored.getAuthorizationDeadlineMillis()).isEqualTo(20);
        assertThat(restored.getTaskBytes()).isEqualTo(original.toByteString());
        assertThat(restored.parseTask()).isEqualTo(original);
        assertThat(serializer.serialize(restored)).containsExactly(encoded);
        Arrays.fill(encoded, (byte) 0);
        assertThat(restored.getTaskBytes()).isEqualTo(original.toByteString());
        assertThat(restored.toString()).doesNotContain("secret", "Authorization", "example.com");
    }

    @Test
    void framingHasAStableMagicAndFieldOrder() throws Exception {
        byte[] encoded = serializer.serialize(envelope());
        ByteBuffer frame = ByteBuffer.wrap(encoded);
        assertThat(serializer.getVersion()).isEqualTo(1);
        assertThat(frame.getInt()).isEqualTo(0x43545345);
        int length = frame.getInt();
        byte[] queue = new byte[length];
        frame.get(queue);
        assertThat(new String(queue, StandardCharsets.UTF_8)).isEqualTo(QUEUE);
        assertThat(frame.getLong()).isEqualTo(1234);
        assertThat(frame.getLong()).isEqualTo(9876);
        assertThat(frame.getInt()).isEqualTo(task("secret-payload").getSerializedSize());
        assertThat(encoded.length)
                .isEqualTo(32 + length + task("secret-payload").getSerializedSize());
    }

    @Test
    void validDispatchTokensRetainTheirCompleteWireFields() throws Exception {
        for (boolean oidc : new boolean[] {true, false}) {
            var request = task("body").getHttpRequest().toBuilder();
            if (oidc) {
                request.setOidcToken(
                        OidcToken.newBuilder()
                                .setServiceAccountEmail("worker@example.iam.gserviceaccount.com")
                                .setAudience("https://example.com/audience"));
            } else {
                request.setOauthToken(
                        OAuthToken.newBuilder()
                                .setServiceAccountEmail("worker@example.iam.gserviceaccount.com")
                                .setScope("https://www.googleapis.com/auth/cloud-platform"));
            }
            Task original = task("body").toBuilder().setHttpRequest(request).build();
            var envelope = CloudTasksCommittable.fromTask(QUEUE, 1, 2, original);
            var restored = serializer.deserialize(1, serializer.serialize(envelope));
            assertThat(restored.getTaskBytes()).isEqualTo(original.toByteString());
            assertThat(restored.parseTask()).isEqualTo(original);
        }
    }

    @Test
    void rejectsUnknownVersionAndMagic() throws Exception {
        byte[] encoded = serializer.serialize(envelope());
        assertThatThrownBy(() -> serializer.deserialize(2, encoded))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("version 2")
                .hasMessageContaining("version is 1");
        encoded[0] ^= 1;
        resign(encoded);
        assertThatThrownBy(() -> serializer.deserialize(1, encoded)).hasMessageContaining("magic");
    }

    @Test
    void rejectsEveryTruncationAndEverySingleByteCorruption() throws Exception {
        byte[] encoded = serializer.serialize(envelope());
        for (int i = 0; i < encoded.length; i++) {
            byte[] truncated = Arrays.copyOf(encoded, i);
            assertThatThrownBy(() -> serializer.deserialize(1, truncated))
                    .isInstanceOf(IOException.class);
            byte[] corrupted = encoded.clone();
            corrupted[i] ^= 1;
            assertThatThrownBy(() -> serializer.deserialize(1, corrupted))
                    .isInstanceOf(IOException.class);
        }
    }

    @Test
    void rejectsLengthsBeforeAllocatingFromThemEvenWithAValidChecksum() throws Exception {
        for (int invalid : new int[] {Integer.MIN_VALUE, -1, 0, 100_001, Integer.MAX_VALUE}) {
            byte[] queue = serializer.serialize(envelope());
            ByteBuffer.wrap(queue).putInt(4, invalid);
            resign(queue);
            assertThatThrownBy(() -> serializer.deserialize(1, queue))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("queue length");
            byte[] task = serializer.serialize(envelope());
            ByteBuffer.wrap(task).putInt(24 + QUEUE.length(), invalid);
            resign(task);
            assertThatThrownBy(() -> serializer.deserialize(1, task))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("task length");
        }
        byte[] oversized = new byte[200_033];
        assertThatThrownBy(() -> serializer.deserialize(1, oversized))
                .hasMessageContaining("frame size");
        assertThatThrownBy(() -> serializer.deserialize(1, null))
                .hasMessageContaining("frame size");
    }

    @Test
    void rejectsTrailingBytesInvalidEncodingAndInvalidDeadlines() throws Exception {
        byte[] original = serializer.serialize(envelope());
        byte[] trailing = Arrays.copyOf(original, original.length + 1);
        resign(trailing);
        assertThatThrownBy(() -> serializer.deserialize(1, trailing))
                .hasMessageContaining("trailing bytes");
        byte[] encoding = original.clone();
        encoding[8] = (byte) 0xff;
        resign(encoding);
        assertThatThrownBy(() -> serializer.deserialize(1, encoding))
                .hasMessageContaining("encoding");
        byte[] deadline = original.clone();
        ByteBuffer.wrap(deadline).putLong(16 + QUEUE.length(), 1234);
        resign(deadline);
        assertThatThrownBy(() -> serializer.deserialize(1, deadline))
                .hasMessageContaining("deadline");
        byte[] queue = original.clone();
        queue[8] = 'x';
        resign(queue);
        assertThatThrownBy(() -> serializer.deserialize(1, queue))
                .hasMessageContaining("queue path");
    }

    @Test
    void validatesTaskSizeAfterNamingAtTheExactBoundary() throws Exception {
        Task small = task("");
        int bodySize = CloudTasksCommittable.MAX_TASK_BYTES - small.getSerializedSize() - 8;
        Task atLimit = withBodySize(small, bodySize);
        while (atLimit.getSerializedSize() < CloudTasksCommittable.MAX_TASK_BYTES) {
            atLimit = withBodySize(small, ++bodySize);
        }
        assertThat(atLimit.getSerializedSize()).isEqualTo(100_000);
        var envelope = CloudTasksCommittable.fromTask(QUEUE, 1, 2, atLimit);
        assertThat(serializer.deserialize(1, serializer.serialize(envelope))).isEqualTo(envelope);
        Task tooLarge = withBodySize(small, bodySize + 1);
        assertThatThrownBy(() -> CloudTasksCommittable.fromTask(QUEUE, 1, 2, tooLarge))
                .hasMessageContaining("100000");
    }

    @Test
    void rejectsPoisonTaskNamesAndEncodingWithoutDisclosingContents() {
        for (String name :
                new String[] {
                    "", QUEUE + "/tasks/secret", "projects/other/tasks/" + "a".repeat(32)
                }) {
            assertThatThrownBy(
                            () ->
                                    CloudTasksCommittable.fromTask(
                                            QUEUE,
                                            1,
                                            2,
                                            task("secret-payload").toBuilder()
                                                    .setName(name)
                                                    .build()))
                    .isInstanceOf(IOException.class)
                    .hasMessageNotContaining("secret");
        }
        assertThatThrownBy(
                        () ->
                                new CloudTasksCommittable(
                                                QUEUE,
                                                1,
                                                2,
                                                ByteString.copyFrom(new byte[] {(byte) 0xff}))
                                        .parseTask())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("encoding")
                .hasNoCause();
    }

    @Test
    void refusesMalformedQueueAndInvalidTargetsWithoutEchoingValues() {
        assertThatThrownBy(() -> CloudTasksCommittable.fromTask(QUEUE + "\uD800", 1, 2, task("x")))
                .hasMessageContaining("encoding");
        Task invalid =
                task("secret-payload").toBuilder()
                        .setHttpRequest(
                                task("secret-payload").getHttpRequest().toBuilder()
                                        .setUrl("secret-token"))
                        .build();
        assertThatThrownBy(() -> CloudTasksCommittable.fromTask(QUEUE, 1, 2, invalid))
                .hasMessageContaining("target constraints")
                .hasMessageNotContaining("secret")
                .hasNoCause();
    }

    private static Task withBodySize(Task task, int size) {
        return task.toBuilder()
                .setHttpRequest(
                        task.getHttpRequest().toBuilder()
                                .setBody(ByteString.copyFrom(new byte[size])))
                .build();
    }

    private static void resign(byte[] bytes) {
        CRC32C checksum = new CRC32C();
        checksum.update(bytes, 0, bytes.length - 4);
        ByteBuffer.wrap(bytes).putInt(bytes.length - 4, (int) checksum.getValue());
    }
}
