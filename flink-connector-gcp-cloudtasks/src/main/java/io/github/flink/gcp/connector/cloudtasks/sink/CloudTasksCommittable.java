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
import org.apache.flink.util.StringUtils;

import com.google.cloud.tasks.v2.HttpMethod;
import com.google.cloud.tasks.v2.Task;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.util.Objects;

/**
 * One immutable checkpoint committable. Task bytes include the name and are never regenerated. The
 * collector owns recovery; this value is not writer state.
 */
@Internal
public final class CloudTasksCommittable {
    /** Conservative decimal interpretation of the CreateTask reference's 100 KB limit. */
    public static final int MAX_TASK_BYTES = 100_000;

    /** Per-envelope allowance used by the writer's accounting, not a measured heap bound. */
    public static final int ACCOUNTING_OVERHEAD_BYTES = 256;

    private final String queuePath;
    private final long originEpochMillis;
    private final long authorizationDeadlineMillis;
    private final ByteString taskBytes;

    CloudTasksCommittable(
            String queuePath,
            long originEpochMillis,
            long authorizationDeadlineMillis,
            ByteString taskBytes)
            throws IOException {
        validateQueuePath(queuePath);
        if (authorizationDeadlineMillis <= originEpochMillis) {
            throw new IOException(
                    "Invalid Cloud Tasks envelope: authorization deadline must follow origin.");
        }
        if (taskBytes == null || taskBytes.isEmpty() || taskBytes.size() > MAX_TASK_BYTES) {
            throw new IOException(
                    "Invalid Cloud Tasks envelope: task wire size must be 1.."
                            + MAX_TASK_BYTES
                            + " bytes.");
        }
        this.queuePath = queuePath;
        this.originEpochMillis = originEpochMillis;
        this.authorizationDeadlineMillis = authorizationDeadlineMillis;
        this.taskBytes = taskBytes;
    }

    /** Validates a named task before allocating its persisted wire representation. */
    public static CloudTasksCommittable fromTask(
            String queuePath, long originEpochMillis, long authorizationDeadlineMillis, Task task)
            throws IOException {
        validateQueuePath(queuePath);
        validateTask(queuePath, task);
        return new CloudTasksCommittable(
                queuePath, originEpochMillis, authorizationDeadlineMillis, task.toByteString());
    }

    static void validateQueuePath(String queuePath) throws IOException {
        if (queuePath == null || queuePath.length() > MAX_TASK_BYTES) {
            throw new IOException("Invalid Cloud Tasks envelope queue path length.");
        }
        String[] parts = queuePath.split("/", -1);
        if (parts.length != 6
                || !parts[0].equals("projects")
                || !parts[2].equals("locations")
                || !parts[4].equals("queues")) {
            throw new IOException("Invalid Cloud Tasks envelope queue path structure.");
        }
        try {
            QueueDestination.of(parts[1], parts[3], parts[5]);
        } catch (IllegalArgumentException | NullPointerException e) {
            throw new IOException("Invalid Cloud Tasks envelope queue path components.");
        }
        // Reject ill-formed UTF-16 before protobuf or UTF-8 can replace a surrogate.
        for (int i = 0; i < queuePath.length(); i++) {
            char c = queuePath.charAt(i);
            if (Character.isHighSurrogate(c)) {
                if (++i == queuePath.length() || !Character.isLowSurrogate(queuePath.charAt(i))) {
                    throw new IOException("Invalid Cloud Tasks envelope queue path encoding.");
                }
            } else if (Character.isLowSurrogate(c)) {
                throw new IOException("Invalid Cloud Tasks envelope queue path encoding.");
            }
        }
    }

    private static void validateTask(String queuePath, Task task) throws IOException {
        if (task == null
                || task.getSerializedSize() <= 0
                || task.getSerializedSize() > MAX_TASK_BYTES) {
            throw new IOException(
                    "Cloud Tasks task exceeds the staging wire size limit of "
                            + MAX_TASK_BYTES
                            + " bytes (including its name).");
        }
        String prefix = queuePath + "/tasks/";
        String name = task.getName();
        if (!name.startsWith(prefix)) {
            throw new IOException(
                    "Invalid Cloud Tasks envelope: task name must address its queue.");
        }
        String id = name.substring(prefix.length());
        if ((id.length() != 32 && id.length() != 64)
                || !id.chars().allMatch(c -> c >= '0' && c <= '9' || c >= 'a' && c <= 'f')) {
            throw new IOException(
                    "Invalid Cloud Tasks envelope: task identity must be 32 or 64 lowercase hex characters.");
        }
        // Match the constraints our target serializers already own. Never normalize staged bytes.
        try {
            if (task.hasHttpRequest()) {
                var request = task.getHttpRequest();
                if (request.hasOidcToken()
                                && StringUtils.isNullOrWhitespaceOnly(
                                        request.getOidcToken().getServiceAccountEmail())
                        || request.hasOauthToken()
                                && StringUtils.isNullOrWhitespaceOnly(
                                        request.getOauthToken().getServiceAccountEmail())) {
                    throw new IllegalArgumentException();
                }
                if (!(request.getUrl().startsWith("http://")
                                || request.getUrl().startsWith("https://"))
                        || request.getHttpMethod() == HttpMethod.HTTP_METHOD_UNSPECIFIED
                        || request.getHttpMethod() == HttpMethod.UNRECOGNIZED) {
                    throw new IllegalArgumentException();
                }
                if (!request.getBody().isEmpty()
                        && request.getHttpMethod() != HttpMethod.POST
                        && request.getHttpMethod() != HttpMethod.PUT
                        && request.getHttpMethod() != HttpMethod.PATCH) {
                    throw new IllegalArgumentException();
                }
            } else if (task.hasAppEngineHttpRequest()) {
                var request = task.getAppEngineHttpRequest();
                AppEngineTargetChecks.checkRelativeUri(request.getRelativeUri(), "relativeUri");
                AppEngineTargetChecks.checkAndNormalizeRouting(
                        request.getAppEngineRouting(), "routing");
                for (String nameKey : request.getHeadersMap().keySet()) {
                    AppEngineTargetChecks.checkHeaderName(nameKey);
                }
                if (request.getHttpMethod() == HttpMethod.HTTP_METHOD_UNSPECIFIED
                        || request.getHttpMethod() == HttpMethod.UNRECOGNIZED
                        || !request.getBody().isEmpty()
                                && request.getHttpMethod() != HttpMethod.POST
                                && request.getHttpMethod() != HttpMethod.PUT) {
                    throw new IllegalArgumentException();
                }
            } else {
                throw new IllegalArgumentException();
            }
        } catch (IllegalArgumentException | NullPointerException e) {
            // Existing field validators include values in their messages. State failures must not.
            throw new IOException("Cloud Tasks task violates the serializer target constraints.");
        }
    }

    /** Returns the resolved queue path. */
    public String getQueuePath() {
        return queuePath;
    }

    /** Returns the original staging wall-clock instant. */
    public long getOriginEpochMillis() {
        return originEpochMillis;
    }

    /** Returns the durable authorization deadline, which recovery must never loosen. */
    public long getAuthorizationDeadlineMillis() {
        return authorizationDeadlineMillis;
    }

    /** Returns immutable, complete named Task wire bytes. */
    public ByteString getTaskBytes() {
        return taskBytes;
    }

    /** Returns wire bytes plus the writer's fixed per-envelope accounting allowance. */
    public long getAccountedBytes() {
        return (long) taskBytes.size() + ACCOUNTING_OVERHEAD_BYTES;
    }

    /** Parses and validates the persisted task at commit, without rebuilding or renaming it. */
    public Task parseTask() throws IOException {
        final Task task;
        try {
            task = Task.parseFrom(taskBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new IOException("Invalid Cloud Tasks envelope Task encoding.");
        }
        validateTask(queuePath, task);
        return task;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof CloudTasksCommittable)) {
            return false;
        }
        CloudTasksCommittable that = (CloudTasksCommittable) other;
        return originEpochMillis == that.originEpochMillis
                && authorizationDeadlineMillis == that.authorizationDeadlineMillis
                && queuePath.equals(that.queuePath)
                && taskBytes.equals(that.taskBytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queuePath, originEpochMillis, authorizationDeadlineMillis, taskBytes);
    }

    @Override
    public String toString() {
        return "CloudTasksCommittable{taskBytes="
                + taskBytes.size()
                + ", originEpochMillis="
                + originEpochMillis
                + ", authorizationDeadlineMillis="
                + authorizationDeadlineMillis
                + "}";
    }
}
