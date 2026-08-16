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

import org.apache.flink.annotation.Public;
import org.apache.flink.util.Preconditions;

import com.google.cloud.tasks.v2.Task;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.failure.FailureHandler;

import javax.annotation.Nullable;

/**
 * A single task that terminally failed to be created in Cloud Tasks, as passed to a {@link
 * FailureHandler FailureHandler&lt;FailedTask&gt;}.
 *
 * <p>Carries the {@link Task} the serializer produced rather than the original record: the sink
 * writer is stateless and retains only serialized tasks, so by the time a creation is rejected the
 * original record object no longer exists. When serialization itself failed, {@link #getTask()} is
 * {@code null}.
 *
 * <p>{@link #getPayloadBytes()} is the <em>whole</em> serialized task, not just its HTTP body, so
 * the target URL, the method, the headers and the authorization survive a dead-letter round trip: a
 * consumer recovers them with {@code Task.parseFrom(bytes)}. The task carries no name unless {@code
 * taskIdExtractor(...)} is set, in which case it holds the hashed one the sink composed.
 *
 * <p>Instances are created by the sink and are not serializable.
 */
@Public
public final class FailedTask implements FailedElement {

    private final QueueDestination destination;
    @Nullable private final Task task;
    private final String errorMessage;
    @Nullable private final Throwable cause;

    private FailedTask(
            QueueDestination destination,
            @Nullable Task task,
            String errorMessage,
            @Nullable Throwable cause) {
        this.destination = Preconditions.checkNotNull(destination, "destination must not be null");
        this.task = task;
        this.errorMessage =
                Preconditions.checkNotNull(errorMessage, "errorMessage must not be null");
        this.cause = cause;
    }

    /**
     * Creates a failed task. Intended for the sink implementation (and tests of custom handlers).
     *
     * @param destination the queue the task was routed to
     * @param task the serialized task, or {@code null} when serialization itself failed
     * @param errorMessage the failure description
     * @param cause the underlying failure, or {@code null}
     * @return the failed task
     */
    public static FailedTask of(
            QueueDestination destination,
            @Nullable Task task,
            String errorMessage,
            @Nullable Throwable cause) {
        return new FailedTask(destination, task, errorMessage, cause);
    }

    /** Returns the queue the task was routed to. */
    public QueueDestination getDestination() {
        return destination;
    }

    /**
     * Returns the task the serializer produced, or {@code null} when the record could not be
     * serialized in the first place.
     */
    @Nullable
    public Task getTask() {
        return task;
    }

    @Override
    public String getConnector() {
        return "cloudtasks";
    }

    /**
     * Returns the queue in the {@code projects/<project>/locations/<location>/queues/<queue>} form.
     */
    @Override
    public String describeDestination() {
        return destination.toQueuePath();
    }

    /**
     * Returns the serialized {@link Task} — target, body, headers and authorization alike — or
     * {@code null} when serialization itself failed.
     */
    @Override
    @Nullable
    public ByteString getPayloadBytes() {
        return task == null ? null : task.toByteString();
    }

    @Override
    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    @Nullable
    public Throwable getCause() {
        return cause;
    }

    @Override
    public String toString() {
        return "FailedTask{destination="
                + destination
                + ", task="
                + (task == null ? "null" : task.getSerializedSize() + " bytes")
                + ", errorMessage="
                + errorMessage
                + "}";
    }
}
