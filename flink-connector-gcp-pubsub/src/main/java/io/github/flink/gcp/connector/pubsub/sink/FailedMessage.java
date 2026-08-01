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

package io.github.flink.gcp.connector.pubsub.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.failure.FailureHandler;

import javax.annotation.Nullable;

/**
 * A single message that terminally failed to be published to Pub/Sub, as passed to a {@link
 * FailureHandler FailureHandler&lt;FailedMessage&gt;}.
 *
 * <p>Carries the {@link PubsubMessage} the serializer produced rather than the original record: the
 * sink writer is stateless and retains only serialized messages, so by the time a publish is
 * rejected the original record object no longer exists. When serialization itself failed, {@link
 * #getPubsubMessage()} is {@code null}.
 *
 * <p>{@link #getPayloadBytes()} is the <em>whole</em> serialized message, not just its data, so the
 * attributes and the ordering key survive a dead-letter round trip: a consumer recovers them with
 * {@code PubsubMessage.parseFrom(bytes)}.
 *
 * <p>Instances are created by the sink and are not serializable.
 */
@PublicEvolving
public final class FailedMessage implements FailedElement {

    private final TopicDestination destination;
    @Nullable private final PubsubMessage message;
    private final String errorMessage;
    @Nullable private final Throwable cause;

    private FailedMessage(
            TopicDestination destination,
            @Nullable PubsubMessage message,
            String errorMessage,
            @Nullable Throwable cause) {
        this.destination = Preconditions.checkNotNull(destination, "destination must not be null");
        this.message = message;
        this.errorMessage =
                Preconditions.checkNotNull(errorMessage, "errorMessage must not be null");
        this.cause = cause;
    }

    /**
     * Creates a failed message. Intended for the sink implementation (and tests of custom
     * handlers).
     *
     * @param destination the topic the message was routed to
     * @param message the serialized message, or {@code null} when serialization itself failed
     * @param errorMessage the failure description
     * @param cause the underlying failure, or {@code null}
     * @return the failed message
     */
    public static FailedMessage of(
            TopicDestination destination,
            @Nullable PubsubMessage message,
            String errorMessage,
            @Nullable Throwable cause) {
        return new FailedMessage(destination, message, errorMessage, cause);
    }

    /** Returns the topic the message was routed to. */
    public TopicDestination getDestination() {
        return destination;
    }

    /**
     * Returns the message the serializer produced, or {@code null} when the record could not be
     * serialized in the first place.
     */
    @Nullable
    public PubsubMessage getPubsubMessage() {
        return message;
    }

    @Override
    public String getConnector() {
        return "pubsub";
    }

    /** Returns the topic in the {@code projects/<p>/topics/<t>} form. */
    @Override
    public String describeDestination() {
        return destination.toTopicPath();
    }

    /**
     * Returns the serialized {@link PubsubMessage} — payload, attributes and ordering key alike —
     * or {@code null} when serialization itself failed.
     */
    @Override
    @Nullable
    public ByteString getPayloadBytes() {
        return message == null ? null : message.toByteString();
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
        return "FailedMessage{destination="
                + destination
                + ", message="
                + (message == null ? "null" : message.getSerializedSize() + " bytes")
                + ", errorMessage="
                + errorMessage
                + "}";
    }
}
