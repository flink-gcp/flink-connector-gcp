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

package io.github.flink.gcp.connector.pubsub.sink;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FailedMessage}. */
class FailedMessageTest {

    private static final TopicDestination TOPIC = TopicDestination.of("my-project", "my-topic");

    private static final PubsubMessage MESSAGE =
            PubsubMessage.newBuilder()
                    .setData(ByteString.copyFromUtf8("payload"))
                    .putAttributes("source", "test")
                    .setOrderingKey("key-1")
                    .build();

    @Test
    void carriesTheDestinationMessageAndCause() {
        IOException cause = new IOException("rejected");
        FailedMessage failed = FailedMessage.of(TOPIC, MESSAGE, "boom", cause);

        assertThat(failed.getDestination()).isEqualTo(TOPIC);
        assertThat(failed.getPubsubMessage()).isEqualTo(MESSAGE);
        assertThat(failed.getErrorMessage()).isEqualTo("boom");
        assertThat(failed.getCause()).isSameAs(cause);
    }

    @Test
    void reportsTheConnectorAndTheTopicResourceName() {
        FailedMessage failed = FailedMessage.of(TOPIC, MESSAGE, "boom", null);

        assertThat(failed.getConnector()).isEqualTo("pubsub");
        // The resource name, not the "project/topic" toString: dead-letter consumers key on it.
        assertThat(failed.describeDestination()).isEqualTo("projects/my-project/topics/my-topic");
        assertThat(failed.getCause()).isNull();
    }

    @Test
    void thePayloadBytesAreTheWholeMessageSoAttributesSurvive() throws Exception {
        FailedMessage failed = FailedMessage.of(TOPIC, MESSAGE, "boom", null);

        ByteString bytes = failed.getPayloadBytes();
        assertThat(bytes).isNotNull();
        // Not message.getData(): a dead-letter consumer must be able to recover the attributes and
        // the ordering key, which only the serialized message carries.
        assertThat(PubsubMessage.parseFrom(bytes)).isEqualTo(MESSAGE);
    }

    @Test
    void aSerializationFailureCarriesNoMessageAndNoPayload() {
        FailedMessage failed = FailedMessage.of(TOPIC, null, "cannot serialize", null);

        assertThat(failed.getPubsubMessage()).isNull();
        assertThat(failed.getPayloadBytes()).isNull();
        assertThat(failed.toString()).contains("message=null");
    }

    @Test
    void rejectsAMissingDestinationOrErrorMessage() {
        assertThatThrownBy(() -> FailedMessage.of(null, MESSAGE, "boom", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("destination must not be null");
        assertThatThrownBy(() -> FailedMessage.of(TOPIC, MESSAGE, null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("errorMessage must not be null");
    }

    @Test
    void toStringNamesTheDestinationAndTheError() {
        assertThat(FailedMessage.of(TOPIC, MESSAGE, "boom", null).toString())
                .contains("my-project/my-topic")
                .contains("boom")
                .contains(MESSAGE.getSerializedSize() + " bytes");
    }
}
