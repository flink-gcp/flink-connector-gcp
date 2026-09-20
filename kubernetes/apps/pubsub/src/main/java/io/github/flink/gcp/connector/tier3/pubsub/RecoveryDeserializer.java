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

package io.github.flink.gcp.connector.tier3.pubsub;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.util.Collector;

import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;

import java.io.IOException;

/**
 * Preserves service message identity and rejects input delivered through the wrong subscription.
 */
@Internal
final class RecoveryDeserializer implements PubSubDeserializationSchema<String> {
    private static final long serialVersionUID = 1L;
    private final RecoveryOptions options;

    RecoveryDeserializer(RecoveryOptions options) {
        this.options = options;
    }

    @Override
    public void deserialize(
            PubsubMessage message, SubscriptionDestination subscription, Collector<String> out)
            throws IOException {
        if (!message.getData().isValidUtf8()
                || message.getData().size() > 128
                || message.getMessageId().isEmpty()) {
            throw new IOException("Expected bounded UTF-8 input with a service message ID");
        }
        String text = message.getData().toStringUtf8();
        int[] id = RecoveryPayload.parseInput(options, text);
        if (!subscription.equals(
                SubscriptionDestination.of(RecoveryOptions.PROJECT, options.input(id[0])))) {
            throw new IOException("Payload input index differs from its subscription");
        }
        out.collect(text + "|" + RecoveryPayload.encode(message.getMessageId()));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return Types.STRING;
    }
}
