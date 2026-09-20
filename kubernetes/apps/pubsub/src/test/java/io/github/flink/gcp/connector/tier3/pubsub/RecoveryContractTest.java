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

import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecoveryContractTest {
    final RecoveryOptions options = new RecoveryOptions("run-1", 2, 1, "initial", false);

    @ParameterizedTest
    @ValueSource(
            strings = {
                "--unknown=a",
                "--parallelism=3",
                "--records-per-subscription=0",
                "--records-per-subscription=10001",
                "--phase=upgrade",
                "--require-restored=yes",
                "--run-id=x",
                "--phase=wrong"
            })
    void rejectsInvalidOrRepeatedArguments(String arg) {
        assertThatThrownBy(() -> RecoveryOptions.parse(new String[] {"--run-id=run-1", arg}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void stableNamesAndIdentitySurviveSerializationAndRescaling() throws Exception {
        RecoveryOptions copy = InstantiationUtil.clone(options);
        assertThat(copy.input(0)).isEqualTo("t3-run-1-in-0");
        assertThat(copy.output()).isEqualTo("t3-run-1-out");
        RecoveryOptions restored = new RecoveryOptions("run-1", 2, 2, "upgrade", true);
        RecoveryObserver.verifyIdentity(restored, true, List.of(copy.identity(), copy.identity()));
        assertThatThrownBy(() -> RecoveryObserver.verifyIdentity(restored, false, List.of()))
                .hasMessageContaining("requires checkpoint");
        assertThatThrownBy(() -> RecoveryObserver.verifyIdentity(restored, true, List.of()))
                .hasMessageContaining("missing");
        assertThatThrownBy(
                        () ->
                                RecoveryObserver.verifyIdentity(
                                        restored, true, List.of(copy.identity(), "foreign")))
                .hasMessageContaining("another run");
        assertThatThrownBy(
                        () ->
                                RecoveryObserver.verifyIdentity(
                                        new RecoveryOptions("run-1", 3, 1, "initial", false),
                                        true,
                                        List.of(copy.identity())))
                .hasMessageContaining("another run");
    }

    @Test
    void deserializerPreservesIdentityAndRejectsWrongSubscriptionAndDomain() throws Exception {
        List<String> output = new ArrayList<>();
        Collector<String> collector =
                new Collector<>() {
                    @Override
                    public void collect(String value) {
                        output.add(value);
                    }

                    @Override
                    public void close() {}
                };
        RecoveryDeserializer schema = new RecoveryDeserializer(options);
        PubsubMessage message =
                PubsubMessage.newBuilder()
                        .setMessageId("opaque/id")
                        .setData(ByteString.copyFromUtf8(RecoveryPayload.input(options, 0, 1)))
                        .build();
        schema.deserialize(
                message,
                SubscriptionDestination.of(RecoveryOptions.PROJECT, options.input(0)),
                collector);
        assertThat(output).containsExactly("v1|run-1|0|1|" + RecoveryPayload.encode("opaque/id"));
        assertThatThrownBy(
                        () ->
                                schema.deserialize(
                                        message,
                                        SubscriptionDestination.of(
                                                RecoveryOptions.PROJECT, options.input(1)),
                                        collector))
                .hasMessageContaining("subscription");
        assertThatThrownBy(
                        () ->
                                schema.deserialize(
                                        message.toBuilder().clearMessageId().build(),
                                        SubscriptionDestination.of(
                                                RecoveryOptions.PROJECT, options.input(0)),
                                        collector))
                .hasMessageContaining("message ID");
        assertThatThrownBy(() -> RecoveryPayload.parseInput(options, "v1|run-1|0|2"))
                .hasMessageContaining("domain");
        assertThatThrownBy(() -> RecoveryPayload.parseInput(options, "v1|run-2|0|1"))
                .hasMessageContaining("run");
        assertThatThrownBy(() -> RecoveryPayload.parseInput(options, "v1|run-1|0|01"))
                .hasMessageContaining("canonical");
    }

    @Test
    void eachProcessingCallHasItsOwnIdWithoutChangingLogicalAndServiceIdentity() {
        String input = "v1|run-1|0|0|" + RecoveryPayload.encode("input-1");
        String attempt = UUID.randomUUID().toString();
        String first = RecoveryPayload.observation(options, input, attempt, false);
        String second = RecoveryPayload.observation(options, input, attempt, false);
        assertThat(first)
                .startsWith(input + "|" + attempt + "|")
                .endsWith("|initial|false")
                .isNotEqualTo(second);
    }
}
