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
    final RecoveryOptions options =
            new RecoveryOptions("run-1", 2, 1, "initial", false, "datastream");

    @ParameterizedTest
    @ValueSource(
            strings = {
                "--unknown=a",
                "--parallelism=4",
                "--parallelism=0",
                "--restore-deduplication=yes",
                "--replay-delay-millis=-1",
                "--replay-delay-millis=1001",
                "--max-outstanding-messages=0",
                "--max-outstanding-messages=100001",
                "--hold-after-checkpoint-millis=-1",
                "--hold-after-checkpoint-millis=600001",
                "--records-per-subscription=0",
                "--records-per-subscription=10001",
                "--phase=upgrade",
                "--require-restored=yes",
                "--run-id=x",
                "--phase=wrong",
                "--entry-point=sql",
                "--entry-point=Table"
            })
    void rejectsInvalidOrRepeatedArguments(String arg) {
        assertThatThrownBy(() -> RecoveryOptions.parse(new String[] {"--run-id=run-1", arg}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void entryPointDefaultsToDataStream() {
        assertThat(RecoveryOptions.parse(new String[] {"--run-id=run-1"}).entryPoint)
                .isEqualTo("datastream");
        assertThat(
                        RecoveryOptions.parse(
                                        new String[] {"--run-id=run-1", "--entry-point=table"})
                                .entryPoint)
                .isEqualTo("table");
        assertThatThrownBy(
                        () ->
                                RecoveryOptions.parse(
                                        new String[] {
                                            "--run-id=run-1",
                                            "--entry-point=table",
                                            "--entry-point=table"
                                        }))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void stableNamesAndIdentitySurviveSerializationAndRescaling() throws Exception {
        RecoveryOptions copy = InstantiationUtil.clone(options);
        assertThat(copy.input(0)).isEqualTo("t3-run-1-in-0");
        assertThat(copy.output()).isEqualTo("t3-run-1-out");
        RecoveryOptions restored =
                new RecoveryOptions("run-1", 2, 2, "upgrade", true, "datastream");
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
                                        new RecoveryOptions(
                                                "run-1", 3, 1, "initial", false, "datastream"),
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
    void tagComparesTheFullSubscriptionPathTheTableMetadataCarries() throws Exception {
        ByteString data = ByteString.copyFromUtf8(RecoveryPayload.input(options, 1, 0));
        String path = "projects/flink-gcp/subscriptions/t3-run-1-in-1";
        assertThat(RecoveryPayload.tag(options, data, "m", path))
                .isEqualTo("v1|run-1|1|0|" + RecoveryPayload.encode("m"));
        assertThatThrownBy(() -> RecoveryPayload.tag(options, data, "m", "t3-run-1-in-1"))
                .hasMessageContaining("subscription");
        assertThatThrownBy(
                        () ->
                                RecoveryPayload.tag(
                                        options,
                                        data,
                                        "m",
                                        "projects/flink-gcp/subscriptions/t3-run-1-in-0"))
                .hasMessageContaining("subscription");
        assertThatThrownBy(() -> RecoveryPayload.tag(options, null, "m", path))
                .hasMessageContaining("bounded UTF-8");
        assertThatThrownBy(() -> RecoveryPayload.tag(options, data, null, path))
                .hasMessageContaining("message ID");
        assertThatThrownBy(
                        () ->
                                RecoveryPayload.tag(
                                        options,
                                        ByteString.copyFrom(new byte[] {(byte) 0xff}),
                                        "m",
                                        path))
                .hasMessageContaining("bounded UTF-8");
        assertThatThrownBy(
                        () ->
                                RecoveryPayload.tag(
                                        options,
                                        ByteString.copyFromUtf8("v".repeat(129)),
                                        "m",
                                        path))
                .hasMessageContaining("bounded UTF-8");
    }

    @Test
    void eachProcessingCallHasItsOwnIdWithoutChangingLogicalAndServiceIdentity() {
        String input = "v1|run-1|0|0|" + RecoveryPayload.encode("input-1");
        String attempt = UUID.randomUUID().toString();
        String first = RecoveryPayload.observation(options, input, attempt, false, 0, 2, 1, 0);
        String second = RecoveryPayload.observation(options, input, attempt, false, 0, 2, 1, 0);
        assertThat(first)
                .startsWith(input + "|" + attempt + "|")
                .endsWith("|initial|false|0|2|1|0")
                .isNotEqualTo(second);
    }

    @Test
    void theReplayCohortIsTheMiddleThirdOfEachInput() {
        RecoveryOptions ten = new RecoveryOptions("run-1", 10, 1, "initial", false, "datastream");
        String tagged = "|" + RecoveryPayload.encode("m");
        assertThat(ten.inReplayCohort(RecoveryPayload.input(ten, 1, 2) + tagged)).isFalse();
        assertThat(ten.inReplayCohort(RecoveryPayload.input(ten, 1, 3) + tagged)).isTrue();
        assertThat(ten.inReplayCohort(RecoveryPayload.input(ten, 0, 5) + tagged)).isTrue();
        assertThat(ten.inReplayCohort(RecoveryPayload.input(ten, 0, 6) + tagged)).isFalse();
    }

    @Test
    void parsesTheDeduplicationTrialArguments() {
        RecoveryOptions parsed =
                RecoveryOptions.parse(
                        new String[] {
                            "--run-id=run-1",
                            "--parallelism=3",
                            "--restore-deduplication=true",
                            "--replay-delay-millis=20",
                            "--max-outstanding-messages=100000",
                            "--hold-after-checkpoint-millis=15000"
                        });
        assertThat(parsed.parallelism).isEqualTo(3);
        assertThat(parsed.restoreDeduplication).isTrue();
        assertThat(parsed.replayDelayMillis).isEqualTo(20);
        assertThat(parsed.maxOutstandingMessages).isEqualTo(100000);
        assertThat(parsed.holdAfterCheckpointMillis).isEqualTo(15000);
        RecoveryOptions defaults = RecoveryOptions.parse(new String[] {"--run-id=run-1"});
        assertThat(defaults.restoreDeduplication).isFalse();
        assertThat(defaults.replayDelayMillis).isZero();
        assertThat(defaults.maxOutstandingMessages).isEqualTo(1000);
        assertThat(defaults.holdAfterCheckpointMillis).isZero();
    }

    @Test
    void holdsOnlyTheFirstReplayRecordAfterEachSnapshot() {
        RecoveryOptions held =
                RecoveryOptions.parse(
                        new String[] {
                            "--run-id=run-1",
                            "--records-per-subscription=9",
                            "--hold-after-checkpoint-millis=15000"
                        });
        String tagged = "|" + RecoveryPayload.encode("message");
        String before = RecoveryPayload.input(held, 0, 2) + tagged;
        String replay = RecoveryPayload.input(held, 0, 3) + tagged;
        String nextReplay = RecoveryPayload.input(held, 1, 4) + tagged;
        RecoveryObserver observer = new RecoveryObserver(held);

        // Nothing is held before a snapshot.
        assertThat(observer.holdFor(replay)).isZero();
        observer.checkpointTaken(1);
        // A record outside the replay cohort neither holds nor uses up the hold.
        assertThat(observer.holdFor(before)).isZero();
        assertThat(observer.holdFor(replay)).isEqualTo(15000);
        assertThat(observer.holdFor(nextReplay)).isZero();
        // A notification that runs before any replay record disarms the hold, so no hold is
        // logged for a checkpoint whose notification already ran.
        observer.checkpointTaken(2);
        observer.notifyCheckpointComplete(2);
        assertThat(observer.holdFor(nextReplay)).isZero();
        // An earlier checkpoint's notification leaves a later snapshot's hold armed.
        observer.checkpointTaken(3);
        observer.notifyCheckpointComplete(2);
        assertThat(observer.holdFor(nextReplay)).isEqualTo(15000);
        // The next snapshot arms it again.
        observer.checkpointTaken(4);
        assertThat(observer.holdFor(nextReplay)).isEqualTo(15000);
    }

    @Test
    void holdsNothingWithoutAHoldOrAfterARestore() {
        String tagged = "|" + RecoveryPayload.encode("message");
        RecoveryOptions unheld =
                RecoveryOptions.parse(
                        new String[] {"--run-id=run-1", "--records-per-subscription=9"});
        RecoveryObserver plain = new RecoveryObserver(unheld);
        plain.checkpointTaken(1);
        assertThat(plain.holdFor(RecoveryPayload.input(unheld, 0, 3) + tagged)).isZero();

        RecoveryOptions held =
                RecoveryOptions.parse(
                        new String[] {
                            "--run-id=run-1",
                            "--records-per-subscription=9",
                            "--hold-after-checkpoint-millis=15000"
                        });
        RecoveryObserver restored = new RecoveryObserver(held);
        restored.markRestored();
        restored.checkpointTaken(5);
        assertThat(restored.holdFor(RecoveryPayload.input(held, 0, 3) + tagged)).isZero();
    }
}
