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

package io.github.flink.gcp.connector.pubsub.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

import io.github.flink.gcp.connector.pubsub.sink.CreateDisposition;

import java.time.Duration;

/**
 * The {@code WITH} options of the {@code pubsub} table connector.
 *
 * <p>Each option corresponds to exactly one setter on {@link
 * io.github.flink.gcp.connector.pubsub.sink.PubSubSinkBuilder} or {@link
 * io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions.Builder}: the DataStream API is
 * the source of truth and this layer only maps onto it. There is deliberately no {@code
 * properties.*} passthrough — the connector's option objects take plain values rather than SDK
 * types, so a typed option exists for every knob and an untyped escape hatch would only reintroduce
 * the SDK surface the programmatic API keeps out.
 *
 * <p>Almost every option is declared without a default, and the factory applies it with {@code
 * getOptional(...).ifPresent(...)}. "Absent from the DDL" then means "left at the connector's or
 * the SDK's default", with no third state to invent and no default duplicated between here and the
 * options object it feeds.
 *
 * <p>Byte-valued options are {@link MemorySize}, so they can be written as {@code 64mb}. The value
 * is converted to a {@code long} where it is applied and never reaches the connector's public API.
 */
@PublicEvolving
public final class PubSubConnectorOptions {

    // ------------------------------------------------------------------------
    //  Shared
    // ------------------------------------------------------------------------

    public static final ConfigOption<String> PROJECT =
            ConfigOptions.key("project")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Google Cloud project owning the topic or subscription. Topic and"
                                    + " subscription names are resolved against it, so both are"
                                    + " given as bare names rather than full resource paths.");

    public static final ConfigOption<String> EMULATOR_ENDPOINT =
            ConfigOptions.key("emulator-endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Host and port of a Pub/Sub emulator to use instead of the service."
                                    + " Setting it switches the connector to a plaintext channel"
                                    + " with no credentials. For tests only.");

    // ------------------------------------------------------------------------
    //  Sink — destination
    // ------------------------------------------------------------------------

    public static final ConfigOption<String> TOPIC =
            ConfigOptions.key("topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The topic to publish to, resolved against 'project'. Required when the"
                                    + " table is written to.");

    public static final ConfigOption<CreateDisposition> SINK_CREATE_DISPOSITION =
            ConfigOptions.key("sink.create-disposition")
                    .enumType(CreateDisposition.class)
                    .noDefaultValue()
                    .withDescription(
                            "Whether the sink may create the topic when it does not exist."
                                    + " Defaults to create-if-needed.");

    // ------------------------------------------------------------------------
    //  Sink — publisher batching
    // ------------------------------------------------------------------------

    public static final ConfigOption<Long> SINK_BATCHING_ELEMENT_COUNT_THRESHOLD =
            ConfigOptions.key("sink.batching.element-count-threshold")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "How many messages a publisher batches into one publish request.");

    public static final ConfigOption<MemorySize> SINK_BATCHING_REQUEST_BYTE_THRESHOLD =
            ConfigOptions.key("sink.batching.request-byte-threshold")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "How many bytes a publisher batches into one publish request.");

    public static final ConfigOption<Duration> SINK_BATCHING_DELAY_THRESHOLD =
            ConfigOptions.key("sink.batching.delay-threshold")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "How long a publisher waits for a batch to fill before sending it.");

    // ------------------------------------------------------------------------
    //  Sink — publish retries
    // ------------------------------------------------------------------------

    public static final ConfigOption<Duration> SINK_RETRY_TOTAL_TIMEOUT =
            ConfigOptions.key("sink.retry.total-timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The total time budget of a publish including its retries.");

    public static final ConfigOption<Duration> SINK_RETRY_INITIAL_DELAY =
            ConfigOptions.key("sink.retry.initial-delay")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The delay before the first publish retry.");

    public static final ConfigOption<Double> SINK_RETRY_DELAY_MULTIPLIER =
            ConfigOptions.key("sink.retry.delay-multiplier")
                    .doubleType()
                    .noDefaultValue()
                    .withDescription(
                            "The factor the retry delay grows by per attempt, at least 1.0.");

    public static final ConfigOption<Duration> SINK_RETRY_MAX_DELAY =
            ConfigOptions.key("sink.retry.max-delay")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The cap on the delay between publish retries.");

    public static final ConfigOption<Duration> SINK_RETRY_INITIAL_RPC_TIMEOUT =
            ConfigOptions.key("sink.retry.initial-rpc-timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The timeout of the first publish RPC attempt.");

    public static final ConfigOption<Double> SINK_RETRY_RPC_TIMEOUT_MULTIPLIER =
            ConfigOptions.key("sink.retry.rpc-timeout-multiplier")
                    .doubleType()
                    .noDefaultValue()
                    .withDescription(
                            "The factor the per-RPC timeout grows by per attempt, at least 1.0.");

    public static final ConfigOption<Duration> SINK_RETRY_MAX_RPC_TIMEOUT =
            ConfigOptions.key("sink.retry.max-rpc-timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The cap on the timeout of a publish RPC attempt.");

    public static final ConfigOption<Integer> SINK_RETRY_MAX_ATTEMPTS =
            ConfigOptions.key("sink.retry.max-attempts")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The cap on publish attempts. 0 bounds retries by the total timeout"
                                    + " alone, which is also the default behavior.");

    // ------------------------------------------------------------------------
    //  Sink — ordering, in-flight caps and auto-creation recovery
    // ------------------------------------------------------------------------

    public static final ConfigOption<Boolean> SINK_MESSAGE_ORDERING_ENABLED =
            ConfigOptions.key("sink.message-ordering.enabled")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Whether publishers honor message ordering keys. Defaults to false, and"
                                    + " must be true for a table that writes the 'ordering-key'"
                                    + " metadata column.");

    public static final ConfigOption<Integer> SINK_IN_FLIGHT_MAX_MESSAGES =
            ConfigOptions.key("sink.in-flight.max-messages")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The cap on unacknowledged publishes per sink subtask. Defaults to"
                                    + " 1000.");

    public static final ConfigOption<MemorySize> SINK_IN_FLIGHT_MAX_BYTES =
            ConfigOptions.key("sink.in-flight.max-bytes")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "The cap on the serialized bytes of unacknowledged publishes per sink"
                                    + " subtask. Defaults to 64 mb.");

    public static final ConfigOption<Duration> SINK_RECOVERY_INITIAL_BACKOFF =
            ConfigOptions.key("sink.recovery.initial-backoff")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The first backoff of the topic auto-creation recovery. Defaults to"
                                    + " 500 ms.");

    public static final ConfigOption<Duration> SINK_RECOVERY_MAX_BACKOFF =
            ConfigOptions.key("sink.recovery.max-backoff")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The cap on the backoff of the topic auto-creation recovery. Defaults"
                                    + " to 10 s.");

    public static final ConfigOption<Integer> SINK_RECOVERY_MAX_ATTEMPTS =
            ConfigOptions.key("sink.recovery.max-attempts")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The cap on republish attempts of the topic auto-creation recovery."
                                    + " Defaults to 10.");

    private PubSubConnectorOptions() {}
}
