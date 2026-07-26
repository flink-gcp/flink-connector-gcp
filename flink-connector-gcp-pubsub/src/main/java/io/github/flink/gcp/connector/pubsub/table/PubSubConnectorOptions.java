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
import io.github.flink.gcp.connector.pubsub.source.DeserializationFailurePolicy;
import io.github.flink.gcp.connector.pubsub.source.OrderingMode;

import java.time.Duration;
import java.util.List;

/**
 * The {@code WITH} options of the {@code pubsub} table connector.
 *
 * <p>Each option corresponds to exactly one setter on {@link
 * io.github.flink.gcp.connector.pubsub.sink.PubSubSinkBuilder}, {@link
 * io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions.Builder}, {@link
 * io.github.flink.gcp.connector.pubsub.source.PubSubSourceBuilder} or {@link
 * io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions.Builder}: the DataStream API
 * is the source of truth and this layer only maps onto it. There is deliberately no {@code
 * properties.*} passthrough — the connector's option objects take plain values rather than SDK
 * types, so a typed option exists for every knob and an untyped escape hatch would only reintroduce
 * the SDK surface the programmatic API keeps out.
 *
 * <p>Every option is declared without a default, and the factory applies it with {@code
 * getOptional(...).ifPresent(...)}. "Absent from the DDL" then means "left at the connector's or
 * the SDK's default", with no third state to invent. No default value is restated in a description
 * either: the options object it feeds is the one place a default is written, and a copy here is a
 * copy nothing checks.
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
    //  Source — subscriptions
    // ------------------------------------------------------------------------

    public static final ConfigOption<List<String>> SUBSCRIPTION =
            ConfigOptions.key("subscription")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "The subscriptions to consume, resolved against 'project' and separated"
                                    + " by ';'. Required when the table is read from. A subscription"
                                    + " in another project cannot be named here.");

    public static final ConfigOption<OrderingMode> SCAN_ORDERING_MODE =
            ConfigOptions.key("scan.ordering-mode")
                    .enumType(OrderingMode.class)
                    .noDefaultValue()
                    .withDescription(
                            "Whether the source preserves per-ordering-key delivery order. 'per-key'"
                                    + " pins each subscription to one subtask, so it caps the"
                                    + " effective source parallelism at the subscription count.");

    public static final ConfigOption<DeserializationFailurePolicy>
            SCAN_DESERIALIZATION_FAILURE_POLICY =
                    ConfigOptions.key("scan.deserialization-failure-policy")
                            .enumType(DeserializationFailurePolicy.class)
                            .noDefaultValue()
                            .withDescription(
                                    "What to do with a message the format cannot decode: 'fail' the"
                                            + " job, 'drop' the message, or 'nack' it for the"
                                            + " subscription's dead-letter policy to deal with.");

    // ------------------------------------------------------------------------
    //  Source — subscriber tuning
    // ------------------------------------------------------------------------

    public static final ConfigOption<Long> SCAN_FLOW_CONTROL_MAX_OUTSTANDING_ELEMENT_COUNT =
            ConfigOptions.key("scan.flow-control.max-outstanding-element-count")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "How many messages the subscriber keeps outstanding before pausing the"
                                    + " stream.");

    public static final ConfigOption<MemorySize> SCAN_FLOW_CONTROL_MAX_OUTSTANDING_REQUEST_BYTES =
            ConfigOptions.key("scan.flow-control.max-outstanding-request-bytes")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "How many bytes of messages the subscriber keeps outstanding before"
                                    + " pausing the stream.");

    public static final ConfigOption<Integer> SCAN_PARALLEL_PULL_COUNT =
            ConfigOptions.key("scan.parallel-pull-count")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "How many streaming-pull connections each subscriber opens. Rejected"
                                    + " above 1 with 'scan.ordering-mode' = 'per-key', which needs"
                                    + " a single connection to preserve order.");

    public static final ConfigOption<Duration> SCAN_ACK_MAX_EXTENSION_PERIOD =
            ConfigOptions.key("scan.ack.max-extension-period")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "How long the subscriber keeps extending a message's acknowledgement"
                                    + " deadline. It has to outlast the checkpoint interval, since"
                                    + " a completing checkpoint is what acknowledges.");

    public static final ConfigOption<Duration> SCAN_ACK_MIN_DURATION_PER_EXTENSION =
            ConfigOptions.key("scan.ack.min-duration-per-extension")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The shortest acknowledgement deadline extension to request.");

    public static final ConfigOption<Duration> SCAN_ACK_MAX_DURATION_PER_EXTENSION =
            ConfigOptions.key("scan.ack.max-duration-per-extension")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The longest acknowledgement deadline extension to request.");

    public static final ConfigOption<Duration> SCAN_ACK_AWAIT_CONFIRMATION =
            ConfigOptions.key("scan.ack.await-confirmation")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "How long a completing checkpoint waits for the service to confirm its"
                                    + " acknowledgements. Unset means it does not wait.");

    public static final ConfigOption<Duration> SCAN_SHUTDOWN_TIMEOUT =
            ConfigOptions.key("scan.shutdown-timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("How long closing a reader waits for its subscriber to stop.");

    public static final ConfigOption<Integer> SCAN_MAX_RECORDS_PER_FETCH =
            ConfigOptions.key("scan.max-records-per-fetch")
                    .intType()
                    .noDefaultValue()
                    .withDescription("How many messages one fetch drains from a split's buffer.");

    public static final ConfigOption<Duration> SCAN_FIRST_CHECKPOINT_TIMEOUT =
            ConfigOptions.key("scan.first-checkpoint-timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "How long a reader holding unacknowledged messages waits for its first"
                                    + " checkpoint before failing, which is how a job running"
                                    + " without checkpointing is caught. Zero disables the check.");

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
                            "Whether the sink may create the topic when it does not exist.");

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
                            "The cap on publish attempts. 0 means the retries are bounded by the"
                                    + " total timeout alone.");

    // ------------------------------------------------------------------------
    //  Sink — ordering, in-flight caps and auto-creation recovery
    // ------------------------------------------------------------------------

    public static final ConfigOption<Boolean> SINK_MESSAGE_ORDERING_ENABLED =
            ConfigOptions.key("sink.message-ordering.enabled")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Whether publishers honor message ordering keys. Must be true for a"
                                    + " table that writes the 'ordering-key' metadata column.");

    public static final ConfigOption<Integer> SINK_IN_FLIGHT_MAX_MESSAGES =
            ConfigOptions.key("sink.in-flight.max-messages")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The cap on unacknowledged publishes per sink subtask.");

    public static final ConfigOption<MemorySize> SINK_IN_FLIGHT_MAX_BYTES =
            ConfigOptions.key("sink.in-flight.max-bytes")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "The cap on the serialized bytes of unacknowledged publishes per sink"
                                    + " subtask.");

    public static final ConfigOption<Duration> SINK_RECOVERY_INITIAL_BACKOFF =
            ConfigOptions.key("sink.recovery.initial-backoff")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The first backoff of the topic auto-creation recovery.");

    public static final ConfigOption<Duration> SINK_RECOVERY_MAX_BACKOFF =
            ConfigOptions.key("sink.recovery.max-backoff")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The cap on the backoff of the topic auto-creation recovery.");

    public static final ConfigOption<Integer> SINK_RECOVERY_MAX_ATTEMPTS =
            ConfigOptions.key("sink.recovery.max-attempts")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The cap on republish attempts of the topic auto-creation recovery.");

    private PubSubConnectorOptions() {}
}
