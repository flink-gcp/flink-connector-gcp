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

package io.github.flink.gcp.connector.pubsub.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

import io.github.flink.gcp.connector.pubsub.sink.CreateDisposition;
import io.github.flink.gcp.connector.pubsub.source.DeserializationFailurePolicy;
import io.github.flink.gcp.connector.pubsub.source.OrderingMode;
import io.github.flink.gcp.connector.pubsub.source.StartPosition;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * The {@code WITH} options of the {@code pubsub} table connector.
 *
 * <p>Each option corresponds to a setter — one each, but for the three exceptions below — on {@link
 * io.github.flink.gcp.connector.pubsub.sink.PubSubSinkBuilder}, {@link
 * io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions.Builder}, {@link
 * io.github.flink.gcp.connector.pubsub.sink.TopicCreateOptions.Builder}, {@link
 * io.github.flink.gcp.connector.pubsub.source.PubSubSourceBuilder}, {@link
 * io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions.Builder} or {@link
 * io.github.flink.gcp.connector.pubsub.source.SubscriptionCreateOptions.Builder}: the DataStream
 * API is the source of truth and this layer only maps onto it. There is deliberately no {@code
 * properties.*} passthrough — the connector's option objects take plain values rather than SDK
 * types, so a typed option exists for every knob and an untyped escape hatch would only reintroduce
 * the SDK surface the programmatic API keeps out.
 *
 * <p>Three setters are not one option each, because a {@code ConfigOption} cannot take their shape.
 * {@code startPosition(...)} takes a mode and, for one mode, an instant, so it is the {@code
 * scan.startup.mode} and {@code scan.startup.timestamp-millis} pair Kafka also uses. {@code
 * neverExpire()} takes no argument and contradicts {@code expirationTtl(...)}, so it is a boolean
 * beside the duration and setting both is rejected. {@code deadLetterPolicy(...)} takes two
 * arguments, so it is two options that are required together. The mappers under {@code
 * table.source} hold those rules; nothing else invents a value.
 *
 * <p>Every option is declared without a default, and the factory applies it with {@code
 * getOptional(...).ifPresent(...)}. "Absent from the DDL" then means "left at the connector's or
 * the SDK's default", with no third state to invent. No default value is restated in a description
 * either — including one derived from another option rather than declared: {@code
 * reference/pubsub.md} carries the derivation and the resolved value, and the option table's "Maps
 * to" column is the pointer there. A test rejects the default-restating phrases found in this file;
 * it is a regression guard over those forms, not a semantic parser for arbitrary prose.
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

    public static final ConfigOption<String> SERVICE_ACCOUNT_KEY_FILE =
            ConfigOptions.key("service-account-key-file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Path to a service-account JSON key file readable from every eligible"
                                    + " runtime process: sink writers on task managers, or a source"
                                    + " enumerator on the job manager and readers on task managers."
                                    + " Cannot be combined with emulator-endpoint.");

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

    public static final ConfigOption<Long> SCAN_PAUSED_SPLIT_BUFFER_MAX_MESSAGES =
            ConfigOptions.key("scan.paused-split-buffer.max-messages")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "How many messages a split paused by watermark alignment may buffer"
                                    + " before its subscriber is stopped, to be reopened when the"
                                    + " split resumes. Lowering it can park a split while a"
                                    + " checkpoint covering its output is still in flight, which"
                                    + " re-emits those records on resume.");

    public static final ConfigOption<MemorySize> SCAN_PAUSED_SPLIT_BUFFER_MAX_BYTES =
            ConfigOptions.key("scan.paused-split-buffer.max-bytes")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "The same in bytes, applied together with the message cap: whichever is"
                                    + " exceeded first stops the subscriber.");

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
                                    + " acknowledgements.");

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
    //  Source — start position
    // ------------------------------------------------------------------------

    public static final ConfigOption<StartPosition.Mode> SCAN_STARTUP_MODE =
            ConfigOptions.key("scan.startup.mode")
                    .enumType(StartPosition.Mode.class)
                    .noDefaultValue()
                    .withDescription(
                            "Where the source starts consuming. Everything but"
                                    + " 'continue-from-subscription' seeks, which rewrites"
                                    + " subscription state shared by every consumer including other"
                                    + " jobs, and which runs once at a job's first start and never"
                                    + " on a restore. Use it only on a subscription the job owns.");

    public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP_MILLIS =
            ConfigOptions.key("scan.startup.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "The publish time to start from, in milliseconds since the epoch."
                                    + " Required by 'scan.startup.mode' = 'timestamp' and rejected"
                                    + " with every other mode.");

    // ------------------------------------------------------------------------
    //  Source — subscription auto-creation
    // ------------------------------------------------------------------------

    public static final ConfigOption<Map<String, String>> SCAN_AUTO_CREATE_TOPICS =
            ConfigOptions.key("scan.auto-create.topics")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "The existing topic to bind each missing subscription to, as a map from"
                                    + " bare subscription name to bare topic name, both resolved"
                                    + " against 'project'. The map keys must exactly match"
                                    + " 'subscription'. Setting it authorizes creating missing"
                                    + " subscriptions; without it every subscription must already"
                                    + " exist. Only subscriptions are created — a source never"
                                    + " creates a topic, unlike 'sink.create-disposition'. Other"
                                    + " 'scan.auto-create.*' settings apply when each mapped"
                                    + " subscription is missing and must be created."
                                    + " Prefer one prefixed option per binding, for example"
                                    + " 'scan.auto-create.topics.orders-sub' = 'orders'. The packed"
                                    + " form separates entries with ',' and each key from its value"
                                    + " with ':'.");

    public static final ConfigOption<Duration> SCAN_AUTO_CREATE_ACK_DEADLINE =
            ConfigOptions.key("scan.auto-create.ack-deadline")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "How long a created subscription waits for a message to be"
                                    + " acknowledged before redelivering it. A whole number of"
                                    + " seconds.");

    public static final ConfigOption<Boolean> SCAN_AUTO_CREATE_MESSAGE_ORDERING_ENABLED =
            ConfigOptions.key("scan.auto-create.message-ordering.enabled")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Whether a created subscription delivers messages of one ordering key"
                                    + " in order. Required by 'scan.ordering-mode' = 'per-key', and"
                                    + " fixed at creation.");

    public static final ConfigOption<Duration> SCAN_AUTO_CREATE_MESSAGE_RETENTION =
            ConfigOptions.key("scan.auto-create.message-retention")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "How long a created subscription retains an unacknowledged message.");

    public static final ConfigOption<Boolean> SCAN_AUTO_CREATE_RETAIN_ACKED_MESSAGES =
            ConfigOptions.key("scan.auto-create.retain-acked-messages")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Whether a created subscription keeps acknowledged messages within its"
                                    + " retention window, which is what makes a backwards seek"
                                    + " replay them.");

    public static final ConfigOption<Duration> SCAN_AUTO_CREATE_EXPIRATION_TTL =
            ConfigOptions.key("scan.auto-create.expiration-ttl")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "How long a created subscription may sit inactive before Pub/Sub"
                                    + " deletes it. Cannot be combined with"
                                    + " 'scan.auto-create.never-expire'.");

    public static final ConfigOption<Boolean> SCAN_AUTO_CREATE_NEVER_EXPIRE =
            ConfigOptions.key("scan.auto-create.never-expire")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Whether a created subscription never expires, however long it sits"
                                    + " inactive. Cannot be combined with"
                                    + " 'scan.auto-create.expiration-ttl'.");

    public static final ConfigOption<String> SCAN_AUTO_CREATE_DEAD_LETTER_TOPIC =
            ConfigOptions.key("scan.auto-create.dead-letter.topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The existing topic a created subscription forwards undeliverable"
                                    + " messages to, resolved against 'project'. Required with"
                                    + " 'scan.auto-create.dead-letter.max-delivery-attempts'."
                                    + " Pub/Sub also needs its own service account granted publish"
                                    + " on that topic and subscribe on this subscription, or it"
                                    + " silently keeps redelivering.");

    public static final ConfigOption<Integer> SCAN_AUTO_CREATE_DEAD_LETTER_MAX_DELIVERY_ATTEMPTS =
            ConfigOptions.key("scan.auto-create.dead-letter.max-delivery-attempts")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "How many times a created subscription delivers a message before"
                                    + " forwarding it to the dead-letter topic. Required together"
                                    + " with 'scan.auto-create.dead-letter.topic'. Deliveries are"
                                    + " counted, not causes, so a redelivery after a job restart"
                                    + " raises the same counter a nack does.");

    public static final ConfigOption<String> SCAN_AUTO_CREATE_FILTER =
            ConfigOptions.key("scan.auto-create.filter")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The expression a created subscription filters its topic's messages"
                                    + " with. Fixed at creation.");

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
    //  Sink — topic auto-creation settings
    // ------------------------------------------------------------------------

    public static final ConfigOption<Duration> SINK_AUTO_CREATE_MESSAGE_RETENTION =
            ConfigOptions.key("sink.auto-create.message-retention")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "How long a created topic retains published messages, acknowledged or"
                                    + " not. Without it a message survives only as long as some"
                                    + " subscription's own retention covers it, so a subscription"
                                    + " created later — or a backwards seek — cannot reach it.");

    public static final ConfigOption<String> SINK_AUTO_CREATE_KMS_KEY_NAME =
            ConfigOptions.key("sink.auto-create.kms-key-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Cloud KMS key a created topic encrypts messages with"
                                    + " (customer-managed encryption), as a full key resource name."
                                    + " The key must exist and the Pub/Sub service account needs"
                                    + " encrypt/decrypt on it, or publishes to the created topic"
                                    + " fail.");

    public static final ConfigOption<List<String>> SINK_AUTO_CREATE_STORAGE_POLICY_ALLOWED_REGIONS =
            ConfigOptions.key("sink.auto-create.storage-policy.allowed-regions")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "The Cloud regions a created topic may persist messages in (its message"
                                    + " storage policy). Without it the project's organization"
                                    + " policy decides.");

    public static final ConfigOption<Boolean> SINK_AUTO_CREATE_STORAGE_POLICY_ENFORCE_IN_TRANSIT =
            ConfigOptions.key("sink.auto-create.storage-policy.enforce-in-transit")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Whether a created topic also rejects publishes travelling through"
                                    + " regions outside the allowed ones, instead of only"
                                    + " restricting where messages are stored. Requires"
                                    + " 'sink.auto-create.storage-policy.allowed-regions'.");

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
                    .withDescription(
                            "The total time budget of a publish including its"
                                    + " retries. Cannot be combined with"
                                    + " 'sink.message-ordering.enabled' = 'true'.");

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
                                    + " total timeout alone. Cannot be combined with"
                                    + " 'sink.message-ordering.enabled' = 'true'.");

    // ------------------------------------------------------------------------
    //  Sink — ordering, in-flight caps and auto-creation recovery
    // ------------------------------------------------------------------------

    public static final ConfigOption<Boolean> SINK_MESSAGE_ORDERING_ENABLED =
            ConfigOptions.key("sink.message-ordering.enabled")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Whether publishers honor message ordering keys. Must be true for a"
                                    + " table that writes the 'ordering-key' metadata column. An"
                                    + " ordering-enabled publisher retries without limit, so"
                                    + " 'sink.retry.total-timeout' and 'sink.retry.max-attempts'"
                                    + " cannot be set alongside it.");

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

    public static final ConfigOption<Integer> SINK_MAX_CONSECUTIVE_REJECTIONS =
            ConfigOptions.key("sink.max-consecutive-rejections")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "How many consecutive confirmed rejections fail the job under a"
                                    + " dropping failure handler; any successful publish resets the"
                                    + " count, and -1 removes the bound.");

    public static final ConfigOption<Duration> SINK_PUBLISH_PROGRESS_TIMEOUT =
            ConfigOptions.key("sink.publish-progress-timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "How long the sink may wait with no publish completing before it fails."
                                    + " The budget restarts at every completion, so it bounds a"
                                    + " publisher that has stopped answering rather than a slow topic."
                                    + " It covers both waits the sink makes on the task thread, the"
                                    + " in-flight admission gate and the checkpoint drain. With"
                                    + " 'sink.message-ordering.enabled' the SDK retries a publish"
                                    + " without limit, so nothing inside the sink ends such an"
                                    + " outage but this.");

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

    public static final ConfigOption<Duration> SINK_SHUTDOWN_TIMEOUT =
            ConfigOptions.key("sink.shutdown-timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "How long the sink's close waits for one publisher to shut down. The"
                                    + " budget is measured from the moment the publisher is asked"
                                    + " to stop, and every publisher is asked before any is waited"
                                    + " on, so a close costs one such timeout however many topics"
                                    + " were written to.");

    public static final ConfigOption<Boolean> SINK_METRICS_PER_DESTINATION =
            ConfigOptions.key("sink.metrics.per-destination")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Whether the sink registers per-topic send counters beside its totals."
                                    + " Flink cannot unregister a metric, so with dynamic"
                                    + " destinations every topic the job writes to keeps a row in"
                                    + " the metric registry for the lifetime of the task.");

    private PubSubConnectorOptions() {}
}
