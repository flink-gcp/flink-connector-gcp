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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.table.sink.PubSubDynamicSink;
import io.github.flink.gcp.connector.pubsub.table.sink.PublisherOptionsMapper;
import io.github.flink.gcp.connector.pubsub.table.sink.TopicCreateOptionsMapper;
import io.github.flink.gcp.connector.pubsub.table.source.PubSubDynamicSource;
import io.github.flink.gcp.connector.pubsub.table.source.StartPositionMapper;
import io.github.flink.gcp.connector.pubsub.table.source.SubscriberOptionsMapper;
import io.github.flink.gcp.connector.pubsub.table.source.SubscriptionCreateOptionsMapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The {@code pubsub} table connector factory.
 *
 * <p>Only {@code project} and {@code format} are required. The destination of each direction —
 * {@code topic} for a sink, {@code subscription} for a source — is checked in the {@code create...}
 * method that needs it rather than declared required, because one factory serves both reading and
 * writing and a table used for only one of them must not be forced to configure the other.
 */
@Internal
public class PubSubDynamicTableFactory
        implements DynamicTableSinkFactory, DynamicTableSourceFactory {

    /** The value of {@code 'connector'} that selects this factory. */
    public static final String IDENTIFIER = "pubsub";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>(Arrays.asList(PubSubConnectorOptions.PROJECT, FactoryUtil.FORMAT));
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>(
                Arrays.asList(
                        PubSubConnectorOptions.EMULATOR_ENDPOINT,
                        PubSubConnectorOptions.SERVICE_ACCOUNT_KEY_FILE,
                        PubSubConnectorOptions.SUBSCRIPTION,
                        PubSubConnectorOptions.SCAN_ORDERING_MODE,
                        PubSubConnectorOptions.SCAN_DESERIALIZATION_FAILURE_POLICY,
                        PubSubConnectorOptions.SCAN_FLOW_CONTROL_MAX_OUTSTANDING_ELEMENT_COUNT,
                        PubSubConnectorOptions.SCAN_FLOW_CONTROL_MAX_OUTSTANDING_REQUEST_BYTES,
                        PubSubConnectorOptions.SCAN_SUBSCRIBER_BUFFER_MAX_MESSAGES,
                        PubSubConnectorOptions.SCAN_SUBSCRIBER_BUFFER_MAX_BYTES,
                        PubSubConnectorOptions.SCAN_PAUSED_SPLIT_BUFFER_MAX_MESSAGES,
                        PubSubConnectorOptions.SCAN_PAUSED_SPLIT_BUFFER_MAX_BYTES,
                        PubSubConnectorOptions.SCAN_PARALLEL_PULL_COUNT,
                        PubSubConnectorOptions.SCAN_ACK_MAX_EXTENSION_PERIOD,
                        PubSubConnectorOptions.SCAN_ACK_MIN_DURATION_PER_EXTENSION,
                        PubSubConnectorOptions.SCAN_ACK_MAX_DURATION_PER_EXTENSION,
                        PubSubConnectorOptions.SCAN_ACK_AWAIT_CONFIRMATION,
                        PubSubConnectorOptions.SCAN_SHUTDOWN_TIMEOUT,
                        PubSubConnectorOptions.SCAN_MAX_RECORDS_PER_FETCH,
                        PubSubConnectorOptions.SCAN_FIRST_CHECKPOINT_TIMEOUT,
                        PubSubConnectorOptions.SCAN_STARTUP_MODE,
                        PubSubConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS,
                        PubSubConnectorOptions.SCAN_AUTO_CREATE_TOPICS,
                        PubSubConnectorOptions.SCAN_AUTO_CREATE_ACK_DEADLINE,
                        PubSubConnectorOptions.SCAN_AUTO_CREATE_MESSAGE_ORDERING_ENABLED,
                        PubSubConnectorOptions.SCAN_AUTO_CREATE_MESSAGE_RETENTION,
                        PubSubConnectorOptions.SCAN_AUTO_CREATE_RETAIN_ACKED_MESSAGES,
                        PubSubConnectorOptions.SCAN_AUTO_CREATE_EXPIRATION_TTL,
                        PubSubConnectorOptions.SCAN_AUTO_CREATE_NEVER_EXPIRE,
                        PubSubConnectorOptions.SCAN_AUTO_CREATE_DEAD_LETTER_TOPIC,
                        PubSubConnectorOptions.SCAN_AUTO_CREATE_DEAD_LETTER_MAX_DELIVERY_ATTEMPTS,
                        PubSubConnectorOptions.SCAN_AUTO_CREATE_FILTER,
                        FactoryUtil.SOURCE_PARALLELISM,
                        PubSubConnectorOptions.TOPIC,
                        PubSubConnectorOptions.SINK_CREATE_DISPOSITION,
                        PubSubConnectorOptions.SINK_AUTO_CREATE_MESSAGE_RETENTION,
                        PubSubConnectorOptions.SINK_AUTO_CREATE_KMS_KEY_NAME,
                        PubSubConnectorOptions.SINK_AUTO_CREATE_STORAGE_POLICY_ALLOWED_REGIONS,
                        PubSubConnectorOptions.SINK_AUTO_CREATE_STORAGE_POLICY_ENFORCE_IN_TRANSIT,
                        PubSubConnectorOptions.SINK_BATCHING_ELEMENT_COUNT_THRESHOLD,
                        PubSubConnectorOptions.SINK_BATCHING_REQUEST_BYTE_THRESHOLD,
                        PubSubConnectorOptions.SINK_BATCHING_DELAY_THRESHOLD,
                        PubSubConnectorOptions.SINK_RETRY_TOTAL_TIMEOUT,
                        PubSubConnectorOptions.SINK_RETRY_INITIAL_DELAY,
                        PubSubConnectorOptions.SINK_RETRY_DELAY_MULTIPLIER,
                        PubSubConnectorOptions.SINK_RETRY_MAX_DELAY,
                        PubSubConnectorOptions.SINK_RETRY_INITIAL_RPC_TIMEOUT,
                        PubSubConnectorOptions.SINK_RETRY_RPC_TIMEOUT_MULTIPLIER,
                        PubSubConnectorOptions.SINK_RETRY_MAX_RPC_TIMEOUT,
                        PubSubConnectorOptions.SINK_RETRY_MAX_ATTEMPTS,
                        PubSubConnectorOptions.SINK_MESSAGE_ORDERING_ENABLED,
                        PubSubConnectorOptions.SINK_IN_FLIGHT_MAX_MESSAGES,
                        PubSubConnectorOptions.SINK_IN_FLIGHT_MAX_BYTES,
                        PubSubConnectorOptions.SINK_MAX_CONSECUTIVE_REJECTIONS,
                        PubSubConnectorOptions.SINK_RECOVERY_INITIAL_BACKOFF,
                        PubSubConnectorOptions.SINK_RECOVERY_MAX_BACKOFF,
                        PubSubConnectorOptions.SINK_RECOVERY_MAX_ATTEMPTS,
                        PubSubConnectorOptions.SINK_PUBLISH_PROGRESS_TIMEOUT,
                        PubSubConnectorOptions.SINK_SHUTDOWN_TIMEOUT,
                        PubSubConnectorOptions.SINK_METRICS_PER_DESTINATION,
                        FactoryUtil.SINK_PARALLELISM));
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        EncodingFormat<SerializationSchema<RowData>> encodingFormat =
                helper.discoverEncodingFormat(SerializationFormatFactory.class, FactoryUtil.FORMAT);
        helper.validate();

        ReadableConfig config = helper.getOptions();
        validateCredentialsMode(config);
        String topic =
                config.getOptional(PubSubConnectorOptions.TOPIC)
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                String.format(
                                                        "Option '%s' is required to write to a"
                                                                + " '%s' table.",
                                                        PubSubConnectorOptions.TOPIC.key(),
                                                        IDENTIFIER)));
        // After the check that refuses an option outright; see validateEmulatorEndpoint.
        validateEmulatorEndpoint(config);
        return new PubSubDynamicSink(
                context.getPhysicalRowDataType(),
                encodingFormat,
                TopicDestination.of(config.get(PubSubConnectorOptions.PROJECT), topic),
                config.getOptional(PubSubConnectorOptions.SINK_CREATE_DISPOSITION).orElse(null),
                TopicCreateOptionsMapper.map(config),
                PublisherOptionsMapper.map(config),
                config.getOptional(PubSubConnectorOptions.SERVICE_ACCOUNT_KEY_FILE).orElse(null),
                config.getOptional(PubSubConnectorOptions.EMULATOR_ENDPOINT).orElse(null),
                config.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null));
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        validateAutoCreateTopicsSyntax(context);
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                helper.discoverDecodingFormat(
                        DeserializationFormatFactory.class, FactoryUtil.FORMAT);
        helper.validate();

        ReadableConfig config = helper.getOptions();
        validateCredentialsMode(config);
        List<String> subscriptionNames =
                config.getOptional(PubSubConnectorOptions.SUBSCRIPTION)
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                String.format(
                                                        "Option '%s' is required to read from a"
                                                                + " '%s' table.",
                                                        PubSubConnectorOptions.SUBSCRIPTION.key(),
                                                        IDENTIFIER)));
        // After the check that refuses an option outright; see validateEmulatorEndpoint.
        validateEmulatorEndpoint(config);
        String project = config.get(PubSubConnectorOptions.PROJECT);
        List<SubscriptionDestination> subscriptions = new ArrayList<>(subscriptionNames.size());
        for (String name : subscriptionNames) {
            subscriptions.add(SubscriptionDestination.of(project, name));
        }
        return new PubSubDynamicSource(
                context.getPhysicalRowDataType(),
                decodingFormat,
                subscriptions,
                SubscriptionCreateOptionsMapper.map(config),
                StartPositionMapper.map(config),
                config.getOptional(PubSubConnectorOptions.SCAN_ORDERING_MODE).orElse(null),
                config.getOptional(PubSubConnectorOptions.SCAN_DESERIALIZATION_FAILURE_POLICY)
                        .orElse(null),
                SubscriberOptionsMapper.map(config),
                config.getOptional(PubSubConnectorOptions.SERVICE_ACCOUNT_KEY_FILE).orElse(null),
                config.getOptional(PubSubConnectorOptions.EMULATOR_ENDPOINT).orElse(null),
                config.getOptional(FactoryUtil.SOURCE_PARALLELISM).orElse(null));
    }

    private static void validateAutoCreateTopicsSyntax(Context context) {
        String option = PubSubConnectorOptions.SCAN_AUTO_CREATE_TOPICS.key();
        Map<String, String> rawOptions = context.getCatalogTable().getOptions();
        boolean hasPackedMap = rawOptions.containsKey(option);
        boolean hasPrefixedEntry =
                rawOptions.keySet().stream().anyMatch(key -> key.startsWith(option + "."));
        if (hasPackedMap && hasPrefixedEntry) {
            throw new ValidationException(
                    String.format(
                            "Option '%s' must use either the packed map syntax or prefixed map"
                                    + " entries, not both.",
                            option));
        }
    }

    private static void validateCredentialsMode(ReadableConfig config) {
        config.getOptional(PubSubConnectorOptions.SERVICE_ACCOUNT_KEY_FILE)
                .ifPresent(
                        path -> {
                            if (path.isBlank()) {
                                throw new ValidationException(
                                        String.format(
                                                "Option '%s' must not be blank.",
                                                PubSubConnectorOptions.SERVICE_ACCOUNT_KEY_FILE
                                                        .key()));
                            }
                        });
        if (config.getOptional(PubSubConnectorOptions.SERVICE_ACCOUNT_KEY_FILE).isPresent()
                && config.getOptional(PubSubConnectorOptions.EMULATOR_ENDPOINT).isPresent()) {
            throw new ValidationException(
                    String.format(
                            "Options '%s' and '%s' cannot be combined: an emulator uses a"
                                    + " plaintext channel with no credentials. Remove one of the"
                                    + " two options.",
                            PubSubConnectorOptions.SERVICE_ACCOUNT_KEY_FILE.key(),
                            PubSubConnectorOptions.EMULATOR_ENDPOINT.key()));
        }
    }

    /**
     * Names the option key rather than the builder setter a SQL caller never wrote (issue #1019,
     * {@code docs/adr/0127}).
     *
     * <p>The value reached {@code EmulatorEndpoint.parse} before this, through {@code
     * PubSubSinkBuilder.emulatorEndpoint(String)} or {@code
     * PubSubSourceBuilder.emulatorEndpoint(String)} during plan-to-runtime translation, so the
     * failure already landed on the client — but named {@code emulatorEndpoint}. Those setters keep
     * their parse: it is the check a DataStream caller meets, where that name is the right one.
     *
     * <p>Call it after every check that refuses an option outright, never before one: a DDL told to
     * remove {@code emulator-endpoint} beside {@code service-account-key-file} is not helped by an
     * answer about its shape. It also follows the {@code topic} and {@code subscription}
     * requirements, so a table that has not said where it points hears that first. The option
     * mappers below run later and carry refusals of their own, so a DDL that trips one of those and
     * carries a malformed endpoint reads this message first.
     *
     * <p>The rejection is left as the {@code IllegalArgumentException} the parse throws, which
     * {@code FactoryUtil} wraps, matching {@code TopicDestination.of} rather than the {@code
     * ValidationException} the option checks in this class raise directly.
     */
    private static void validateEmulatorEndpoint(ReadableConfig config) {
        config.getOptional(PubSubConnectorOptions.EMULATOR_ENDPOINT)
                .ifPresent(
                        value ->
                                EmulatorEndpoint.parse(
                                        value, PubSubConnectorOptions.EMULATOR_ENDPOINT.key()));
    }
}
