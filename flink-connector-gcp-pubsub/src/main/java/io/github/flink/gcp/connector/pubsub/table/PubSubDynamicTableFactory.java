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
                        PubSubConnectorOptions.SUBSCRIPTION,
                        PubSubConnectorOptions.SCAN_ORDERING_MODE,
                        PubSubConnectorOptions.SCAN_DESERIALIZATION_FAILURE_POLICY,
                        PubSubConnectorOptions.SCAN_FLOW_CONTROL_MAX_OUTSTANDING_ELEMENT_COUNT,
                        PubSubConnectorOptions.SCAN_FLOW_CONTROL_MAX_OUTSTANDING_REQUEST_BYTES,
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
                        PubSubConnectorOptions.SCAN_AUTO_CREATE_TOPIC,
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
                        PubSubConnectorOptions.SINK_RECOVERY_INITIAL_BACKOFF,
                        PubSubConnectorOptions.SINK_RECOVERY_MAX_BACKOFF,
                        PubSubConnectorOptions.SINK_RECOVERY_MAX_ATTEMPTS,
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
        return new PubSubDynamicSink(
                context.getPhysicalRowDataType(),
                encodingFormat,
                TopicDestination.of(config.get(PubSubConnectorOptions.PROJECT), topic),
                config.getOptional(PubSubConnectorOptions.SINK_CREATE_DISPOSITION).orElse(null),
                TopicCreateOptionsMapper.map(config),
                PublisherOptionsMapper.map(config),
                config.getOptional(PubSubConnectorOptions.EMULATOR_ENDPOINT).orElse(null),
                config.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null));
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                helper.discoverDecodingFormat(
                        DeserializationFormatFactory.class, FactoryUtil.FORMAT);
        helper.validate();

        ReadableConfig config = helper.getOptions();
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
                config.getOptional(PubSubConnectorOptions.EMULATOR_ENDPOINT).orElse(null),
                config.getOptional(FactoryUtil.SOURCE_PARALLELISM).orElse(null));
    }
}
